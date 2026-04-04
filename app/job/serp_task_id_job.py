"""SERP Task ID Job - Polls task result and sends to response queue"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Optional

import httpx
from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.config.config import get_config
from app.helper.rabbitmq_helper import RabbitMQHelper


class SerpTaskIdJob(BaseAppJob):
    """Job to poll task result and send to serp_response_queue"""

    RESPONSE_QUEUE = "serp_response_queue"
    REQ_QUEUE = "serp_req_queue"  # For retrying failed tasks

    def __init__(self):
        super().__init__()
        self.config = get_config()
        self.rabbitmq_helper: Optional[RabbitMQHelper] = None

    async def _ensure_rabbitmq(self) -> RabbitMQHelper:
        """Ensure RabbitMQ connection is established"""
        if self.rabbitmq_helper is None:
            self.rabbitmq_helper = RabbitMQHelper()
        return self.rabbitmq_helper

    async def execute(self, message: Any) -> None:
        """
        Poll task result and send to serp_response_queue.

        Args:
            message: Message with format:
                {
                    "query_id": "uuid",
                    "task_id": "uuid",
                    "search_type": "google-web"
                }
        """
        query_id = message.get("query_id")
        task_id = message.get("task_id")
        search_type = message.get("search_type")

        if not all([query_id, task_id]):
            raise ValueError(f"Missing required fields in message: {message}")

        logger.info(f"Processing task result: query_id={query_id}, task_id={task_id}")

        # Poll for task completion and get results
        result_data = await self._poll_task_result(task_id)

        if result_data is None:
            raise RuntimeError(f"Failed to get SERP results for task_id: {task_id}")

        # Check if task failed due to AI-management issues (e.g., wait_for timeout)
        task_status = result_data.get("task_status", "").lower()
        if task_status in ["failed", "error"]:
            # Requeue to serp_req_queue for retry
            await self._requeue_to_req_queue(query_id, search_type, result_data)
            logger.info(f"Failed task requeued to serp_req_queue: query_id={query_id}")
            return

        # Send successful result to serp_response_queue
        await self._send_to_response_queue(query_id, task_id, search_type, result_data)

        logger.info(f"SERP task ID job completed: query_id={query_id}, task_id={task_id}")

    async def _poll_task_result(
        self,
        task_id: str,
        max_attempts: int = 120,
        poll_interval: float = 5.0
    ) -> Optional[dict]:
        """Poll for task result until completion"""

        result_url = f"{self.config.AI_MANAGEMENT_SERVICE_URL}/task-output/read/{task_id}/"

        headers = {
            "accept": "application/json",
            "X-API-Key": self.config.AI_MANAGEMENT_SERVICE_API_KEY
        }

        logger.info(f"Polling task result: URL={result_url}")

        for attempt in range(max_attempts):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(result_url, headers=headers)

                    logger.debug(f"Poll attempt {attempt + 1}: status={response.status_code}")

                    if response.status_code == 200:
                        data = response.json()

                        if data.get("success") and data.get("data"):
                            result_data = data["data"]
                            status = result_data.get("task_status", "").lower()

                            if status in ["completed", "complete", "success", "done"]:
                                logger.info(f"Task {task_id} completed on attempt {attempt + 1}")
                                return result_data

                            if status in ["failed", "error"]:
                                logger.warning(f"Task {task_id} failed: {result_data.get('output_data', {}).get('error', 'Unknown error')}")
                                # Return failed result - it's a terminal state, no retry needed
                                return result_data

                            logger.debug(
                                f"Task {task_id} status: {status}, "
                                f"attempt {attempt + 1}/{max_attempts}"
                            )
                        else:
                            status = data.get("task_status", data.get("status", "")).lower()
                            if status in ["completed", "complete", "success", "done"]:
                                logger.info(f"Task {task_id} completed on attempt {attempt + 1}")
                                return data
                            if status in ["failed", "error"]:
                                logger.warning(f"Task {task_id} failed")
                                return data

                    elif response.status_code == 404:
                        logger.debug(f"Task {task_id} not found yet, waiting...")

            except Exception as e:
                logger.warning(f"Error polling task {task_id}: {e}")

            await asyncio.sleep(poll_interval)

        logger.error(f"Task {task_id} timed out after {max_attempts} attempts")
        return None

    async def _requeue_to_req_queue(
        self,
        query_id: str,
        search_type: str,
        result_data: dict
    ) -> None:
        """Requeue failed task to serp_req_queue for retry"""
        rabbitmq = await self._ensure_rabbitmq()

        # Extract query from result_data
        output_data = result_data.get("output_data", {})
        variables = output_data.get("variables", {})
        query = variables.get("query") or variables.get("queries")

        if not query:
            logger.error(f"Cannot requeue: no query found in result_data for query_id={query_id}")
            return

        # Prepare message for serp_req_queue
        message_data = {
            "query": query,
            "query_id": query_id,
            "search_type": search_type
        }

        success = await rabbitmq.publish_message(
            queue_name=self.REQ_QUEUE,
            message=message_data,
            priority=5
        )

        if success:
            logger.info(f"Failed task requeued to {self.REQ_QUEUE}: query_id={query_id}, query={query}")
        else:
            logger.error(f"Failed to requeue to {self.REQ_QUEUE}: query_id={query_id}")
            raise RuntimeError(f"Failed to requeue to {self.REQ_QUEUE} for query_id: {query_id}")

    async def _send_to_response_queue(
        self,
        query_id: str,
        task_id: str,
        search_type: str,
        result_data: dict
    ) -> None:
        """Send SERP result to serp_response_queue"""
        rabbitmq = await self._ensure_rabbitmq()

        # Extract output_data which contains the SERP results
        output_data = result_data.get("output_data", {})
        task_status = result_data.get("task_status", "").lower()

        # Parse serp_result JSON string if present
        serp_result = None
        error_message = None
        variables = {}

        if output_data:
            # Check for error in output_data
            error_message = output_data.get("error")

            if output_data.get("variables"):
                # Copy variables and remove serp_result to avoid duplication
                variables = {
                    k: v for k, v in output_data["variables"].items()
                    if k != "serp_result"
                }

                serp_result_str = output_data["variables"].get("serp_result")
                if serp_result_str:
                    try:
                        serp_result = json.loads(serp_result_str)
                    except json.JSONDecodeError:
                        serp_result = serp_result_str

        # Prepare message data
        message_data = {
            "query_id": query_id,
            "task_id": task_id,
            "search_type": search_type,
            "task_status": result_data.get("task_status"),
            "success": task_status in ["completed", "complete", "success", "done"],
            "started_at": result_data.get("started_at"),
            "completed_at": result_data.get("completed_at"),
            "serp_result": serp_result,
            "error": error_message,
            "variables": variables
        }

        # Publish to serp_response_queue
        success = await rabbitmq.publish_message(
            queue_name=self.RESPONSE_QUEUE,
            message=message_data,
            priority=5
        )

        if success:
            logger.info(f"SERP result sent to {self.RESPONSE_QUEUE}: query_id={query_id}")
        else:
            logger.error(f"Failed to send SERP result to {self.RESPONSE_QUEUE}: query_id={query_id}")
            raise RuntimeError(f"Failed to send result to queue for query_id: {query_id}")

    async def cleanup(self) -> None:
        """Cleanup resources"""
        if self.rabbitmq_helper:
            await self.rabbitmq_helper.close()
            self.rabbitmq_helper = None
