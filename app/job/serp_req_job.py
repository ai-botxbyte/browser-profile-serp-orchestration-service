"""SERP Request Job - Creates SERP task and sends task_id to queue"""

from __future__ import annotations

from typing import Any, Optional

import json
import httpx
import aio_pika
from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.config.config import get_config


class SerpReqJob(BaseAppJob):
    """Job to create SERP task and send task_id to serp_task_id_queue"""

    TASK_ID_QUEUE = "serp_task_id_queue"

    def __init__(self):
        super().__init__()
        self.config = get_config()
        self.channel: Optional[aio_pika.Channel] = None

    def set_channel(self, channel: aio_pika.Channel) -> None:
        """Set the RabbitMQ channel for publishing"""
        self.channel = channel

    async def execute(self, message: Any) -> None:
        """
        Process SERP request - create task and send task_id to queue.

        Args:
            message: Message with format:
                {
                    "query": "search query",
                    "query_id": "uuid",
                    "search_type": "google-web"
                }
        """
        query = message.get("query")
        query_id = message.get("query_id")
        search_type = message.get("search_type")

        if not all([query, query_id, search_type]):
            raise ValueError(f"Missing required fields in message: {message}")

        # Get workflow ID for search type
        workflow_id = self.config.get_workflow_id(search_type)
        if not workflow_id:
            raise ValueError(f"No workflow ID configured for search_type: {search_type}")

        logger.info(f"Processing SERP request: query_id={query_id}, search_type={search_type}")

        # Call AI Management Service to create task
        task_id = await self._create_serp_task(query, workflow_id, search_type)

        if not task_id:
            raise RuntimeError(f"Failed to create SERP task for query_id: {query_id}")

        logger.info(f"SERP task created: task_id={task_id}")

        # Send task_id to serp_task_id_queue for result polling
        await self._send_to_task_id_queue(query_id, task_id, search_type)

        logger.info(f"SERP request job completed: query_id={query_id}, task_id={task_id}")

    async def _create_serp_task(
        self,
        query: str,
        workflow_id: str,
        search_type: str
    ) -> Optional[str]:
        """Create SERP task via AI Management Service"""
        url = f"{self.config.AI_MANAGEMENT_SERVICE_URL}/serp/create/"

        variables = self._get_search_variables(search_type)

        payload = {
            "query": query,
            "workflow_ids": [workflow_id],
            "priority": "medium",
            "variables": variables
        }

        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "X-API-Key": self.config.AI_MANAGEMENT_SERVICE_API_KEY
        }

        logger.info(f"SERP API Request: URL={url}")
        logger.debug(f"SERP API Payload: {payload}")

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(url, json=payload, headers=headers)

                logger.info(f"SERP API Response: status={response.status_code}")

                if response.status_code not in [200, 201]:
                    logger.error(f"SERP API Error Response: {response.text}")
                    response.raise_for_status()

                data = response.json()
                logger.info(f"SERP API Response data: {data}")

                # Response structure: {"data": {"task_ids": ["..."], "tasks": [...]}}
                if data.get("success") and data.get("data"):
                    task_ids = data["data"].get("task_ids", [])
                    if task_ids:
                        return task_ids[0]

                return data.get("task_id") or data.get("id")

        except httpx.HTTPStatusError as e:
            logger.error(f"SERP API HTTP error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"SERP API request failed: {e}")
            raise

    def _get_search_variables(self, search_type: str) -> dict:
        """Get search variables based on search type"""
        base_vars = {
            "device": "desktop",
            "gl": "us",
            "hl": "en"
        }

        if search_type.startswith("bing"):
            base_vars["search_engine"] = "bing"
        else:
            base_vars["search_engine"] = "google"

        return base_vars

    async def _send_to_task_id_queue(
        self,
        query_id: str,
        task_id: str,
        search_type: str
    ) -> None:
        """Send task_id to serp_task_id_queue for result polling"""
        if not self.channel:
            raise RuntimeError("RabbitMQ channel not set. Call set_channel() first.")

        message_data = {
            "query_id": query_id,
            "task_id": task_id,
            "search_type": search_type
        }

        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message_data).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=self.TASK_ID_QUEUE
        )

        logger.info(f"Task ID sent to queue: {self.TASK_ID_QUEUE}, task_id={task_id}")
