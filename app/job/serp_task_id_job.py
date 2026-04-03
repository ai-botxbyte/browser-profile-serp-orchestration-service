"""SERP Task ID Job - Polls task result and stores in Redis"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Optional

import httpx
from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.config.config import get_config
from app.helper.redis_helper import RedisHelper


class SerpTaskIdJob(BaseAppJob):
    """Job to poll task result and store in Redis"""

    def __init__(self):
        super().__init__()
        self.config = get_config()
        self.redis_helper: Optional[RedisHelper] = None

    async def _ensure_redis(self) -> RedisHelper:
        """Ensure Redis connection is established"""
        if self.redis_helper is None:
            self.redis_helper = RedisHelper()
            await self.redis_helper.initialize()
        return self.redis_helper

    async def execute(self, message: Any) -> None:
        """
        Poll task result and store in Redis.

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

        # Store result in Redis with 3-hour TTL
        await self._store_result(query_id, task_id, search_type, result_data)

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
                                logger.error(f"Task {task_id} failed: {data}")
                                return None

                            logger.debug(
                                f"Task {task_id} status: {status}, "
                                f"attempt {attempt + 1}/{max_attempts}"
                            )
                        else:
                            status = data.get("task_status", data.get("status", "")).lower()
                            if status in ["completed", "complete", "success", "done"]:
                                logger.info(f"Task {task_id} completed on attempt {attempt + 1}")
                                return data

                    elif response.status_code == 404:
                        logger.debug(f"Task {task_id} not found yet, waiting...")

            except Exception as e:
                logger.warning(f"Error polling task {task_id}: {e}")

            await asyncio.sleep(poll_interval)

        logger.error(f"Task {task_id} timed out after {max_attempts} attempts")
        return None

    async def _store_result(
        self,
        query_id: str,
        task_id: str,
        search_type: str,
        result_data: dict
    ) -> None:
        """Store SERP result in Redis with TTL"""
        redis = await self._ensure_redis()

        cache_key = f"serp:result:{query_id}"
        ttl = self.config.SERP_REDIS_TTL  # 3 hours

        # Extract output_data which contains the SERP results
        output_data = result_data.get("output_data", {})

        # Parse serp_result JSON string if present
        serp_result = None
        if output_data and output_data.get("variables"):
            serp_result_str = output_data["variables"].get("serp_result")
            if serp_result_str:
                try:
                    serp_result = json.loads(serp_result_str)
                except json.JSONDecodeError:
                    serp_result = serp_result_str

        # Store structured data
        store_data = {
            "query_id": query_id,
            "task_id": task_id,
            "search_type": search_type,
            "task_status": result_data.get("task_status"),
            "started_at": result_data.get("started_at"),
            "completed_at": result_data.get("completed_at"),
            "serp_result": serp_result,
            "variables": output_data.get("variables", {}) if output_data else {}
        }

        success = await redis.set(cache_key, store_data, ttl=ttl)

        if success:
            logger.info(f"SERP result stored in Redis: {cache_key} (TTL: {ttl}s)")
        else:
            logger.error(f"Failed to store SERP result in Redis: {cache_key}")
            raise RuntimeError(f"Failed to store result in Redis for query_id: {query_id}")

    async def cleanup(self) -> None:
        """Cleanup resources"""
        if self.redis_helper:
            await self.redis_helper.close()
            self.redis_helper = None
