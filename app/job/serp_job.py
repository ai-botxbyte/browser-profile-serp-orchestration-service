"""SERP Job - Calls AI Management Service and stores results in Redis"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

import httpx
from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.config.config import get_config
from app.helper.redis_helper import RedisHelper


class SerpJob(BaseAppJob):
    """Job to process SERP requests via AI Management Service"""

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
        Process SERP request.

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

        # Call AI Management Service
        task_id = await self._create_serp_task(query, workflow_id, search_type)

        if not task_id:
            raise RuntimeError(f"Failed to create SERP task for query_id: {query_id}")

        logger.info(f"SERP task created: task_id={task_id}")

        # Poll for task completion and get results
        result_data = await self._poll_task_result(task_id)

        if result_data is None:
            raise RuntimeError(f"Failed to get SERP results for task_id: {task_id}")

        # Store result in Redis with 3-hour TTL
        await self._store_result(query_id, result_data)

        logger.info(f"SERP job completed: query_id={query_id}, task_id={task_id}")

    async def _create_serp_task(
        self,
        query: str,
        workflow_id: str,
        search_type: str
    ) -> Optional[str]:
        """Create SERP task via AI Management Service"""
        url = f"{self.config.AI_MANAGEMENT_SERVICE_URL}/serp/create/"

        # Determine variables based on search type
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

                # Fallback to other possible response formats
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

        # Add search engine specific variables if needed
        if search_type.startswith("bing"):
            base_vars["search_engine"] = "bing"
        else:
            base_vars["search_engine"] = "google"

        return base_vars

    async def _poll_task_result(
        self,
        task_id: str,
        max_attempts: int = 60,
        poll_interval: float = 5.0
    ) -> Optional[dict]:
        """Poll for task result until completion"""

        # Correct endpoint for task output
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

                        # Check if response has success and data
                        if data.get("success") and data.get("data"):
                            result_data = data["data"]
                            status = result_data.get("status", "").lower()

                            if status in ["completed", "success", "done"]:
                                logger.info(f"Task {task_id} completed on attempt {attempt + 1}")
                                return result_data.get("output") or result_data

                            if status in ["failed", "error"]:
                                logger.error(f"Task {task_id} failed: {data}")
                                return None

                            # Still processing
                            logger.debug(
                                f"Task {task_id} status: {status}, "
                                f"attempt {attempt + 1}/{max_attempts}"
                            )
                        else:
                            # Direct response format
                            status = data.get("status", "").lower()
                            if status in ["completed", "success", "done"]:
                                logger.info(f"Task {task_id} completed on attempt {attempt + 1}")
                                return data.get("result") or data.get("data") or data

                    elif response.status_code == 404:
                        # Task not found yet, might still be processing
                        logger.debug(f"Task {task_id} not found yet, waiting...")

            except Exception as e:
                logger.warning(f"Error polling task {task_id}: {e}")

            await asyncio.sleep(poll_interval)

        logger.error(f"Task {task_id} timed out after {max_attempts} attempts")
        return None

    async def _store_result(self, query_id: str, result_data: dict) -> None:
        """Store SERP result in Redis with TTL"""
        redis = await self._ensure_redis()

        cache_key = f"serp:result:{query_id}"
        ttl = self.config.SERP_REDIS_TTL  # 3 hours

        success = await redis.set(cache_key, result_data, ttl=ttl)

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
