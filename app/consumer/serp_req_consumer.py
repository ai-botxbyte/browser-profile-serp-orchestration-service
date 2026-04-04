"""SERP Request Consumer - Creates tasks and sends to task_id queue"""

from __future__ import annotations

from typing import Optional, Any, Union

import json
import asyncio
import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from loguru import logger

from app.exception.baseapp_exception import ConsumerJobException
from app.job.baseapp_job import BaseAppJob


class SerpReqConsumer:
    """Consumer for SERP request queue"""

    QUEUE_NAME = "serp_req_queue"
    DLX_QUEUE_NAME = "serp_req_dlx_queue"
    TASK_ID_QUEUE_NAME = "serp_task_id_queue"
    TASK_ID_DLX_QUEUE_NAME = "serp_task_id_dlx_queue"
    CONCURRENCY_LIMIT = 10  # Process up to 10 messages concurrently

    def __init__(
        self,
        config: Any,
        job_processor: Optional[Union[list[BaseAppJob], BaseAppJob]] = None
    ):
        self.queue_name = self.QUEUE_NAME
        self.dlx_queue_name = self.DLX_QUEUE_NAME
        self.config = config
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queue: Optional[aio_pika.Queue] = None
        self.job_processor = job_processor
        self._active_tasks: set[asyncio.Task] = set()
        logger.info(f"SerpReqConsumer initialized for queue: {self.queue_name}")

    async def connect(self) -> None:
        """Establish connection to RabbitMQ and declare all SERP queues"""
        try:
            self.connection = await aio_pika.connect_robust(
                self.config.RABBITMQ_URL,
                heartbeat=60,
                blocked_connection_timeout=300,
            )
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=self.CONCURRENCY_LIMIT)

            # Declare all 4 SERP queues
            queue_args = {"x-max-priority": 10}

            # 1. serp_req_queue (main)
            self.queue = await self.channel.declare_queue(
                self.QUEUE_NAME,
                durable=True,
                auto_delete=False,
                arguments=queue_args
            )

            # 2. serp_req_dlx_queue
            await self.channel.declare_queue(
                self.DLX_QUEUE_NAME,
                durable=True,
                auto_delete=False,
                arguments=queue_args
            )

            # 3. serp_task_id_queue
            await self.channel.declare_queue(
                self.TASK_ID_QUEUE_NAME,
                durable=True,
                auto_delete=False,
                arguments=queue_args
            )

            # 4. serp_task_id_dlx_queue
            await self.channel.declare_queue(
                self.TASK_ID_DLX_QUEUE_NAME,
                durable=True,
                auto_delete=False,
                arguments=queue_args
            )

            # Pass channel to job processors
            jobs = self.job_processor if isinstance(self.job_processor, list) else [self.job_processor]
            for job in jobs:
                if hasattr(job, 'set_channel'):
                    job.set_channel(self.channel)

            logger.info(f"Connected to RabbitMQ. All SERP queues declared.")

        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def disconnect(self) -> None:
        """Close RabbitMQ connection"""
        # Wait for active tasks to complete
        if self._active_tasks:
            logger.info(f"Waiting for {len(self._active_tasks)} active tasks to complete...")
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("Disconnected from RabbitMQ")

    async def _consume_message(self, message: AbstractIncomingMessage) -> None:
        """Handle incoming message - spawn async task for processing"""
        task = asyncio.create_task(self._process_message_async(message))
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

    async def _process_message_async(self, message: AbstractIncomingMessage) -> None:
        """Process message asynchronously"""
        try:
            message_data = json.loads(message.body.decode())
            logger.debug(f"SERP Consumer received message: {message_data}")

            try:
                await self._execute_jobs(message_data)
                await message.ack()
                logger.info("SERP Consumer completed processing message successfully")

            except ConsumerJobException as job_error:
                logger.error(f"SERP Consumer: Job execution failed: {job_error}")
                await self._send_to_dlx(message_data, str(job_error))
                await message.ack()
            except Exception as job_error:
                logger.error(f"SERP Consumer: Unexpected error: {job_error}")
                await self._send_to_dlx(message_data, str(job_error))
                await message.ack()

        except json.JSONDecodeError as e:
            logger.error(f"SERP Consumer JSON decode error: {e}")
            await message.ack()
        except Exception as e:
            logger.error(f"SERP Consumer processing error: {e}")
            await message.nack(requeue=False)

    async def _send_to_dlx(self, message_data: dict, error: str) -> None:
        """Send failed message to DLX queue (infinite retry)"""
        try:
            dlx_message = {
                **message_data,
                "error": error
            }
            # Remove retry_count - infinite retry
            dlx_message.pop("retry_count", None)

            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(dlx_message).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=self.dlx_queue_name
            )
            logger.info(f"Message sent to DLX queue: {self.dlx_queue_name}")
        except Exception as e:
            logger.error(f"Failed to send message to DLX: {e}")

    async def start_consuming(self) -> None:
        """Start consuming messages"""
        if not self.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")

        logger.info(
            f"Starting to consume messages from: {self.queue_name} "
            f"(concurrency: {self.CONCURRENCY_LIMIT})"
        )
        await self.queue.consume(self._consume_message, no_ack=False)

        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")

    async def _execute_jobs(self, message: dict) -> None:
        """Execute all jobs"""
        jobs = self.job_processor if isinstance(self.job_processor, list) else [self.job_processor]

        logger.info(f"SERP Consumer: Starting execution of {len(jobs)} job(s)")

        for job in jobs:
            job_name = job.__class__.__name__
            logger.info(f"SERP Consumer: Executing {job_name}")

            try:
                await job.execute(message=message)
                logger.info(f"SERP Consumer: Job {job_name} completed successfully")
            except Exception as e:
                logger.error(f"SERP Consumer: Job {job_name} failed: {e}")
                raise ConsumerJobException(
                    queue_name=self.queue_name,
                    job_name=job_name,
                    job_error=str(e)
                ) from e
