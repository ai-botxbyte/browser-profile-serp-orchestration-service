"""SERP Task ID Consumer - Polls task results and stores in Redis"""

from __future__ import annotations

from typing import Optional, Any, Union

import json
import asyncio
import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from loguru import logger

from app.exception.baseapp_exception import ConsumerJobException
from app.job.baseapp_job import BaseAppJob


class SerpTaskIdConsumer:
    """Consumer for SERP task ID queue"""

    QUEUE_NAME = "serp_task_id_queue"
    DLX_QUEUE_NAME = "serp_task_id_dlx_queue"

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
        logger.info(f"SerpTaskIdConsumer initialized for queue: {self.queue_name}")

    async def connect(self) -> None:
        """Establish connection to RabbitMQ"""
        try:
            self.connection = await aio_pika.connect_robust(
                self.config.RABBITMQ_URL,
                heartbeat=60,
                blocked_connection_timeout=300,
            )
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)

            queue_args = {"x-max-priority": 10}

            # Main task ID queue
            self.queue = await self.channel.declare_queue(
                self.queue_name,
                durable=True,
                auto_delete=False,
                arguments=queue_args
            )

            # DLX queue
            await self.channel.declare_queue(
                self.dlx_queue_name,
                durable=True,
                auto_delete=False,
                arguments=queue_args
            )

            logger.info(f"Connected to RabbitMQ queue: {self.queue_name}")

        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def disconnect(self) -> None:
        """Close RabbitMQ connection"""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("Disconnected from RabbitMQ")

    async def _consume_message(self, message: AbstractIncomingMessage) -> None:
        """Handle incoming message"""
        async with message.process(requeue=False):
            try:
                message_data = json.loads(message.body.decode())
                logger.debug(f"Task ID Consumer received message: {message_data}")

                try:
                    await self._execute_jobs(message_data)
                    logger.info("Task ID Consumer completed processing message successfully")

                except ConsumerJobException as job_error:
                    logger.error(f"Task ID Consumer: Job execution failed: {job_error}")
                    await self._send_to_dlx(message_data, str(job_error))
                except Exception as job_error:
                    logger.error(f"Task ID Consumer: Unexpected error: {job_error}")
                    await self._send_to_dlx(message_data, str(job_error))

            except json.JSONDecodeError as e:
                logger.error(f"Task ID Consumer JSON decode error: {e}")
            except Exception as e:
                logger.error(f"Task ID Consumer processing error: {e}")

    async def _send_to_dlx(self, message_data: dict, error: str) -> None:
        """Send failed message to DLX queue (infinite retry)"""
        try:
            dlx_message = {
                **message_data,
                "error": error
            }
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

        logger.info(f"Starting to consume messages from: {self.queue_name}")
        await self.queue.consume(self._consume_message)

        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")

    async def _execute_jobs(self, message: dict) -> None:
        """Execute all jobs"""
        jobs = self.job_processor if isinstance(self.job_processor, list) else [self.job_processor]

        logger.info(f"Task ID Consumer: Starting execution of {len(jobs)} job(s)")

        for job in jobs:
            job_name = job.__class__.__name__
            logger.info(f"Task ID Consumer: Executing {job_name}")

            try:
                await job.execute(message=message)
                logger.info(f"Task ID Consumer: Job {job_name} completed successfully")
            except Exception as e:
                logger.error(f"Task ID Consumer: Job {job_name} failed: {e}")
                raise ConsumerJobException(
                    queue_name=self.queue_name,
                    job_name=job_name,
                    job_error=str(e)
                ) from e
