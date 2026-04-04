"""SERP Request DLX Consumer - Requeues failed messages infinitely"""

from __future__ import annotations

from typing import Optional, Any

import json
import asyncio
import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from loguru import logger


class SerpReqDlxConsumer:
    """Consumer for SERP request DLX queue - infinite retry"""

    QUEUE_NAME = "serp_req_dlx_queue"
    TARGET_QUEUE_NAME = "serp_req_queue"
    RETRY_DELAY = 5.0  # seconds
    CONCURRENCY_LIMIT = 10  # Process up to 10 messages concurrently

    def __init__(self, config: Any):
        self.queue_name = self.QUEUE_NAME
        self.target_queue_name = self.TARGET_QUEUE_NAME
        self.config = config
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queue: Optional[aio_pika.Queue] = None
        self._active_tasks: set[asyncio.Task] = set()
        logger.info(f"SerpReqDlxConsumer initialized for queue: {self.queue_name}")

    async def connect(self) -> None:
        """Establish connection to RabbitMQ"""
        try:
            self.connection = await aio_pika.connect_robust(
                self.config.RABBITMQ_URL,
                heartbeat=60,
                blocked_connection_timeout=300,
            )
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=self.CONCURRENCY_LIMIT)

            queue_args = {"x-max-priority": 10}

            # Ensure DLX queue exists
            self.queue = await self.channel.declare_queue(
                self.queue_name,
                durable=True,
                auto_delete=False,
                arguments=queue_args
            )

            # Ensure target queue exists
            await self.channel.declare_queue(
                self.target_queue_name,
                durable=True,
                auto_delete=False,
                arguments=queue_args
            )

            logger.info(f"Connected to RabbitMQ DLX queue: {self.queue_name}")

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
        """Handle incoming DLX message - spawn async task for processing"""
        task = asyncio.create_task(self._process_message_async(message))
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

    async def _process_message_async(self, message: AbstractIncomingMessage) -> None:
        """Process DLX message asynchronously and requeue"""
        try:
            message_data = json.loads(message.body.decode())
            logger.info(f"DLX Consumer received message: query_id={message_data.get('query_id')}")

            # Wait before requeuing
            await asyncio.sleep(self.RETRY_DELAY)

            # Remove error field and requeue
            requeue_data = {k: v for k, v in message_data.items() if k != "error"}

            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(requeue_data).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=self.target_queue_name
            )

            await message.ack()
            logger.info(f"Message requeued to: {self.target_queue_name}")

        except json.JSONDecodeError as e:
            logger.error(f"DLX Consumer JSON decode error: {e}")
            await message.ack()
        except Exception as e:
            logger.error(f"DLX Consumer processing error: {e}")
            await message.nack(requeue=True)

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
