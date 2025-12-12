from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Callable, Any

import aio_pika
from loguru import logger


class BaseAppConsumer(ABC):
    """Base consumer class that can be reused by all consumers following baseapp patterns"""
    
    def __init__(self,  queue_name: str, config: Any):
        """
        Initialize the base consumer.
        """
        self.queue_name = queue_name
        self.config = config
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queue: Optional[aio_pika.Queue] = None
        logger.info(f"{self.__class__.__name__} initialized for queue: {queue_name}")
        
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
            
            # [ ] We never ever declare queue while consuming - it's producing time works.
            # Declare main queue with priority support
            # [x] Idempotently declare the main queue. This is a best practice for consumers
            # to ensure the queue exists with the correct properties before they start listening.
            # If the queue already exists, this command does nothing.
            self.queue = await self.channel.declare_queue(
                self.queue_name,
                durable=True,
                auto_delete=False,
                arguments={
                    "x-max-priority": 10  # Support priority levels 0-10
                }
            )

            # [ ] We never ever declare queue while consuming - it's producing time works.
            # Declare dead letter queue for permanent storage (no consumer needed)

            # [x] Idempotently declare the associated dead-letter queue. The consumer is responsible
            # for this as it controls the logic for dead-lettering failed messages.
            await self.channel.declare_queue(
                f"{self.queue_name}_dead_letter",
                durable=True,
                auto_delete=False,
                arguments={
                    "x-max-priority": 10  # Match the priority setting of main queue
                }
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
    
    @abstractmethod
    async def start_consuming(self) -> None:
        """Start consuming messages from the queue - Must be implemented by subclasses"""
