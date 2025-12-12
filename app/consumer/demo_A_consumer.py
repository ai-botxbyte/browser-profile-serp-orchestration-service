from __future__ import annotations

import asyncio
from loguru import logger

from aio_pika.abc import AbstractIncomingMessage

from app.consumer.baseapp_consumer import BaseAppConsumer
from app.config.baseapp_config import get_base_config


class DemoAConsumer(BaseAppConsumer):
    """
    Demo A consumer - only consumes messages.
    
    Job execution is handled by DemoAWorker.
    """
    
    def __init__(self):
        """
        Initialize the demo A consumer for message consumption only.
        """
        config = get_base_config()
        
        # Consumer only consumes messages - job processing is handled by DemoAWorker
        super().__init__(
            queue_name="demo_A_queue",  # Queue for demo A service
            config=config
        )
        
        logger.info(f"{self.__class__.__name__} initialized for message consumption only")
        
        # Establish connection during initialization
        asyncio.create_task(self.connect())
    
    async def start_consuming(self) -> None:
        """Start consuming messages from demo_A_queue"""
        if not self.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")
        
        logger.info(f"Starting to consume messages from: {self.queue_name}")
        
        async def consume_message(message: AbstractIncomingMessage) -> None:
            """Simple message consumption handler"""
            async with message.process():
                logger.debug(f"Consumer received message from queue: {self.queue_name}")
        
        # Start consuming and get consumer tag
        logger.info(f"Consumer started successfully for queue: {self.queue_name}")
        
        # Keep the consumer running indefinitely
        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
    
