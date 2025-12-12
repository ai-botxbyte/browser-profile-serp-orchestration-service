"""Base worker class for job execution"""

from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from typing import Any, Optional

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from loguru import logger

from app.exception.consumer_demo_exception import ConsumerDemoJobException


class BaseAppWorker(ABC):
    """Base worker class that executes jobs"""
    
    def __init__(self, queue_name: str, consumer: Any):
        """
        Initialize the base worker.
        
        Args:
            queue_name: Name of the queue (for reference)
            consumer: Consumer instance for connection management
        """
        self.queue_name = queue_name
        self.consumer = consumer
        logger.info(f"{self.__class__.__name__} initialized for queue: {queue_name}")
    
    async def connect(self) -> None:
        """Establish connection to RabbitMQ using consumer's connection"""
        # Use consumer's connection
        await self.consumer.connect()
        logger.info(f"Worker connected to RabbitMQ queue: {self.queue_name}")
    
    async def disconnect(self) -> None:
        """Close RabbitMQ connection using consumer's disconnect"""
        await self.consumer.disconnect()
        logger.info("Worker disconnected from RabbitMQ")
    
    async def start_consuming(self) -> None:
        """Start consuming messages from the queue"""
        if not self.consumer.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")
        
        logger.info(f"Starting worker to consume messages from: {self.queue_name}")
        
        logger.info(f"Worker started successfully for queue: {self.queue_name}")
        
        # Keep the worker running indefinitely
        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
    
    async def process_message(self, message: AbstractIncomingMessage) -> None:
        """
        Process a single message: execute jobs.
        
        This is the main orchestration method that:
        1. Parses the message
        2. Executes related jobs
        
        Uses try/except with raise for easy debugging.
        """
        async with message.process():
            try:
                # Parse message body
                message_data = json.loads(message.body.decode())
                logger.debug(f"Worker received message: {message_data}")
                
                # Execute jobs
                try:
                    await self._execute_jobs(message_data)
                    logger.info("Worker completed processing message successfully")
                    
                except ConsumerDemoJobException as job_error:
                    # Job execution failed - raise for debugging
                    logger.error(f"Worker: Job execution failed: {job_error}")
                    raise  # Re-raise for debugging
                except Exception as job_error:
                    # Unexpected error - raise for debugging
                    logger.error(f"Worker: Unexpected error: {job_error}")
                    raise  # Re-raise for debugging
                
            except json.JSONDecodeError as e:
                logger.error(f"Worker JSON decode error in queue '{self.queue_name}': {e}")
                raise  # Re-raise for debugging
                
            except Exception as e:
                logger.error(f"Worker processing error in queue '{self.queue_name}': {e}")
                raise  # Re-raise for debugging
    
    @abstractmethod
    async def _execute_jobs(self, message: dict) -> None:
        """
        Execute all related jobs for the message.
        
        Args:
            message: Message data dictionary
            
        Raises:
            Exception: If any job fails (for debugging)
        """
        pass

