from __future__ import annotations

from abc import ABC
from typing import Optional, Any, Union

import json
import asyncio
import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from loguru import logger

from app.exception.baseapp_exception import ConsumerJobException
from app.job.baseapp_job import BaseAppJob

class BaseAppConsumer(ABC):
    """Base consumer class that can be reused by all consumers following baseapp patterns"""
    
    def __init__(
        self,
        queue_name: str, 
        config: Any, 
        job_processor: Optional[Union[list[BaseAppJob], BaseAppJob]] = None
        ):
        """
        Initialize the base consumer.
        
        Args:
            queue_name: Name of the RabbitMQ queue
            config: Configuration object
            job_processor: Single job or list of jobs to execute (optional)
        """
        self.queue_name = queue_name
        self.config = config
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queue: Optional[aio_pika.Queue] = None
        self.job_processor = job_processor
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
            

            self.queue = await self.channel.declare_queue(
                self.queue_name,
                durable=True,
                auto_delete=False,
                arguments={
                    "x-max-priority": 10  # Support priority levels 0-10
                }
            )

            await self.channel.declare_queue(
                f"{self.queue_name}_dlx",
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
    
    
    async def _consume_message(self, message: AbstractIncomingMessage) -> None:
        """Handle incoming message"""
        async with message.process():
            try:
                # Parse message body
                message_data = json.loads(message.body.decode())
                logger.debug(f"Consumer received message: {message_data}")
                
                # Execute jobs
                try:
                    await self._execute_jobs(message_data)
                    logger.info("Consumer completed processing message successfully")
                    
                except ConsumerJobException as job_error:
                    logger.error(f"Consumer: Job execution failed: {job_error}")
                    raise  # Re-raise for dead letter queue
                except Exception as job_error:
                    logger.error(f"Consumer: Unexpected error: {job_error}")
                    raise  # Re-raise for dead letter queue
                
            except json.JSONDecodeError as e:
                logger.error(f"Consumer JSON decode error: {e}")
                raise  # Re-raise for dead letter queue
            except Exception as e:
                logger.error(f"Consumer processing error: {e}")
                raise  # Re-raise for dead letter queue

    
    async def start_consuming(self) -> None:
        """Start consuming messages from demo_A_queue"""
        if not self.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")
        
        logger.info(f"Starting to consume messages from: {self.queue_name}")
        
        # Start consuming
        await self.queue.consume(self._consume_message)
        
        # Keep the consumer running indefinitely
        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")

    async def _execute_jobs(self, message: dict) -> None:
        """
        Execute all jobs in the job processor.
        
        Args:
            message: Message data dictionary
            
        Raises:
            ConsumerJobException: If any job fails
        """
        # Convert single job to list for uniform processing
        jobs = self.job_processor if isinstance(self.job_processor, list) else [self.job_processor]
        
        logger.info(f"Consumer: Starting execution of {len(jobs)} job(s)")
        
        # Execute each job in sequence
        for job in jobs:
            job_name = job.__class__.__name__
            logger.info(f"Consumer: Executing {job_name}")
            
            try:
                await job.execute(message=message)
                logger.info(f"Consumer: Job {job_name} completed successfully")
            except Exception as e:
                logger.error(f"Consumer: Job {job_name} failed: {e}")
                raise ConsumerJobException(
                    queue_name=self.queue_name,
                    job_name=job_name,
                    job_error=str(e)
                ) from e