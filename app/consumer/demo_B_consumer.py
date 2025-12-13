from __future__ import annotations

import asyncio
from loguru import logger
import json

from aio_pika.abc import AbstractIncomingMessage

from app.consumer.baseapp_consumer import BaseAppConsumer
from app.config.baseapp_config import get_base_config
from app.job.demo_b1_job import DemoB1Job
from app.exception.consumer_demo_exception import ConsumerDemoJobException


class DemoBConsumer(BaseAppConsumer):
    """
    Demo B consumer - consumes messages and executes jobs.
    """
    
    def __init__(self):
        """
        Initialize the demo B consumer with job handlers.
        """
        config = get_base_config()
        
        # Initialize job handlers
        self.job_handlers: dict[str, any] = {
            'job1': DemoB1Job(),  # Name Validation job
        }
        
        # Create a job processor function that routes to the appropriate job
        async def job_processor(message_data: dict) -> None:
            await self._execute_jobs(message_data)
        
        super().__init__(
            queue_name="demo_B_queue",
            config=config
        )
        
        logger.info(
            f"{self.__class__.__name__} initialized with {len(self.job_handlers)} job handlers: "
            f"{list(self.job_handlers.keys())}"
        )
    
    async def _execute_jobs(self, message: dict) -> None:
        """
        Execute the appropriate job based on job_type.
        
        Args:
            message: Message data dictionary
            
        Raises:
            ConsumerDemoJobException: If job_type not found or job fails
        """
        job_type = message.get("job_type")
        
        logger.info(f"Consumer: Starting job execution for job_type: {job_type}")
        
        # Get appropriate job handler
        job_handler = self.job_handlers.get(job_type)
        
        if job_handler is None:
            error_msg = f"No job handler found for job_type: {job_type}"
            logger.error(error_msg)
            raise ConsumerDemoJobException(
                queue_name=self.queue_name,
                job_name="Unknown",
                job_error=error_msg
            )
        
        job_name = job_handler.__class__.__name__
        logger.info(f"Consumer: Routing to {job_name}")
        
        # Execute job
        await job_handler.execute(message=message)
        logger.info(f"Consumer: Job {job_name} completed successfully")
    
    async def start_consuming(self) -> None:
        """Start consuming messages from demo_B_queue"""
        if not self.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")
        
        logger.info(f"Starting to consume messages from: {self.queue_name}")
        
        async def consume_message(message: AbstractIncomingMessage) -> None:
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
                        
                    except ConsumerDemoJobException as job_error:
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
        
        # Start consuming
        await self.queue.consume(consume_message)
        
        # Keep the consumer running indefinitely
        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
    
