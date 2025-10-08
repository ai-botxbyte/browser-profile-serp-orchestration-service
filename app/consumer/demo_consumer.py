from __future__ import annotations

import asyncio
import json
from datetime import datetime
from loguru import logger

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from app.consumer.baseapp_consumer import BaseAppConsumer

from app.config.baseapp_config import get_base_config
from app.job.demo_job import DemoJob
from app.schema.demo_orchestration_schema import DemoOrchestrationMessageSchema


class DemoConsumer(BaseAppConsumer):
    """Demo-specific consumer implementation following baseapp patterns"""
    
    def __init__(self):
        """
        Initialize the log consumer with connection.
        """
        # Create job (orchestrator)
        job = DemoJob()
        config = get_base_config()
        super().__init__(
            queue_name="demo_queue",  # Single queue for all services
            job_processor=job,
            config=config
        )
        logger.info(f"{self.__class__.__name__} initialized for log processing")
        
        # Establish connection during initialization
        asyncio.create_task(self.connect())
    
    async def start_consuming(self) -> None:
        """Start consuming messages from the queue"""
        if not self.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")
        
        logger.info(f"Starting to consume messages from: {self.queue_name}")
        
        # Start consuming and get consumer tag
        self.consumer_tag = await self.queue.consume(self.send_to_job)
        logger.info(f"Consumer started successfully for queue: {self.queue_name}")
        logger.info(f"Consumer tag: {self.consumer_tag}")
        
        # Keep the consumer running indefinitely
        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
    
    async def send_to_job(self, message: AbstractIncomingMessage) -> None:
        """Process a single message with validation - Template Method Pattern (aio_pika callback)"""
        async with message.process():
            try:
                # Parse message body
                message_data = json.loads(message.body.decode())
                logger.debug(f"Received message: {message_data}")
                
                # Validate message using schema
                validated_message = self._validate_message(message_data=message_data)
                
                # Skip processing if validation failed
                if validated_message is None:
                    logger.warning("Skipping invalid message")
                    return
                
                # Delegate to job processor (orchestrator)
                await self.job_processor.execute(message=validated_message)
                
                logger.info(f"Successfully processed message from queue: {self.queue_name}")
                        
            except json.JSONDecodeError as e:
                # Demo JSON decode error
                logger.error(f"JSON decode error in queue '{self.queue_name}': {e}")
                
                # Send to dead letter queue but don't raise
                await self._send_to_dead_letter_queue(message_body=message.body, error_reason=f"JSON Decode Error: {e}")
                
            except ValueError as e:  # From validation
                # Demo validation error
                logger.error(f"Validation error in queue '{self.queue_name}': {e}")
                
                # Send to dead letter queue but don't raise
                await self._send_to_dead_letter_queue(message_body=message.body, error_reason=f"Validation Error: {e}")
                
            except (RuntimeError, AttributeError, TypeError) as e:  # From job/service
                # Demo job processing error
                logger.error(f"Job processing error in queue '{self.queue_name}': {e}")
                
                # Send to dead letter queue but don't raise
                await self._send_to_dead_letter_queue(message_body=message.body, error_reason=f"Job Processing Error: {e}")
                
            # Consumer continues running - no exceptions raised!
    
    # [] Shall we ignore this function? or is it possible at the schema level end? 
    def _validate_message(self, message_data: dict) -> DemoOrchestrationMessageSchema:
        """
        Validate log message using DemoOrchestrationMessageSchema with built-in business rules
        """
        try:
            # Validate and parse message using Pydantic schema (includes business rules)
            demo_message = DemoOrchestrationMessageSchema(**message_data)
            print(demo_message,"demo_message")
            
            logger.debug(f"Validated demo message: {demo_message.data}")
            return demo_message
            
        except (ValueError, TypeError, KeyError) as e:
            # Demo the error with the raw message data for debugging
            logger.error("Message validation failed. Error: {}, Message data: {}", str(e), message_data)
            # Skip invalid messages instead of raising an error
            return None
    
    async def _send_to_dead_letter_queue(self,  message_body: bytes, error_reason: str) -> None:
        """
        Store failed message permanently in dead letter queue for monitoring and analysis
        """
        try:
            dead_letter_message = {
                "original_message": message_body.decode() if isinstance(message_body, bytes) else str(message_body),
                "error_reason": error_reason,
                "failed_at": datetime.utcnow().isoformat(),
                "queue_name": self.queue_name,
                "service_name": getattr(self, 'service_name', 'unknown'),
                "message_id": f"dlq_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{hash(message_body) % 10000}"
            }
            
            # Store in dead letter queue (permanent storage)
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    json.dumps(dead_letter_message).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers={
                        "x-dead-letter-reason": error_reason,
                        "x-failed-at": dead_letter_message["failed_at"],
                        "x-original-queue": self.queue_name
                    }
                ),
                routing_key=f"{self.queue_name}_dead_letter"
            )
            
            # Monitoring and alerting
            logger.error("  FAILED MESSAGE STORED IN DLQ")
            logger.error(f"   Queue: {self.queue_name}")
            logger.error(f"   Error: {error_reason}")
            logger.error(f"   Message ID: {dead_letter_message['message_id']}")
            logger.error(f"   Failed at: {dead_letter_message['failed_at']}")
            
            
            
        except (ConnectionError, RuntimeError, AttributeError) as e:
            logger.critical(f"CRITICAL: Failed to store message in dead letter queue: {e}")
            logger.critical("This message will be permanently lost!")
            # Don't re-raise to avoid infinite error loops