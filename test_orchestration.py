"""Test script for orchestration service - publishes messages to queues"""

import asyncio
import json
import sys
import os
from uuid import uuid4

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

import aio_pika
from loguru import logger
from app.config.baseapp_config import get_base_config


async def publish_test_messages():
    """Publish test messages to demo_A_queue and demo_B_queue"""
    
    # Configure logger
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        level="INFO"
    )
    
    config = get_base_config()
    
    try:
        # Connect to RabbitMQ
        logger.info("=" * 60)
        logger.info("Connecting to RabbitMQ...")
        connection = await aio_pika.connect_robust(config.RABBITMQ_URL)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        
        logger.info("✓ Connected to RabbitMQ successfully!")
        logger.info("")
        
        # Declare queues with priority support
        logger.info("Declaring queues...")
        demo_a_queue = await channel.declare_queue(
            "demo_A_queue",
            durable=True,
            auto_delete=False,
            arguments={"x-max-priority": 10}
        )
        demo_b_queue = await channel.declare_queue(
            "demo_B_queue",
            durable=True,
            auto_delete=False,
            arguments={"x-max-priority": 10}
        )
        logger.info("✓ Queues declared successfully!")
        logger.info("")
        
        # ========== Demo A Queue Messages ==========
        logger.info("=" * 60)
        logger.info("Publishing messages to demo_A_queue...")
        logger.info("Note: Each message will automatically trigger both jobs:")
        logger.info("  - DemoA1Job (Social Validation)")
        logger.info("  - DemoA2Job (Auto Tagging)")
        logger.info("")
        
        # Demo A - Message 1 (will trigger both jobs)
        demo_a_message1 = {
            "data": {
                "name": "Test Userxx",
                "user_id": str(uuid4()),
                "workspace_id": str(uuid4()),
                "social_accounts": [
                    {
                        "platform": "twitter",
                        "username": "testuser",
                        "url": "https://twitter.com/testuser",
                        "followers": 1000,
                        "verified": True
                    }
                ]
            }
        }
        
        await channel.default_exchange.publish(
            aio_pika.Message(
                json.dumps(demo_a_message1).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                priority=5
            ),
            routing_key="demo_A_queue"
        )
        logger.info(f"✓ Published Demo A - Message 1")
        logger.info(f"   Will execute: DemoA1Job → DemoA2Job")
        logger.info("")
        
        # Demo A - Message 2 (will trigger both jobs)
        demo_a_message2 = {
            "data": {
                "name": "Test Content",
                "description": "This is a test content for auto tagging",
                "user_id": str(uuid4()),
                "workspace_id": str(uuid4())
            }
        }
        
        await channel.default_exchange.publish(
            aio_pika.Message(
                json.dumps(demo_a_message2).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                priority=5
            ),
            routing_key="demo_A_queue"
        )
        logger.info(f"✓ Published Demo A - Message 2")
        logger.info(f"   Will execute: DemoA1Job → DemoA2Job")
        logger.info("")
        
        # ========== Demo B Queue Messages ==========
        logger.info("=" * 60)
        logger.info("Publishing messages to demo_B_queue...")
        logger.info("Note: Each message will automatically trigger DemoB1Job (Name Validation)")
        logger.info("")
        
        # Demo B - Message 1 (will trigger DemoB1Job)
        demo_b_message1 = {
            "data": {
                "name": "John Doe"
            }
        }
        
        await channel.default_exchange.publish(
            aio_pika.Message(
                json.dumps(demo_b_message1).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                priority=5
            ),
            routing_key="demo_B_queue"
        )
        logger.info(f"✓ Published Demo B - Message 1")
        logger.info(f"   Will execute: DemoB1Job")
        logger.info("")
        
        # Close connection
        await connection.close()
        
        logger.info("=" * 60)
        logger.info("✓ All test messages published successfully!")
        logger.info("")
        logger.info("Next steps:")
        logger.info("1. Make sure both workers are running:")
        logger.info("   - python app/worker/run_demo_A_worker.py")
        logger.info("   - python app/worker/run_demo_B_worker.py")
        logger.info("2. Check the worker logs to see job execution")
        logger.info("3. Expected behavior:")
        logger.info("   - Demo A messages: Will execute DemoA1Job → DemoA2Job (both jobs)")
        logger.info("   - Demo B messages: Will execute DemoB1Job (single job)")
        logger.info("4. You should see 'DemoA1Job successful', 'DemoA2Job successful',")
        logger.info("   and 'DemoB1Job successful' printed in the worker outputs")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error publishing messages: {e}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(publish_test_messages())
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

