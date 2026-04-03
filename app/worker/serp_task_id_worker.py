"""Run SERP Task ID Worker as standalone service - polls results and stores in Redis"""

import asyncio
import sys
import os

# Add project root to Python path BEFORE importing app modules
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from loguru import logger
from app.consumer.serp_task_id_consumer import SerpTaskIdConsumer
from app.job.serp_task_id_job import SerpTaskIdJob
from app.config.config import get_config


async def main():
    """Main entry point for SERP Task ID Worker"""
    # Configure loguru for production
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
        level="INFO"
    )

    logger.info("Starting SERP Task ID Worker...")

    # Create task ID job
    task_id_job = SerpTaskIdJob()

    # Create consumer
    consumer = SerpTaskIdConsumer(
        config=get_config(),
        job_processor=task_id_job
    )

    try:
        # Connect and start consuming
        await consumer.connect()
        await consumer.start_consuming()

    except (ConnectionError, RuntimeError, asyncio.TimeoutError) as e:
        logger.error(f"SERP Task ID Worker error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("SERP Task ID Worker interrupted by user")
    finally:
        # Cleanup job resources
        await task_id_job.cleanup()
        await consumer.disconnect()
        logger.info("SERP Task ID Worker shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("SERP Task ID Worker interrupted by user")
        sys.exit(0)
    except (SystemExit, RuntimeError) as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
