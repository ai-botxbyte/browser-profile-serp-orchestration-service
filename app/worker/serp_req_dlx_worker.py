"""Run SERP Request DLX Worker as standalone service - infinite retry"""

import asyncio
import sys
import os

# Add project root to Python path BEFORE importing app modules
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from loguru import logger
from app.consumer.serp_req_dlx_consumer import SerpReqDlxConsumer
from app.config.config import get_config


async def main():
    """Main entry point for SERP Request DLX Worker"""
    # Configure loguru for production
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
        level="INFO"
    )

    logger.info("Starting SERP Request DLX Worker...")

    # Create DLX consumer (infinite retry - no job processor)
    consumer = SerpReqDlxConsumer(config=get_config())

    try:
        # Connect and start consuming
        await consumer.connect()
        await consumer.start_consuming()

    except (ConnectionError, RuntimeError, asyncio.TimeoutError) as e:
        logger.error(f"SERP Request DLX Worker error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("SERP Request DLX Worker interrupted by user")
    finally:
        await consumer.disconnect()
        logger.info("SERP Request DLX Worker shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("SERP Request DLX Worker interrupted by user")
        sys.exit(0)
    except (SystemExit, RuntimeError) as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
