"""Run Demo B Consumer as standalone service"""

import asyncio
import sys
import os

# Add project root to Python path BEFORE importing app modules
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from loguru import logger
from app.consumer.demo_B_consumer import DemoBConsumer


async def main():
    """Main entry point for DemoBConsumer as standalone service"""
    # Configure loguru for production
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
        level="INFO"
    )
    
    # Create consumer
    consumer = DemoBConsumer()
    
    try:
        # Connect and start consuming
        await consumer.connect()
        await consumer.start_consuming()
                
    except (ConnectionError, RuntimeError, asyncio.TimeoutError) as e:
        logger.error(f"Demo B consumer service error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Demo B Consumer interrupted by user")
    finally:
        await consumer.disconnect()
        logger.info("Demo B Consumer service shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Demo B Consumer interrupted by user")
        sys.exit(0)
    except (SystemExit, RuntimeError) as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

