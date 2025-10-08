import asyncio
import signal
import sys
import os
from loguru import logger

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from app.consumer.demo_consumer import DemoConsumer



async def main():
    """Main entry point for DemoConsumer as standalone service"""
    # Configure loguru for production
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
        level="INFO"
    )
    
    # Create consumer (job is created internally)
    consumer = DemoConsumer()
    
    # Setup signal handlers for graceful shutdown
    shutdown_event = asyncio.Event()
    
    def signal_handler(signum, _frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Connect and start consuming
        await consumer.connect()
        consume_task = asyncio.create_task(consumer.start_consuming())
        # [] Shall we make shutdown_task only at local level? Because in kubernetes we can't shutdown this way.
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        
        # Wait for either completion or shutdown
        done, pending = await asyncio.wait(
            [consume_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel pending tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Check for errors
        if consume_task in done:
            try:
                consume_task.result()
            except Exception as e:
                logger.error(f"Demo consumer failed: {e}")
                raise
                
    except (ConnectionError, RuntimeError, asyncio.TimeoutError) as e:
        logger.error(f"Demo consumer service error: {e}")
        sys.exit(1)
    finally:
        await consumer.disconnect()
        logger.info("Demo Consumer service shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Demo Consumer interrupted by user")
        sys.exit(0)
    except (SystemExit, RuntimeError) as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)