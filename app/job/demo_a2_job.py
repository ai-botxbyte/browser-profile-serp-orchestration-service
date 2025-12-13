"""Demo A2 Job - Auto Tagging Workflow"""

from __future__ import annotations
from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.exception.consumer_demo_exception import JobDemoServiceException


class DemoA2Job(BaseAppJob):
    """Job handler for Demo A - Job Type 2 (Auto Tagging)"""
    
    def __init__(self):
        super().__init__()
        logger.info(f"{self.__class__.__name__} initialized")

    async def execute(self, message: dict) -> None:
        """Process auto tagging message."""
        tagging_data = message.get("data")
        
        logger.info("Starting Demo A2 Job - Auto Tagging")
        
        try:
            logger.info("Step 1: Analyzing content for tags")
            analysis = await self._analyze_content(tagging_data)
            
            logger.info("Step 2: Generating tags")
            tags = await self._generate_tags(analysis)
            
            logger.info("Step 3: Applying tags")
            await self._apply_tags(tagging_data, tags)
            
            logger.info(f"✓ Demo A2 Job completed: {len(tags)} tags applied")
            
        except ValueError as validation_error:
            logger.error(f"Validation error: {validation_error}")
            raise JobDemoServiceException(
                job_name=self.__class__.__name__,
                service_name="DemoAService",
                service_error=f"Validation failed: {validation_error}"
            ) from validation_error
            
        except Exception as e:
            logger.error(f"Demo A2 Job failed: {e}")
            raise JobDemoServiceException(
                job_name=self.__class__.__name__,
                service_name="DemoAService",
                service_error=str(e)
            ) from e
    
    async def _analyze_content(self, tagging_data: dict) -> dict:
        """Analyze content - testing only."""
        print(f"{self.__class__.__name__} successful")
        return {}
    
    async def _generate_tags(self, analysis: dict) -> list:
        """Generate tags - testing only."""
        print(f"{self.__class__.__name__} successful")
        return []
    
    async def _apply_tags(self, _tagging_data: dict, tags: list) -> dict:
        """Apply tags - testing only."""
        print(f"{self.__class__.__name__} successful")
        return {}
