"""Demo A1 Job - Social Validation Workflow"""

from __future__ import annotations
from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.exception.consumer_demo_exception import JobDemoServiceException


class DemoA1Job(BaseAppJob):
    """Job handler for Demo A - Job Type 1 (Social Validation)"""
    
    def __init__(self):
        super().__init__()
        logger.info(f"{self.__class__.__name__} initialized")

    
    async def execute(self, message: dict) -> None:
        """Execute the job."""
        await self.process_message(message=message)
        
    async def process_message(self, message: dict) -> None:
        """Process social validation message."""

        validation_data = message.get("data")
        
        logger.info("Starting Demo A1 Job - Social Validation")
        
        try:
            logger.info("Step 1: Validating social accounts")
            validation = await self._validate_social_accounts(validation_data)
            
            if not validation["valid"]:
                logger.error(f"Social validation failed: {validation['errors']}")
                raise ValueError(f"Social validation failed: {', '.join(validation['errors'])}")
            
            logger.info("Step 2: Processing social accounts")
            await self._process_social_validation(validation_data)
            
            logger.info("✓ Demo A1 Job completed: Social validation successful")
            
        except ValueError as validation_error:
            logger.error(f"Validation error: {validation_error}")
            raise JobDemoServiceException(
                job_name=self.__class__.__name__,
                service_name="DemoAService",
                service_error=f"Validation failed: {validation_error}"
            ) from validation_error
            
        except Exception as e:
            logger.error(f"Demo A1 Job failed: {e}")
            raise JobDemoServiceException(
                job_name=self.__class__.__name__,
                service_name="DemoAService",
                service_error=str(e)
            ) from e
    
    async def _validate_social_accounts(self, validation_data: dict) -> dict:
        """Validate social accounts - testing only."""
        print(f"{self.__class__.__name__} successful")
        return {"valid": True, "errors": []}
    
    async def _process_social_validation(self, validation_data: dict) -> dict:
        """Process social validation - testing only."""
        print(f"{self.__class__.__name__} successful")
        return {}
