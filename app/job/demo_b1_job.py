"""Demo B1 Job - Name Validation Workflow"""

from __future__ import annotations
from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.exception.consumer_demo_exception import JobDemoServiceException


class DemoB1Job(BaseAppJob):
    """Job handler for Demo B - Job Type 1 (Name Validation)"""
    
    def __init__(self):
        super().__init__()
        logger.info(f"{self.__class__.__name__} initialized")

        
    async def execute(self, message: dict) -> None:
        """Process name validation message."""
        validation_data = message.get("data")
        
        logger.info("Starting Demo B1 Job - Name Validation")
        
        try:
            logger.info("Step 1: Validating name")
            validation = await self._validate_name(validation_data)
            
            if not validation["valid"]:
                logger.error(f"Name validation failed: {validation['errors']}")
                raise ValueError(f"Name validation failed: {', '.join(validation['errors'])}")
            
            logger.info("Step 2: Processing name validation")
            await self._process_name_validation(validation_data)
            
            logger.info("✓ Demo B1 Job completed: Name validation successful")
            
        except ValueError as validation_error:
            logger.error(f"Validation error: {validation_error}")
            raise JobDemoServiceException(
                job_name=self.__class__.__name__,
                service_name="DemoBService",
                service_error=f"Validation failed: {validation_error}"
            ) from validation_error
            
        except Exception as e:
            logger.error(f"Demo B1 Job failed: {e}")
            raise JobDemoServiceException(
                job_name=self.__class__.__name__,
                service_name="DemoBService",
                service_error=str(e)
            ) from e
    
    async def _validate_name(self, validation_data: dict) -> dict:
        """Validate name - testing only."""
        print(f"{self.__class__.__name__} successful")
        return {"valid": True, "errors": []}
    
    async def _process_name_validation(self, validation_data: dict) -> dict:
        """Process name validation - testing only."""
        print(f"{self.__class__.__name__} successful")
        return {}
