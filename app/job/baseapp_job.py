from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional
from datetime import datetime
from loguru import logger


class BaseAppJob(ABC):
    """Base job class that can be reused by all jobs following baseapp patterns"""
    
    def __init__(self):
        """
        Initialize the base job.
        """
        self.job_name = self.__class__.__name__
        self.start_time: Optional[datetime] = None
        logger.info(f"{self.job_name} initialized")
    
    @abstractmethod
    async def execute(self,  message: Any) -> None:
        """
        Process a single message - Must be implemented by subclasses
        """