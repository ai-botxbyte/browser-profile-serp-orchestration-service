from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession


class BaseAppService:
    """Base service class that can be reused by all services."""
    
    def __init__(self, db: AsyncSession):
        """
        Initialize the base service.
        
        Args:
            db: Database session
        """
        self.db = db
        