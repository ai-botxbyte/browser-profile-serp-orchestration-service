"""Demo B Orchestration Service"""

from __future__ import annotations
from typing import Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.service.baseapp_service import BaseAppService


class DemoBOrchestrationService(BaseAppService):
    """Service for Demo B orchestration operations"""
    
    def __init__(self, db: AsyncSession):
        super().__init__(db=db)
    
    async def validate_name(self, data: dict) -> Dict[str, Any]:
        """
        Validate name field.
        
        Args:
            data: Data containing name to validate
            
        Returns:
            {
                "valid": bool,
                "errors": list of error messages
            }
        """
        errors = []
        
        if "name" not in data:
            errors.append("Missing required field: name")
        else:
            name = data.get("name", "")
            if not name or len(name) < 1:
                errors.append("Name cannot be empty")
            elif len(name) > 200:
                errors.append("Name must not exceed 200 characters")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
