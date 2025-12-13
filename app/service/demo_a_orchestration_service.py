"""Demo A Orchestration Service"""

from __future__ import annotations
from typing import Dict, Any
from uuid import UUID
from pydantic import AnyHttpUrl

from sqlalchemy.ext.asyncio import AsyncSession

from app.service.baseapp_service import BaseAppService


class DemoAOrchestrationService(BaseAppService):
    """Service for Demo A orchestration operations"""
    
    def __init__(self, db: AsyncSession):
        super().__init__(db=db)
    
    async def validate_creation_data(self, data: dict) -> Dict[str, Any]:
        """
        Validate Demo A creation data including social accounts.
        
        Args:
            data: Creation data to validate
            
        Returns:
            {
                "valid": bool,
                "errors": list of error messages
            }
        """
        errors = []
        
        required_fields = ["name", "user_id", "workspace_id"]
        for field in required_fields:
            if field not in data:
                errors.append(f"Missing required field: {field}")
        
        if "name" in data:
            name = data.get("name", "")
            if not name or len(name) < 1 or len(name) > 200:
                errors.append("Demo A name must be between 1 and 200 characters")
        
        if "user_id" in data:
            try:
                UUID(data["user_id"])
            except (ValueError, TypeError):
                errors.append("user_id must be a valid UUID")
        
        if "workspace_id" in data:
            try:
                UUID(data["workspace_id"])
            except (ValueError, TypeError):
                errors.append("workspace_id must be a valid UUID")
        
        if data.get("description"):
            description = data["description"]
            if len(description) > 500:
                errors.append("Description must not exceed 500 characters")
        
        if data.get("age"):
            age = data["age"]
            if not isinstance(age, int) or age <= 0 or age >= 150:
                errors.append("Age must be between 1 and 149")
        
        if data.get("progress"):
            progress = data["progress"]
            if not isinstance(progress, (int, float)) or progress < 0.0 or progress > 100.0:
                errors.append("Progress must be between 0.0 and 100.0")
        
        if data.get("social_accounts"):
            social_errors = self._validate_social_accounts(data["social_accounts"])
            errors.extend(social_errors)
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
    
    def _validate_social_accounts(self, social_accounts: list) -> list:
        """
        Validate social accounts based on SocialAccountSchema.
        
        Args:
            social_accounts: List of social account dictionaries
            
        Returns:
            List of error messages
        """
        errors = []
        
        if not isinstance(social_accounts, list):
            errors.append("social_accounts must be a list")
            return errors
        
        for idx, account in enumerate(social_accounts):
            if not isinstance(account, dict):
                errors.append(f"Social account {idx}: must be an object")
                continue
            
            required_fields = ["platform", "username", "url", "followers", "verified"]
            for field in required_fields:
                if field not in account:
                    errors.append(f"Social account {idx}: missing required field '{field}'")
            
            if "platform" in account:
                platform = account.get("platform", "")
                if not platform or len(platform) < 1 or len(platform) > 50:
                    errors.append(
                        f"Social account {idx}: platform must be between 1 and 50 characters"
                        )
            
            if "username" in account:
                username = account.get("username", "")
                if not username or len(username) < 1 or len(username) > 50:
                    errors.append(
                        f"Social account {idx}: username must be between 1 and 50 characters"
                        )
            
            if "url" in account:
                url = account.get("url", "")
                if not url:
                    errors.append(f"Social account {idx}: url is required")
                else:
                    try:
                        AnyHttpUrl(url)
                    except (ValueError, TypeError):
                        errors.append(f"Social account {idx}: url must be a valid HTTP/HTTPS URL")
            
            if "followers" in account:
                followers = account.get("followers")
                if not isinstance(followers, int) or followers <= 0:
                    errors.append(f"Social account {idx}: followers must be a positive integer")
            
            if "verified" in account:
                verified = account.get("verified")
                if not isinstance(verified, bool):
                    errors.append(f"Social account {idx}: verified must be a boolean")
        
        return errors
    
    async def validate_social_accounts(self, data: dict) -> Dict[str, Any]:
        """
        Validate social accounts data.
        
        Args:
            data: Data containing social_accounts to validate
            
        Returns:
            {
                "valid": bool,
                "errors": list of error messages
            }
        """
        errors = []
        
        if "social_accounts" not in data:
            errors.append("Missing required field: social_accounts")
        else:
            social_errors = self._validate_social_accounts(data["social_accounts"])
            errors.extend(social_errors)
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
