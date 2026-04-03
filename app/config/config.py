from __future__ import annotations

from functools import lru_cache
from pydantic_settings import SettingsConfigDict
from pydantic import Field
from app.config.baseapp_config import BaseAppConfig


class Config(BaseAppConfig):
    """Application-specific settings extending BaseAppConfig."""

    # Note: env_file is inherited from BaseAppConfig which uses absolute path
    model_config = SettingsConfigDict(
        env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # SERP Configuration
    AI_MANAGEMENT_SERVICE_URL: str = Field(
        default="http://localhost:8901/ai-management-service/api/v1",
        env="AI_MANAGEMENT_SERVICE_URL"
    )
    AI_MANAGEMENT_SERVICE_API_KEY: str = Field(
        default="",
        env="AI_MANAGEMENT_SERVICE_API_KEY"
    )

    # SERP Workflow IDs
    SERP_WORKFLOW_GOOGLE_WEB: str = Field(default="", env="SERP_WORKFLOW_GOOGLE_WEB")
    SERP_WORKFLOW_GOOGLE_NEWS: str = Field(default="", env="SERP_WORKFLOW_GOOGLE_NEWS")
    SERP_WORKFLOW_GOOGLE_IMAGE: str = Field(default="", env="SERP_WORKFLOW_GOOGLE_IMAGE")
    SERP_WORKFLOW_BING_WEB: str = Field(default="", env="SERP_WORKFLOW_BING_WEB")
    SERP_WORKFLOW_BING_NEWS: str = Field(default="", env="SERP_WORKFLOW_BING_NEWS")
    SERP_WORKFLOW_BING_IMAGE: str = Field(default="", env="SERP_WORKFLOW_BING_IMAGE")

    # SERP Redis TTL (3 hours = 10800 seconds)
    SERP_REDIS_TTL: int = Field(default=10800, env="SERP_REDIS_TTL")

    def get_workflow_id(self, search_type: str) -> str:
        """Get workflow ID for a given search type."""
        workflow_map = {
            "google-web": self.SERP_WORKFLOW_GOOGLE_WEB,
            "google-news": self.SERP_WORKFLOW_GOOGLE_NEWS,
            "google-image": self.SERP_WORKFLOW_GOOGLE_IMAGE,
            "bing-web": self.SERP_WORKFLOW_BING_WEB,
            "bing-news": self.SERP_WORKFLOW_BING_NEWS,
            "bing-image": self.SERP_WORKFLOW_BING_IMAGE,
        }
        return workflow_map.get(search_type, "")


@lru_cache
def get_config() -> Config:
    """Get cached configuration instance."""
    return Config()


# Create config instance - will be cached until cache_clear() is called
config = get_config()
