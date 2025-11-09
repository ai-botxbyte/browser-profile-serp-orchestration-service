from __future__ import annotations

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class BaseAppConfig(BaseSettings):
    """Base configuration class with common settings that can be reused by all config classes."""
    
    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8", 
        case_sensitive=False, 
        extra="ignore"
    )
    
    # Common application settings
    ENV: str = Field(default="development", env="ENV")
    APP_NAME: str = Field(default="DemoOrchestrationService", env="APP_NAME")
    APP_VERSION: str = Field(default="2.0.1", env="APP_VERSION")
    PORT: int = Field(default=8801, env="PORT")
    STATUS: str = Field(default="active", env="STATUS")
    
    # [x]: We also need to add the Dtabase url for Read Replica.
    # Database settings
    POSTGRES_HOST: str = Field(default="localhost", env="POSTGRES_HOST")
    POSTGRES_PORT: int = Field(default=5432, env="POSTGRES_PORT")
    POSTGRES_DB: str = Field(default="demo_orchestration_db", env="POSTGRES_DB")
    POSTGRES_USER: str = Field(default="postgres", env="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field(default="postgres", env="POSTGRES_PASSWORD")
    DATABASE_URL: str = Field(
        default="postgresql+psycopg2://postgres:postgres@localhost:5432/demo_orchestration_db",
        env="DATABASE_URL"
    )
    ASYNC_DATABASE_URL: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/demo_orchestration_db",
        env="ASYNC_DATABASE_URL"
    )
    
    # Read Replica Database settings
    POSTGRES_READ_HOST: str = Field(default="localhost", env="POSTGRES_READ_HOST")
    POSTGRES_READ_PORT: int = Field(default=5432, env="POSTGRES_READ_PORT")
    POSTGRES_READ_DB: str = Field(default="demo_orchestration_db", env="POSTGRES_READ_DB")
    POSTGRES_READ_USER: str = Field(default="postgres", env="POSTGRES_READ_USER")
    POSTGRES_READ_PASSWORD: str = Field(default="postgres", env="POSTGRES_READ_PASSWORD")
    READ_DATABASE_URL: str = Field(
        default="postgresql+psycopg2://postgres:postgres@localhost:5432/demo_orchestration_db",
        env="READ_DATABASE_URL"
    )
    ASYNC_READ_DATABASE_URL: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/demo_orchestration_db",
        env="ASYNC_READ_DATABASE_URL"
    )
    

    
    
    # CORS settings
    CORS_ORIGINS: str = Field(default="*", env="CORS_ORIGINS")
    
    # Media settings
    MEDIA_PATH: str = Field(default="app/media", env="MEDIA_PATH")
    LOGO_SUBDIR: str = Field(default="logo", env="LOGO_SUBDIR")
    
    # Cache and messaging settings
    REDIS_URL: str = Field(default="redis://localhost:6379/0", env="REDIS_URL")
    RABBITMQ_URL: str = Field(default="amqp://guest:guest@localhost:5672/", env="RABBITMQ_URL")
    
    # Database routing settings
    USE_READ_REPLICA: bool = Field(default=False, env="USE_READ_REPLICA")
    
    # Debug mode settings
    IS_DEBUG_MODE: bool = Field(default=False, env="IS_DEBUG_MODE")
    

    def get_database_url(self, async_mode: bool = False, read_replica: bool = False) -> str:
        """Get database URL based on async mode and read replica preference."""
        if read_replica:
            return self.ASYNC_READ_DATABASE_URL if async_mode else self.READ_DATABASE_URL
        return self.ASYNC_DATABASE_URL if async_mode else self.DATABASE_URL
    
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return str(self.ENV).lower() in ["production", "prod"]
    
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return str(self.ENV).lower() in ["development", "dev"]

#  orchestration services: it returns a single cached BaseAppConfig per process, avoids re-parsing #  env files on each call, and is cheap and safe."
@lru_cache
def get_base_config() -> BaseAppConfig:
    """Get cached base configuration instance."""
    return BaseAppConfig()
