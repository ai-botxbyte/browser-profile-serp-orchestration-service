# [] Remove This Folder
from fastapi import APIRouter

from app.api.v1.endpoints.health_endpoint import router as health_router

api_router: APIRouter = APIRouter()

api_router.include_router(router=health_router, prefix="/demo-orchestration", tags=["Health"])  # /demo-orchestration/health/
