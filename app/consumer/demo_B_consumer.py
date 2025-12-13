from __future__ import annotations

import asyncio
from loguru import logger

from app.consumer.baseapp_consumer import BaseAppConsumer
from app.config.baseapp_config import get_base_config
from app.job.demo_b1_job import DemoB1Job
from app.exception.baseapp_exception import ConsumerJobException
from app.job.baseapp_job import BaseAppJob


class DemoBConsumer(BaseAppConsumer):
    """
    Demo B consumer - consumes messages and executes jobs.
    """
    

    

    
    
