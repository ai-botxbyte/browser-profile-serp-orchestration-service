from __future__ import annotations

import asyncio
from loguru import logger
from typing import Any
from app.consumer.baseapp_consumer import BaseAppConsumer
from app.config.baseapp_config import get_base_config
from app.exception.baseapp_exception import ConsumerJobException
from app.job.baseapp_job import BaseAppJob


class DemoAConsumer(BaseAppConsumer):
    """
    Demo A consumer - consumes messages and executes jobs.
    """
    


        
   