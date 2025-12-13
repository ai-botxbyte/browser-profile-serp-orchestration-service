from app.exception.baseapp_exception import BaseAppException


class ConsumerDemoException(BaseAppException):
    """Base exception for all Consumer Demo related errors."""


class ConsumerDemoValidationException(ConsumerDemoException):
    """Exception for validation errors in consumer"""
    
    def __init__(self, queue_name: str, validation_error: str):
        message = f"Message validation failed for queue '{queue_name}': {validation_error}"
        super().__init__(message=message)



class JobDemoServiceException(ConsumerDemoException):
    """Exception for service errors in jobs"""
    
    def __init__(self, job_name: str, service_name: str, service_error: str):
        message = f"Service '{service_name}' failed in job '{job_name}': {service_error}"
        super().__init__(message=message)


class WorkerDemoException(ConsumerDemoException):
    """Exception for worker orchestration errors"""
    
    def __init__(self, worker_name: str, error: str):
        message = f"Worker '{worker_name}' failed: {error}"
        super().__init__(message=message)


class WorkerJobExecutionException(WorkerDemoException):
    """Exception for job execution errors in workers"""
    
    def __init__(self, worker_name: str, job_name: str, error: str):
        error_message = f"Job '{job_name}' execution failed: {error}"
        super().__init__(worker_name=worker_name, error=error_message)