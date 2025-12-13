from typing import Optional


class BaseAppException(Exception):
    """Base exception for all application errors."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class NotFoundException(BaseAppException):
    """Exception for when a resource is not found."""

    def __init__(self, resource: str, resource_id: Optional[str] = None):
        message = f"{resource} not found."
        if resource_id:
            message = f"{resource} with ID {resource_id} not found."
        super().__init__(message=message)


class AlreadyExistsException(BaseAppException):
    """Exception for when a resource already exists."""

    def __init__(self, resource: str, field: str = "name"):
        message = f"{resource} with this {field} already exists."
        super().__init__(message=message)


class InvalidDataException(BaseAppException):
    """Exception for when invalid data is provided."""

    def __init__(self, message: str = "Invalid data provided."):
        super().__init__(message=message)


class PermissionDeniedException(BaseAppException):
    """Exception for when a user does not have permission to perform an action."""

    def __init__(
        self, message: str = "You do not have permission to perform this action."
    ):
        super().__init__(message=message)


class UnauthorizedException(BaseAppException):
    """Exception for when a user is not authorized to perform an action."""

    def __init__(self, message: str = "Authentication required."):
        super().__init__(message=message)


class ConflictException(BaseAppException):
    """Exception for when a conflict occurs."""

    def __init__(self, message: str = "Conflict occurred."):
        super().__init__(message=message)


class DependencyException(BaseAppException):
    """Exception for when an operation cannot be completed due to dependencies."""

    def __init__(
        self, message: str = "Operation cannot be completed due to dependencies."
    ):
        super().__init__(message=message)


class RateLimitExceededException(BaseAppException):
    """Exception for when a rate limit is exceeded."""

    def __init__(self, message: str = "Rate limit exceeded. Please try again later."):
        super().__init__(message=message)


class ServiceUnavailableException(BaseAppException):
    """Exception for when a service is temporarily unavailable."""

    def __init__(self, message: str = "Service is temporarily unavailable."):
        super().__init__(message=message)


class InternalServerErrorException(BaseAppException):
    """Exception for when an unexpected error occurs."""

    def __init__(self, message: str = "An unexpected error occurred."):
        super().__init__(message=message)


class ConsumerJobException(BaseAppException):
    """Exception for job execution errors in consumer"""
    
    def __init__(self, queue_name: str, job_name: str, job_error: str):
        self.queue_name = queue_name
        self.job_name = job_name
        self.job_error = job_error
        message = f"Job '{job_name}' failed for queue '{queue_name}': {job_error}"
        super().__init__(message=message)
