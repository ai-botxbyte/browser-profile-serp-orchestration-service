from typing import Optional
# [] HTTPException = Exception and we need make some changes to it right? 
# What about : BaseConsumerException as a file name?
from fastapi import HTTPException, status


class BaseAppException(HTTPException):
    """Base exception for all application errors."""

    def __init__(self, message: str, status_code: int = status.HTTP_400_BAD_REQUEST):
        super().__init__(status_code=status_code, detail=message)


class NotFoundException(BaseAppException):
    """Exception raised when a resource is not found."""
    def __init__(self, resource: str, resource_id: Optional[str] = None):
        message = f"{resource} not found."
        if resource_id:
            message = f"{resource} with ID {resource_id} not found."
        super().__init__(message=message, status_code=status.HTTP_404_NOT_FOUND)


class AlreadyExistsException(BaseAppException):
    """Exception raised when a resource already exists."""
    def __init__(self, resource: str, field: str = "name"):
        message = f"{resource} with this {field} already exists."
        super().__init__(message=message, status_code=status.HTTP_400_BAD_REQUEST)


class InvalidDataException(BaseAppException):
    """Exception raised when invalid data is provided."""
    def __init__(self, message: str = "Invalid data provided."):
        super().__init__(message=message, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)


class PermissionDeniedException(BaseAppException):
    """Exception raised when a user does not have permission to perform an action."""
    def __init__(self, message: str = "You do not have permission to perform this action."):
        super().__init__(message=message, status_code=status.HTTP_403_FORBIDDEN)


class UnauthorizedException(BaseAppException):
    """Exception raised when a user is not authorized to perform an action."""
    def __init__(self, message: str = "Authentication required."):
        super().__init__(message=message, status_code=status.HTTP_401_UNAUTHORIZED)


class ConflictException(BaseAppException):
    """Exception raised when a conflict occurs."""
    def __init__(self, message: str = "Conflict occurred."):
        super().__init__(message=message, status_code=status.HTTP_409_CONFLICT)


class DependencyException(BaseAppException):
    """Exception raised when a dependency fails."""
    def __init__(self, message: str = "Operation cannot be completed due to dependencies."):
        super().__init__(message=message, status_code=status.HTTP_409_CONFLICT)


class RateLimitExceededException(BaseAppException):
    """Exception raised when a rate limit is exceeded."""
    def __init__(self, message: str = "Rate limit exceeded. Please try again later."):
        super().__init__(message=message, status_code=status.HTTP_429_TOO_MANY_REQUESTS)


class ServiceUnavailableException(BaseAppException):
    """Exception raised when a service is temporarily unavailable."""
    def __init__(self, message: str = "Service is temporarily unavailable."):
        super().__init__(message=message, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)


class InternalServerErrorException(BaseAppException):
    """Exception raised when an unexpected error occurs."""
    def __init__(self, message: str = "An unexpected error occurred."):
        super().__init__(message=message, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
