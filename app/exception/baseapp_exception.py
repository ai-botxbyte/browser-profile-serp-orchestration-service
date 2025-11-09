from typing import Optional

# Define HTTP status codes
HTTP_400_BAD_REQUEST = 400
HTTP_401_UNAUTHORIZED = 401
HTTP_403_FORBIDDEN = 403
HTTP_404_NOT_FOUND = 404
HTTP_409_CONFLICT = 409
HTTP_422_UNPROCESSABLE_ENTITY = 422
HTTP_429_TOO_MANY_REQUESTS = 429
HTTP_500_INTERNAL_SERVER_ERROR = 500
HTTP_503_SERVICE_UNAVAILABLE = 503


class BaseAppException(Exception):
    """Domain-level base exception for application/consumer code (not tied to HTTP)."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code

    

class NotFoundException(BaseAppException):
    """Exception raised when a resource is not found."""
    def __init__(self, resource: str, resource_id: Optional[str] = None):
        message = f"{resource} not found."
        if resource_id:
            message = f"{resource} with ID {resource_id} not found."
        super().__init__(message=message, status_code=HTTP_404_NOT_FOUND)


class AlreadyExistsException(BaseAppException):
    """Exception raised when a resource already exists."""
    def __init__(self, resource: str, field: str = "name"):
        message = f"{resource} with this {field} already exists."
        super().__init__(message=message, status_code=HTTP_400_BAD_REQUEST)


class InvalidDataException(BaseAppException):
    """Exception raised when invalid data is provided."""
    def __init__(self, message: str = "Invalid data provided."):
        super().__init__(message=message, status_code=HTTP_422_UNPROCESSABLE_ENTITY)


class PermissionDeniedException(BaseAppException):
    """Exception raised when a user does not have permission to perform an action."""
    def __init__(self, message: str = "You do not have permission to perform this action."):
        super().__init__(message=message, status_code=HTTP_403_FORBIDDEN)


class UnauthorizedException(BaseAppException):
    """Exception raised when a user is not authorized to perform an action."""
    def __init__(self, message: str = "Authentication required."):
        super().__init__(message=message, status_code=HTTP_401_UNAUTHORIZED)


class ConflictException(BaseAppException):
    """Exception raised when a conflict occurs."""
    def __init__(self, message: str = "Conflict occurred."):
        super().__init__(message=message, status_code=HTTP_409_CONFLICT)


class DependencyException(BaseAppException):
    """Exception raised when a dependency fails."""
    def __init__(self, message: str = "Operation cannot be completed due to dependencies."):
        super().__init__(message=message, status_code=HTTP_409_CONFLICT)


class RateLimitExceededException(BaseAppException):
    """Exception raised when a rate limit is exceeded."""
    def __init__(self, message: str = "Rate limit exceeded. Please try again later."):
        super().__init__(message=message, status_code=HTTP_429_TOO_MANY_REQUESTS)


class ServiceUnavailableException(BaseAppException):
    """Exception raised when a service is temporarily unavailable."""
    def __init__(self, message: str = "Service is temporarily unavailable."):
        super().__init__(message=message, status_code=HTTP_503_SERVICE_UNAVAILABLE)


class InternalServerErrorException(BaseAppException):
    """Exception raised when an unexpected error occurs."""
    def __init__(self, message: str = "An unexpected error occurred."):
        super().__init__(message=message, status_code=HTTP_500_INTERNAL_SERVER_ERROR)