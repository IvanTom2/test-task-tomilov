from typing import Optional


class GitHubAPIError(Exception):
    def __init__(
        self,
        status_code: int,
        message: str,
        url: Optional[str] = None,
    ):
        self.status_code = status_code
        self.message = message
        self.url = url
        super().__init__(f"GitHub API Error {status_code}: {message} (URL: {url})")


class GitHubAPIBadRequestError(GitHubAPIError):
    def __init__(self, message: str, url: Optional[str] = None):
        super().__init__(400, message, url)


class GitHubAPIRateLimitError(GitHubAPIError):
    def __init__(
        self,
        message: str,
        url: Optional[str] = None,
        reset_time: Optional[int] = None,
    ):
        super().__init__(403, message, url)
        self.reset_time = reset_time


class GitHubAPINotFoundError(GitHubAPIError):
    def __init__(self, message: str, url: Optional[str] = None):
        super().__init__(404, message, url)


class GitHubAPIConflictError(GitHubAPIError):
    def __init__(self, message: str, url: Optional[str] = None):
        super().__init__(409, message, url)


class GitHubAPIValidationError(GitHubAPIError):
    def __init__(self, message: str, url: Optional[str] = None):
        super().__init__(422, message, url)


class GitHubAPIServerError(GitHubAPIError):
    def __init__(self, status_code: int, message: str, url: Optional[str] = None):
        super().__init__(status_code, message, url)


class GitHubAPIUnauthorizedError(GitHubAPIError):
    def __init__(self, message: str, url: Optional[str] = None):
        super().__init__(401, message, url)


class GitHubAPIForbiddenError(GitHubAPIError):
    def __init__(self, message: str, url: Optional[str] = None):
        super().__init__(403, message, url)


class RetryFailedError(Exception):
    pass
