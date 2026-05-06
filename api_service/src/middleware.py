"""
FastAPI Middleware
──────────────────
Request logging, correlation IDs, and error handling middleware.
"""

import logging
import time
import uuid
import contextvars
from fastapi import Request, FastAPI
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

# Context variable to store current request ID for log propagation
request_id_ctx: contextvars.ContextVar[str | None] = contextvars.ContextVar("request_id", default=None)


class RequestIdFilter(logging.Filter):
    """Logging filter that injects the current request ID into log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        request_id = request_id_ctx.get()
        if request_id:
            record.request_id = request_id
        else:
            record.request_id = "-"
        return True


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware that:
    - Generates a unique request ID for each request
    - Sets request ID in context for log propagation
    - Logs request start/end with timing and status code
    - Catches unhandled exceptions and returns structured JSON error response
    """

    def __init__(self, app: FastAPI):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        request_id = str(uuid.uuid4())[:8]
        request.state.request_id = request_id
        # Set context var for this request (task-local)
        token = request_id_ctx.set(request_id)

        logger.info(
            "→ %s %s (req_id=%s, client=%s)",
            request.method,
            request.url.path,
            request_id,
            request.client.host if request.client else "unknown",
        )

        start_time = time.time()

        try:
            response = await call_next(request)
            duration_ms = (time.time() - start_time) * 1000

            logger.info(
                "← %s %s %d (req_id=%s, %.1fms)",
                request.method,
                request.url.path,
                response.status_code,
                request_id,
                duration_ms,
            )
            response.headers["X-Request-ID"] = request_id
            return response

        except Exception as exc:
            duration_ms = (time.time() - start_time) * 1000
            logger.exception(
                "✗ %s %s (req_id=%s, %.1fms, error=%s)",
                request.method,
                request.url.path,
                request_id,
                duration_ms,
                exc.__class__.__name__,
            )
            return JSONResponse(
                status_code=500,
                content={
                    "error": "internal_server_error",
                    "message": "An unexpected error occurred",
                    "request_id": request_id,
                },
            )
        finally:
            # Reset context var
            request_id_ctx.reset(token)

