from typing import Any

from pydantic import BaseModel


class ApiError(BaseModel):
    code: str
    message: str
    details: dict[str, Any] | None = None
    request_id: str | None = None


class ApiResponse(BaseModel):
    ok: bool
    message: str
    data: dict[str, Any] | list[dict[str, Any]] | None = None
    request_id: str | None = None
