from pydantic import BaseModel, EmailStr


class AuthLoginRequest(BaseModel):
    email: EmailStr
    password: str


class AuthRefreshRequest(BaseModel):
    refresh_token: str | None = None


class AuthLogoutRequest(BaseModel):
    refresh_token: str | None = None
