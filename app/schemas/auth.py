from pydantic import BaseModel, EmailStr


class AuthLoginRequest(BaseModel):
    email: EmailStr
    password: str


class AuthRefreshRequest(BaseModel):
    refresh_token: str | None = None


class AuthLogoutRequest(BaseModel):
    refresh_token: str | None = None


class AuthPasswordCheckRequest(BaseModel):
    password: str
    first_name: str | None = None
    last_name: str | None = None
    display_name: str | None = None
    email: EmailStr | None = None
