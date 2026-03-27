from pydantic import BaseModel, Field


class PaymentCheckoutRequest(BaseModel):
    monthly_record_id: int = Field(ge=1)
    mode: str | None = None


class PaymentSubscribeRequest(BaseModel):
    monthly_record_id: int = Field(ge=1)
    success_url: str | None = None
    cancel_url: str | None = None


class PaymentWebhookRequest(BaseModel):
    monthly_record_id: int = Field(ge=1)
    outcome: str = Field(pattern="^(success|failed)$")
    event_id: str | None = Field(default="", max_length=200)
