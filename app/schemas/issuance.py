from typing import Any

from pydantic import BaseModel, Field


class IssuanceCustomerInput(BaseModel):
    document_number: str = Field(min_length=1, max_length=120)
    full_name: str = Field(min_length=1, max_length=255)
    age: int = Field(ge=1, le=120)
    sex: str = Field(default="M", max_length=1)
    residence_country_id: int = Field(ge=1)
    repatriation_country_id: int = Field(ge=1)


class IssuanceQuoteRequest(BaseModel):
    plan_version_id: int = Field(ge=1)
    customer: IssuanceCustomerInput
    billing_mode: str | None = Field(default="MONTHLY", max_length=30)


class IssuanceStoreRequest(BaseModel):
    quote_id: str
    start_date: str | None = None


class IssuanceCreateRequest(IssuanceStoreRequest):
    pass


class QuotePayload(BaseModel):
    plan_version_id: int
    product_id: int
    company_id: int
    customer: dict[str, Any]
    pricing: dict[str, Any]
    eligibility: dict[str, Any]
