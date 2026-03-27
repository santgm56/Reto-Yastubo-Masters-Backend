from pydantic import BaseModel, Field


class CancellationCreateRequest(BaseModel):
    contract_id: int = Field(ge=1)
    reason: str = Field(min_length=6, max_length=300)