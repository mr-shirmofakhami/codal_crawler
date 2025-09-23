from pydantic import BaseModel, Field
from typing import List, Optional

class FinancialStatementSearchRequest(BaseModel):
    symbol: Optional[str] = Field(None, description="Company symbol or name")
    limit: Optional[int] = Field(10, description="Max results", ge=1, le=100)


class BatchExtractRequest(BaseModel):
    notice_ids: List[int] = Field(..., description="List of notice IDs to extract")
    output_format: str = Field("json", description="Output format: json, code, or dataframe")