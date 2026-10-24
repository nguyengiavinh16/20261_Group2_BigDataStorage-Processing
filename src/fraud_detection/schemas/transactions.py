from typing import Literal
from pydantic import BaseModel, Field, ConfigDict

TransactionType = Literal["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]

class Transaction(BaseModel):
    model_config = ConfigDict(extra="forbid")
    step: int = Field(ge=0)
    type: TransactionType
    amount: float = Field(ge=0)
    nameOrig: str = Field(min_length=1)
    oldbalanceOrg: float = Field(ge=0)
    newbalanceOrig: float = Field(ge=0)
    nameDest: str = Field(min_length=1)
    oldbalanceDest: float = Field(ge=0)
    newbalanceDest: float = Field(ge=0)
    isFlaggedFraud: int = Field(default=0, ge=0, le=1)
    isFraud: int | None = Field(default=None, ge=0, le=1)

class PredictionResponse(BaseModel):
    is_fraud: bool
    fraud_probability: float
    threshold: float
    model_version: str = "baseline-v1"
