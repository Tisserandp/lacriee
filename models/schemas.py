"""
Pydantic models and schemas for LaCriee API.

Defines request/response models for FastAPI endpoints.
"""
from pydantic import BaseModel
from typing import Optional


class ProductItem(BaseModel):
    """
    Schema for a product item in the ProvidersPrices table.

    Represents a price record from a provider with structured fields.
    """
    keyDate: str
    Vendor: str
    ProductName: str
    Code_Provider: str
    Date: str
    Prix: Optional[float] = None
    Categorie: Optional[str] = None

    class Config:
        """Pydantic model configuration."""
        json_schema_extra = {
            "example": {
                "keyDate": "Audierne_2024-01-15_001",
                "Vendor": "Audierne",
                "ProductName": "Bar 3/4 LIGNE",
                "Code_Provider": "001",
                "Date": "2024-01-15",
                "Prix": 15.50,
                "Categorie": "BAR",
            }
        }
