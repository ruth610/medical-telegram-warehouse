"""
Pydantic schemas for API request/response validation
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class TopProduct(BaseModel):
    """Schema for top product response"""
    product_term: str
    mention_count: int
    channels: List[str]
    
    class Config:
        json_schema_extra = {
            "example": {
                "product_term": "paracetamol",
                "mention_count": 45,
                "channels": ["chemed", "tikvahpharma"]
            }
        }


class ChannelActivity(BaseModel):
    """Schema for channel activity response"""
    channel_name: str
    total_posts: int
    avg_views: float
    total_views: int
    date: str
    post_count: int
    
    class Config:
        json_schema_extra = {
            "example": {
                "channel_name": "chemed",
                "total_posts": 150,
                "avg_views": 1250.5,
                "total_views": 187575,
                "date": "2026-01-15",
                "post_count": 5
            }
        }


class MessageSearchResult(BaseModel):
    """Schema for message search result"""
    message_id: int
    channel_name: str
    message_date: datetime
    message_text: str
    views: int
    forwards: int
    has_image: bool
    
    class Config:
        json_schema_extra = {
            "example": {
                "message_id": 12345,
                "channel_name": "chemed",
                "message_date": "2026-01-15T10:30:00",
                "message_text": "Paracetamol available now",
                "views": 150,
                "forwards": 5,
                "has_image": True
            }
        }


class VisualContentStats(BaseModel):
    """Schema for visual content statistics"""
    channel_name: str
    total_images: int
    promotional_count: int
    product_display_count: int
    lifestyle_count: int
    other_count: int
    avg_confidence: float
    
    class Config:
        json_schema_extra = {
            "example": {
                "channel_name": "chemed",
                "total_images": 50,
                "promotional_count": 20,
                "product_display_count": 15,
                "lifestyle_count": 10,
                "other_count": 5,
                "avg_confidence": 0.85
            }
        }


class ErrorResponse(BaseModel):
    """Schema for error responses"""
    detail: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "detail": "Error message here"
            }
        }
