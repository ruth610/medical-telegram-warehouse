"""
FastAPI Analytical API

This API exposes analytical endpoints to query the data warehouse.
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
import logging

from api.database import get_db
from api.schemas import (
    TopProduct,
    ChannelActivity,
    MessageSearchResult,
    VisualContentStats,
    ErrorResponse
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Telegram Health Data Platform API",
    description="Analytical API for querying medical Telegram channel data",
    version="1.0.0"
)


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint"""
    return {
        "message": "Telegram Health Data Platform API",
        "version": "1.0.0",
        "docs": "/docs"
    }


@app.get(
    "/api/reports/top-products",
    response_model=List[TopProduct],
    tags=["Reports"],
    summary="Get top mentioned products",
    description="Returns the most frequently mentioned terms/products across all channels"
)
async def get_top_products(
    limit: int = Query(10, ge=1, le=100, description="Number of top products to return"),
    db: Session = Depends(get_db)
):
    """
    Get top mentioned products across all channels.
    
    Uses text analysis to extract product terms from message text.
    """
    try:
        query = text("""
            WITH word_counts AS (
                SELECT 
                    unnest(string_to_array(lower(message_text), ' ')) as word,
                    channel_key
                FROM marts.fct_messages
                WHERE message_text IS NOT NULL 
                    AND length(trim(message_text)) > 0
            ),
            filtered_words AS (
                SELECT word, channel_key
                FROM word_counts
                WHERE length(word) > 3
                    AND word !~ '^[0-9]+$'
                    AND word NOT IN ('the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'her', 'was', 'one', 'our', 'out', 'day', 'get', 'has', 'him', 'his', 'how', 'its', 'may', 'new', 'now', 'old', 'see', 'two', 'who', 'way', 'use', 'man', 'men', 'say', 'she', 'her', 'put', 'end', 'did', 'set', 'let', 'own', 'run', 'why', 'try', 'few', 'got', 'ask', 'far', 'got', 'got', 'got')
            ),
            product_mentions AS (
                SELECT 
                    word as product_term,
                    COUNT(*) as mention_count,
                    array_agg(DISTINCT dc.channel_name) as channels
                FROM filtered_words fw
                JOIN marts.dim_channels dc ON fw.channel_key = dc.channel_key
                GROUP BY word
                HAVING COUNT(*) >= 2
            )
            SELECT 
                product_term,
                mention_count,
                channels
            FROM product_mentions
            ORDER BY mention_count DESC
            LIMIT :limit
        """)
        
        result = db.execute(query, {"limit": limit})
        rows = result.fetchall()
        
        products = [
            TopProduct(
                product_term=row[0],
                mention_count=row[1],
                channels=row[2] if isinstance(row[2], list) else list(row[2])
            )
            for row in rows
        ]
        
        return products
        
    except Exception as e:
        logger.error(f"Error fetching top products: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/api/channels/{channel_name}/activity",
    response_model=List[ChannelActivity],
    tags=["Channels"],
    summary="Get channel activity",
    description="Returns posting activity and trends for a specific channel"
)
async def get_channel_activity(
    channel_name: str,
    days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
    db: Session = Depends(get_db)
):
    """
    Get posting activity and trends for a specific channel.
    """
    try:
        query = text("""
            SELECT 
                dc.channel_name,
                dc.total_posts,
                dc.avg_views,
                SUM(fm.view_count) as total_views,
                dd.full_date::text as date,
                COUNT(fm.message_id) as post_count
            FROM marts.fct_messages fm
            JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            WHERE dc.channel_name = :channel_name
                AND dd.full_date >= CURRENT_DATE - INTERVAL '1 day' * :days
            GROUP BY dc.channel_name, dc.total_posts, dc.avg_views, dd.full_date
            ORDER BY dd.full_date DESC
        """)
        
        result = db.execute(query, {"channel_name": channel_name, "days": days})
        rows = result.fetchall()
        
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"Channel '{channel_name}' not found or has no activity"
            )
        
        activities = []
        for row in rows:
            activities.append(ChannelActivity(
                channel_name=row[0],
                total_posts=row[1],
                avg_views=float(row[2]) if row[2] else 0.0,
                total_views=row[3] or 0,
                date=str(row[4]),
                post_count=row[5]
            ))
        
        return activities
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching channel activity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/api/search/messages",
    response_model=List[MessageSearchResult],
    tags=["Search"],
    summary="Search messages",
    description="Searches for messages containing a specific keyword"
)
async def search_messages(
    query: str = Query(..., min_length=1, description="Search keyword"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
    channel_name: Optional[str] = Query(None, description="Filter by channel name"),
    db: Session = Depends(get_db)
):
    """
    Search for messages containing a specific keyword.
    """
    try:
        if channel_name:
            sql_query = text("""
                SELECT 
                    fm.message_id,
                    dc.channel_name,
                    fm.message_text,
                    dd.full_date,
                    fm.view_count,
                    fm.forward_count,
                    fm.has_image
                FROM marts.fct_messages fm
                JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
                JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
                WHERE LOWER(fm.message_text) LIKE LOWER(:query_pattern)
                    AND dc.channel_name = :channel_name
                ORDER BY dd.full_date DESC, fm.view_count DESC
                LIMIT :limit
            """)
            result = db.execute(
                sql_query,
                {
                    "query_pattern": f"%{query}%",
                    "channel_name": channel_name,
                    "limit": limit
                }
            )
        else:
            sql_query = text("""
                SELECT 
                    fm.message_id,
                    dc.channel_name,
                    fm.message_text,
                    dd.full_date,
                    fm.view_count,
                    fm.forward_count,
                    fm.has_image
                FROM marts.fct_messages fm
                JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
                JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
                WHERE LOWER(fm.message_text) LIKE LOWER(:query_pattern)
                ORDER BY dd.full_date DESC, fm.view_count DESC
                LIMIT :limit
            """)
            result = db.execute(
                sql_query,
                {"query_pattern": f"%{query}%", "limit": limit}
            )
        
        rows = result.fetchall()
        
        messages = [
            MessageSearchResult(
                message_id=row[0],
                channel_name=row[1],
                message_text=row[2] or "",
                message_date=row[3],
                views=row[4] or 0,
                forwards=row[5] or 0,
                has_image=row[6] or False
            )
            for row in rows
        ]
        
        return messages
        
    except Exception as e:
        logger.error(f"Error searching messages: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/api/reports/visual-content",
    response_model=List[VisualContentStats],
    tags=["Reports"],
    summary="Get visual content statistics",
    description="Returns statistics about image usage across channels"
)
async def get_visual_content_stats(
    db: Session = Depends(get_db)
):
    """
    Get statistics about visual content (images) across channels.
    """
    try:
        query = text("""
            SELECT 
                dc.channel_name,
                COUNT(DISTINCT fid.detection_id) as total_images,
                COUNT(DISTINCT CASE WHEN fid.image_category = 'promotional' THEN fid.detection_id END) as promotional_count,
                COUNT(DISTINCT CASE WHEN fid.image_category = 'product_display' THEN fid.detection_id END) as product_display_count,
                COUNT(DISTINCT CASE WHEN fid.image_category = 'lifestyle' THEN fid.detection_id END) as lifestyle_count,
                COUNT(DISTINCT CASE WHEN fid.image_category = 'other' THEN fid.detection_id END) as other_count,
                AVG(fid.confidence_score) as avg_confidence
            FROM marts.fct_image_detections fid
            JOIN marts.dim_channels dc ON fid.channel_key = dc.channel_key
            GROUP BY dc.channel_name
            ORDER BY total_images DESC
        """)
        
        result = db.execute(query)
        rows = result.fetchall()
        
        stats = []
        for row in rows:
            stats.append(VisualContentStats(
                channel_name=row[0],
                total_images=row[1] or 0,
                promotional_count=row[2] or 0,
                product_display_count=row[3] or 0,
                lifestyle_count=row[4] or 0,
                other_count=row[5] or 0,
                avg_confidence=float(row[6]) if row[6] else 0.0
            ))
        
        return stats
        
    except Exception as e:
        logger.error(f"Error fetching visual content stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health", tags=["Health"])
async def health_check(db: Session = Depends(get_db)):
    """Health check endpoint"""
    try:
        db.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "database": "disconnected", "error": str(e)}
        )
