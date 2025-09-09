from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel, Field
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import time

# Import your modules
from database import get_db, engine
from models import Base, StockNotice, FinancialStatementData
from services import FinancialStatementService
from utils import (
    extract_period_info,
    is_financial_statement,
    search_stored_financial_statements,
    get_financial_summary_stats
)
from scraper_selenium import CodalSeleniumScraper
from notice_content_scraper import NoticeContentScraper
from financial_statement_scraper import FinancialStatementScraper

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create tables
Base.metadata.create_all(bind=engine)

# Thread executor for async operations
content_executor = ThreadPoolExecutor(max_workers=3)

# Initialize FastAPI
app = FastAPI(title="Ultra-Fast Codal Scraper with Financial Statements", version="2.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize financial statement service
financial_service = FinancialStatementService(FinancialStatementScraper, content_executor)


# Pydantic models
class FinancialStatementSearchRequest(BaseModel):
    symbol: Optional[str] = Field(None, description="Company symbol or name")
    limit: Optional[int] = Field(10, description="Max results", ge=1, le=100)


class BatchExtractRequest(BaseModel):
    notice_ids: List[int] = Field(..., description="List of notice IDs to extract")
    output_format: str = Field("json", description="Output format: json, code, or dataframe")


# ============================================================================
# BASIC API ROUTES
# ============================================================================

@app.get("/")
async def root():
    return {
        "message": "Ultra-Fast Codal Scraper with Financial Statements API v2.0",
        "status": "running",
        "features": ["scraping", "financial_statements", "postgresql_storage", "detailed_normalization"]
    }


@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """Health check endpoint"""
    try:
        # Test database connection
        total_notices = db.query(StockNotice).count()
        total_financial = db.query(FinancialStatementData).count()

        return {
            "status": "healthy",
            "database": "connected",
            "total_notices": total_notices,
            "total_financial_data": total_financial
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
        }


# ============================================================================
# FINANCIAL STATEMENT ENDPOINTS - REORDERED TO FIX CONFLICT
# ============================================================================

# PUT THIS ROUTE FIRST - before the {notice_id} route
@app.get("/financial-statement/by-exact-title")
async def get_financial_statement_by_exact_title(
        title: str = Query(..., description="Exact title of the financial statement"),
        symbol: Optional[str] = Query(None, description="Company symbol (optional)"),
        output_format: str = Query("json", description="Output format: json, code, or dataframe"),
        force_refresh: bool = Query(False, description="Force refresh data"),
        db: Session = Depends(get_db)
):
    """Get financial statement by exact title with PostgreSQL storage"""
    return await financial_service.get_by_exact_title(
        title, symbol, output_format, db, force_refresh
    )


# PUT THIS ROUTE SECOND - after the by-exact-title route
@app.get("/financial-statement/{notice_id}")
async def get_financial_statement(
        notice_id: int,
        output_format: str = Query("json", description="Output format: json, code, or dataframe"),
        force_refresh: bool = Query(False, description="Force refresh data"),
        db: Session = Depends(get_db)
):
    """Get financial statement data with PostgreSQL storage and detailed normalization"""
    return await financial_service.get_by_notice_id(
        notice_id, output_format, db, force_refresh
    )


@app.post("/financial-statements/search")
async def search_financial_statements(
        request: FinancialStatementSearchRequest,
        db: Session = Depends(get_db)
):
    """Search for financial statements (both types)"""
    try:
        from sqlalchemy import or_

        query = db.query(StockNotice)

        # Filter for both types of financial statements
        title_condition = or_(
            StockNotice.title.ilike('%اطلاعات و صورت های مالی%'),
            StockNotice.title.ilike('%اطلاعات و صورتهای مالی%'),
            StockNotice.title.ilike('%صورت های سال مالی%'),
            StockNotice.title.ilike('%صورتهای سال مالی%')
        )
        query = query.filter(title_condition)

        # Add symbol filter
        if request.symbol and request.symbol.strip():
            symbol = request.symbol.strip()
            query = query.filter(
                (StockNotice.symbol.ilike(f'%{symbol}%')) |
                (StockNotice.company_name.ilike(f'%{symbol}%'))
            )

        # Order and limit
        query = query.order_by(StockNotice.publish_time.desc())
        limit = request.limit if request.limit and request.limit > 0 else 10
        notices = query.limit(limit).all()

        # Format response with period info
        financial_statements = []
        for notice in notices:
            period_type, audit_status, period_date = extract_period_info(notice.title)

            financial_statements.append({
                "id": notice.id,
                "symbol": notice.symbol or "",
                "company_name": notice.company_name or "",
                "title": notice.title or "",
                "publish_time": notice.publish_time,
                "html_link": notice.html_link or "",
                "period_type": period_type,
                "audit_status": audit_status,
                "period_date": period_date
            })

        return {
            "total": len(financial_statements),
            "financial_statements": financial_statements
        }

    except Exception as e:
        logger.error(f"Search error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/financial-statements/stored")
async def get_stored_financial_statements(
        symbol: Optional[str] = Query(None, description="Company symbol or name"),
        period_type: Optional[str] = Query(None, description="Period type"),
        audit_status: Optional[str] = Query(None, description="Audit status"),
        limit: int = Query(50, description="Maximum number of results"),
        db: Session = Depends(get_db)
):
    """Get stored financial statement data from PostgreSQL"""
    stored_statements = search_stored_financial_statements(
        symbol, period_type, audit_status, limit, db
    )

    return {
        "total": len(stored_statements),
        "stored_statements": stored_statements
    }


@app.get("/financial-statements/stats")
async def get_financial_stats(db: Session = Depends(get_db)):
    """Get financial data statistics"""
    return get_financial_summary_stats(db)


@app.post("/financial-statements/batch-extract")
async def batch_extract_financial_statements(
        request: BatchExtractRequest,
        db: Session = Depends(get_db)
):
    """Extract financial statements from multiple notices with PostgreSQL storage"""
    return await financial_service.batch_extract(
        request.notice_ids, request.output_format, db
    )


# ============================================================================
# SCRAPING ENDPOINTS
# ============================================================================

@app.post("/scrape/{symbol}")
async def scrape_symbol(
        symbol: str,
        background_tasks: BackgroundTasks,
        max_pages: Optional[int] = 1,
        force_refresh: Optional[bool] = False,
        db: Session = Depends(get_db)
):
    """Ultra-fast scraping with targeted data extraction"""
    current_count = db.query(StockNotice).filter(StockNotice.symbol == symbol).count()

    background_tasks.add_task(ultra_fast_scrape, symbol, max_pages, force_refresh)

    return {
        "message": f"Started ULTRA-FAST scraping for symbol: {symbol}",
        "current_records": current_count,
        "max_pages": max_pages,
        "mode": "refresh" if force_refresh else "append",
        "estimated_time": f"{max_pages * 2} seconds"
    }


@app.post("/refresh/{symbol}")
async def refresh_symbol(
        symbol: str,
        background_tasks: BackgroundTasks,
        max_pages: Optional[int] = 1,
        db: Session = Depends(get_db)
):
    """Delete all existing records and scrape fresh data"""
    background_tasks.add_task(ultra_fast_scrape, symbol, max_pages, force_refresh=True)

    return {
        "message": f"Started ULTRA-FAST refresh for symbol: {symbol}",
        "action": "All existing records will be deleted and replaced"
    }


@app.post("/append/{symbol}")
async def append_symbol(
        symbol: str,
        background_tasks: BackgroundTasks,
        max_pages: Optional[int] = 1,
        db: Session = Depends(get_db)
):
    """Keep existing records and add only new ones"""
    background_tasks.add_task(ultra_fast_scrape, symbol, max_pages, force_refresh=False)

    return {
        "message": f"Started ULTRA-FAST append for symbol: {symbol}",
        "action": "New records will be added, duplicates skipped based on publish_time"
    }


# ============================================================================
# NOTICE MANAGEMENT ENDPOINTS
# ============================================================================

@app.get("/count")
def get_count(symbol: Optional[str] = None, db: Session = Depends(get_db)):
    """Get total count of records"""
    query = db.query(StockNotice)
    if symbol:
        query = query.filter(StockNotice.symbol == symbol)
    count = query.count()
    return {"count": count, "symbol": symbol}


@app.get("/symbols")
def get_symbols(db: Session = Depends(get_db)):
    """Get list of all unique symbols"""
    symbols = db.query(StockNotice.symbol).distinct().all()
    return {"symbols": [s[0] for s in symbols if s[0]]}


@app.delete("/symbol/{symbol}")
def delete_symbol(symbol: str, db: Session = Depends(get_db)):
    """Delete all records for a symbol"""
    count = db.query(StockNotice).filter(StockNotice.symbol == symbol).count()

    if count == 0:
        raise HTTPException(status_code=404, detail=f"No records found for symbol: {symbol}")

    deleted = db.query(StockNotice).filter(StockNotice.symbol == symbol).delete()
    db.commit()

    return {
        "message": f"Deleted {deleted} records for symbol: {symbol}",
        "deleted_count": deleted
    }


# ============================================================================
# BACKGROUND SCRAPING FUNCTION (same as before)
# ============================================================================

def ultra_fast_scrape(symbol: str, max_pages: int, force_refresh: bool = False):
    """Ultra-fast background scraping with publish_time duplicate checking"""
    # ... (same implementation as your original code)
    pass


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)