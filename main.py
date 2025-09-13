from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import desc, asc, func, and_, or_, distinct
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel, Field
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import time
import re
from datetime import datetime

from sqlalchemy.testing import db

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
    # allow_origins=[
    #     "http://localhost:3000",
    #     "http://127.0.0.1:3000",
    #     "http://localhost:3001",
    #     "http://127.0.0.1:3001",
    # ],
    allow_origins=["*"],
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
    db = None
    scraper = None
    total_start_time = time.time()

    try:
        from database import SessionLocal
        db = SessionLocal()

        if force_refresh:
            existing_count = db.query(StockNotice).filter(StockNotice.symbol == symbol).count()
            if existing_count > 0:
                logger.info(f"REFRESH: Deleting {existing_count} existing records for '{symbol}'")
                deleted_count = db.query(StockNotice).filter(StockNotice.symbol == symbol).delete()
                db.commit()
                logger.info(f"REFRESH: Deleted {deleted_count} records")
        else:
            existing_count = db.query(StockNotice).filter(StockNotice.symbol == symbol).count()
            logger.info(f"APPEND: Found {existing_count} existing records for '{symbol}'")

        # Create scraper and get notices
        scraper = CodalSeleniumScraper()
        all_notices = scraper.scrape_multiple_pages(symbol, max_pages)

        if not all_notices:
            logger.info(f"No notices found for symbol: {symbol}")
            return

        logger.info(f"Processing {len(all_notices)} notices for database...")

        # Get existing publish_times for duplicate checking
        existing_publish_times = set()
        if not force_refresh:
            existing_records = db.query(StockNotice.publish_time).filter(
                StockNotice.symbol == symbol,
                StockNotice.publish_time.isnot(None),
                StockNotice.publish_time != ''
            ).all()
            existing_publish_times = {record[0] for record in existing_records if record[0]}
            logger.info(f"Loaded {len(existing_publish_times)} existing publish_times for duplicate checking")

        # Process notices
        new_notices = []
        duplicates_count = 0

        for notice_data in all_notices:
            try:
                def safe_truncate(text, max_length):
                    if not text:
                        return ""
                    return str(text)[:max_length] if len(str(text)) > max_length else str(text)

                title = notice_data.get('title', '')
                publish_time = notice_data.get('publish_date', '').strip()

                if not title or len(title) < 5:
                    continue

                # Fast duplicate check
                if publish_time and publish_time in existing_publish_times:
                    duplicates_count += 1
                    continue

                # Create notice
                db_notice_data = {
                    'symbol': safe_truncate(notice_data.get('symbol', ''), 100),
                    'company_name': safe_truncate(notice_data.get('company_name', ''), 500),
                    'title': title,
                    'letter_code': '',
                    'send_time': '',
                    'publish_time': safe_truncate(publish_time, 100),
                    'tracking_number': '',
                    'html_link': notice_data.get('detail_link', ''),
                    'has_html': bool(notice_data.get('detail_link')),
                }

                notice = StockNotice(**db_notice_data)
                new_notices.append(notice)

                if publish_time:
                    existing_publish_times.add(publish_time)

            except Exception as e:
                logger.error(f"Error processing notice: {e}")
                continue

        # Batch insert
        if new_notices:
            logger.info(f"Batch inserting {len(new_notices)} new records...")
            db.add_all(new_notices)
            db.commit()

        total_time = time.time() - total_start_time
        final_count = db.query(StockNotice).filter(StockNotice.symbol == symbol).count()

        logger.info(f"ULTRA-FAST scraping completed for '{symbol}' in {total_time:.2f} seconds:")
        logger.info(f"- Total notices scraped: {len(all_notices)}")
        logger.info(f"- New records added: {len(new_notices)}")
        logger.info(f"- Duplicates skipped: {duplicates_count}")
        logger.info(f"- Final total records: {final_count}")

    except Exception as e:
        logger.error(f"Error in ultra-fast scraping: {e}")
        if db:
            db.rollback()
    finally:
        if scraper:
            scraper.close()
        if db:
            db.close()



# ============================================================================
# SEARCH NOTICES PAGE
# ============================================================================
# Financial notice patterns
FINANCIAL_PATTERNS = [
    "اطلاعات و صورت‌های مالی",
    "اطلاعات و صورتهای مالی",
    "صورت های سال مالی",
    "صورتهای سال مالی",
    "اطلاعات مالی",
    "گزارش مالی",
    "صورت‌های مالی سال مالی"
]

PERIOD_PATTERNS = {
    "3ماهه": ["3ماهه", "سه ماهه", "3 ماهه"],
    "6ماهه": ["6ماهه", "شش ماهه", "6 ماهه"],
    "9ماهه": ["9ماهه", "نه ماهه", "9 ماهه"],
    "سال مالی": ["سال مالی", "سالانه", "12ماهه", "12 ماهه"]
}


def extract_period_type(title: str) -> str:
    """Extract period type from notice title"""
    if not title:
        return ""

    title_lower = title.lower()

    # Check for different period types
    if any(pattern in title_lower for pattern in ['سال مالی', 'سالیانه']):
        return "سال مالی"
    elif any(pattern in title_lower for pattern in ['9 ماهه', '۹ ماهه', 'نه ماهه']):
        return "9 ماهه"
    elif any(pattern in title_lower for pattern in ['6 ماهه', '۶ ماهه', 'شش ماهه', 'شیش ماهه']):
        return "6 ماهه"
    elif any(pattern in title_lower for pattern in ['3 ماهه', '۳ ماهه', 'سه ماهه']):
        return "3 ماهه"
    elif any(pattern in title_lower for pattern in ['حسابرسی شده', 'audited']):
        return "حسابرسی شده"
    elif any(pattern in title_lower for pattern in ['حسابرسی نشده', 'unaudited']):
        return "حسابرسی نشده"
    elif any(pattern in title_lower for pattern in ['تجدید ارائه', 'revised', 'restated']):
        return "تجدید ارائه شده"
    else:
        return "نامشخص""نامشخص"


def extract_date_from_title(title: str) -> str:
    """Extract Persian date from notice title"""
    if not title:
        return ""

    import re

    # Pattern for Persian dates like ۱۴۰۳/۰۹/۳۰ or 1403/09/30
    persian_date_patterns = [
        r'۱۴\d{2}[/\-][\d۰-۹]{2}[/\-][\d۰-۹]{2}',  # Persian digits
        r'14\d{2}[/\-]\d{2}[/\-]\d{2}',  # English digits
        r'منتهی به\s+([۰-۹\d/\-]+)',  # "منتهی به" pattern
        r'به\s+([۰-۹\d/\-]+)',  # "به" pattern
    ]

    for pattern in persian_date_patterns:
        match = re.search(pattern, title)
        if match:
            if 'منتهی به' in pattern or 'به' in pattern:
                return match.group(1).strip()
            else:
                return match.group(0).strip()

    return ""


def is_financial_notice(title: str) -> bool:
    """Check if notice title contains financial patterns"""
    if not title:
        return False

    title_lower = title.lower()
    return any(pattern.lower() in title_lower for pattern in FINANCIAL_PATTERNS)


@app.get("/financial-notices/search")
async def search_financial_notices(
        symbol: str = Query(..., description="Company symbol"),
        page: int = Query(1, ge=1),
        per_page: int = Query(50, ge=1, le=100),
        sort_field: Optional[str] = Query("published_time"),
        sort_direction: Optional[str] = Query("desc"),
        db: Session = Depends(get_db)
):
    """Search financial notices by symbol"""

    try:
        # Build base query
        query = db.query(StockNotice).filter(
            StockNotice.symbol.ilike(f"%{symbol}%")
        )

        # Filter for financial notices
        financial_conditions = [
            StockNotice.title.ilike(f"%{pattern}%")
            for pattern in FINANCIAL_PATTERNS
        ]
        query = query.filter(or_(*financial_conditions))

        # Apply sorting - fix the field name
        valid_sort_fields = ['id', 'symbol', 'company_name', 'title', 'publish_time']
        if sort_field and sort_field in valid_sort_fields:
            sort_column = getattr(StockNotice, sort_field)
            if sort_direction == "desc":
                query = query.order_by(desc(sort_column))
            else:
                query = query.order_by(asc(sort_column))
        else:
            # Default sorting
            query = query.order_by(desc(StockNotice.publish_time))

        # Get total count
        total = query.count()

        # Apply pagination
        offset = (page - 1) * per_page
        notices = query.offset(offset).limit(per_page).all()

        # Format results - Fix the datetime handling
        formatted_notices = []
        for notice in notices:
            # Handle published_time properly
            published_time_str = ""
            if notice.publish_time:
                if hasattr(notice.publish_time, 'isoformat'):
                    # It's a datetime object
                    published_time_str = notice.publish_time.isoformat()
                else:
                    # It's already a string
                    published_time_str = str(notice.publish_time)

            formatted_notices.append({
                "notice_id": notice.id,
                "symbol": notice.symbol or "",
                "company_name": notice.company_name or "",
                "title": notice.title or "",
                "notice_type": extract_period_type(notice.title),
                "date_in_title": extract_date_from_title(notice.title),
                "published_time": published_time_str
            })

        return {
            "notices": formatted_notices,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page
        }

    except Exception as e:
        print(f"Search error: {str(e)}")  # Add logging
        import traceback
        print(traceback.format_exc())  # Print full traceback
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)