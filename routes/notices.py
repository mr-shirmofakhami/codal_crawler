from datetime import datetime, timedelta

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Query
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from database import get_db
from models import StockNotice
from services.scraping_service import ultra_fast_scrape
from sqlalchemy import desc, asc, func, and_, or_, distinct

from temp.models import FinancialStatementData
from utils.financial_utils import FINANCIAL_PATTERNS
from utils.text_utils import  extract_period_type, extract_date_from_title

router = APIRouter()

@router.get("/count")
def get_count(symbol: Optional[str] = None, db: Session = Depends(get_db)):
    """Get total count of records"""
    query = db.query(StockNotice)
    if symbol:
        query = query.filter(StockNotice.symbol == symbol)
    count = query.count()
    return {"count": count, "symbol": symbol}

@router.get("/symbols")
def get_symbols(db: Session = Depends(get_db)):
    """Get list of all unique symbols"""
    symbols = db.query(StockNotice.symbol).distinct().all()
    return {"symbols": [s[0] for s in symbols if s[0]]}

@router.get("/notices/symbols")
def get_symbols(db: Session = Depends(get_db)):
    """Get list of all unique symbols"""
    symbols = db.query(StockNotice.symbol).distinct().all()
    return {"symbols": [s[0] for s in symbols if s[0]]}

@router.delete("/symbol/{symbol}")
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


@router.get("/stats")
async def get_system_stats(db: Session = Depends(get_db)):
    """Get system statistics"""
    try:
        # # Define financial patterns
        # FINANCIAL_PATTERNS = [
        #     "اطلاعات و صورت های مالی",
        #     "صورت های سال مالی"
        # ]

        # Total ALL notices (not just financial)
        all_notices = db.query(StockNotice).count()

        # Total financial notices only
        financial_filter = or_(*[StockNotice.title.ilike(f"%{pattern}%") for pattern in FINANCIAL_PATTERNS])
        total_notices = db.query(StockNotice).filter(financial_filter).count()

        # Active companies (companies with financial notices)
        active_companies = db.query(StockNotice.symbol).filter(financial_filter).distinct().count()

        # Stored statements (use financial notices count as estimate)
        stored_statements = total_notices

        # Latest update - just get the string value
        latest_notice = db.query(StockNotice).order_by(desc(StockNotice.id)).first()
        last_update = None
        if latest_notice and latest_notice.publish_time:
            last_update = str(latest_notice.publish_time)

        return {
            "all_notices": all_notices,  # NEW: Total count of all notices
            "total_notices": total_notices,  # Financial notices only
            "active_companies": active_companies,
            "stored_statements": stored_statements,
            "last_update": last_update,
            "status": "active"
        }

    except Exception as e:
        print(f"Stats error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")


