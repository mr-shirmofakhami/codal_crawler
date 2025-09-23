from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from database import get_db
from models import StockNotice, FinancialStatementData

router = APIRouter()

@router.get("/health")
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