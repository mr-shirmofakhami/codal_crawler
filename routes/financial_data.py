from fastapi import APIRouter, Depends, HTTPException, Query,  BackgroundTasks
from typing import Optional
from database import get_db
from models import StockNotice, FinancialStatementData
from sqlalchemy.orm import Session




router = APIRouter()


from sqlalchemy import func, and_

@router.get("/{symbol}")
def get_financial_data(
    symbol: str,
    period_type: Optional[str] = Query(None, description="Filter by period type"),
    db: Session = Depends(get_db)
):
    """Fetch financial data for a symbol filtered by period_type with conditions"""
    # Base query
    query = db.query(FinancialStatementData).filter(FinancialStatementData.company_symbol == symbol)

    # Apply period_type filter if provided
    if period_type:
        query = query.filter(FinancialStatementData.period_type == period_type)

    # Subquery to get the prioritized record for each period_date
    subquery = (
        db.query(
            FinancialStatementData.period_date,
            func.max(FinancialStatementData.id).label('id')  # Select the smallest id based on conditions
        )
        .filter(FinancialStatementData.company_symbol == symbol)
        .filter(FinancialStatementData.period_type == period_type)
        .group_by(FinancialStatementData.period_date)
        .subquery()
    )

    # Join the original table with the subquery to fetch the prioritized record for each period_date
    final_query = (
        db.query(FinancialStatementData)
        .join(subquery, FinancialStatementData.id == subquery.c.id)
        .order_by(FinancialStatementData.period_date)  # Sort by period_date
    )

    # Execute the query and return the results
    financial_data = final_query.all()

    return {"data": [data.to_dict() for data in financial_data]}