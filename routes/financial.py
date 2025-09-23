from fastapi import APIRouter, Depends, HTTPException, Query,  BackgroundTasks
from sqlalchemy import desc, asc, or_
from typing import List, Optional, Dict, Any
from sqlalchemy import and_, or_, desc, func
from datetime import datetime
from sqlalchemy.orm import Session
from typing import Optional
from database import get_db
from models import StockNotice, FinancialStatementData
from schemas.financial import FinancialStatementSearchRequest, BatchExtractRequest
from services.financial_service import FinancialStatementService
from utils.financial_utils import extract_period_info, FINANCIAL_PATTERNS
from utils.text_utils import extract_period_type, extract_date_from_title,extract_metric_value, filter_amendments, get_all_direct_metrics
from financial_statement_scraper import FinancialStatementScraper
from concurrent.futures import ThreadPoolExecutor
from utils.financial_utils import (
    get_financial_summary_stats,
    search_stored_financial_statements
)
import logging

logger = logging.getLogger(__name__)




router = APIRouter()

# Initialize services
content_executor = ThreadPoolExecutor(max_workers=3)

# Initialize financial statement service
financial_service = FinancialStatementService(FinancialStatementScraper, content_executor)



@router.get("/by-exact-title")
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

@router.get("/stored")
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

@router.get("/stats")
async def get_financial_stats(db: Session = Depends(get_db)):
    """Get financial data statistics"""
    return get_financial_summary_stats(db)

# @router.get("/search")
# async def search_financial_notices(
#         symbol: str = Query(..., description="Company symbol"),
#         page: int = Query(1, ge=1),
#         per_page: int = Query(50, ge=1, le=100),
#         sort_field: Optional[str] = Query("published_time"),
#         sort_direction: Optional[str] = Query("desc"),
#         db: Session = Depends(get_db)
# ):
#     """Search financial notices by symbol"""
#     try:
#         # Build base query
#         query = db.query(StockNotice).filter(
#             StockNotice.symbol.ilike(f"%{symbol}%")
#         )
#
#         # Filter for financial notices
#         financial_conditions = [
#             StockNotice.title.ilike(f"%{pattern}%")
#             for pattern in FINANCIAL_PATTERNS
#         ]
#         query = query.filter(or_(*financial_conditions))
#
#         # Apply sorting - fix the field name
#         valid_sort_fields = ['id', 'symbol', 'company_name', 'title', 'publish_time']
#         if sort_field and sort_field in valid_sort_fields:
#             sort_column = getattr(StockNotice, sort_field)
#             if sort_direction == "desc":
#                 query = query.order_by(desc(sort_column))
#             else:
#                 query = query.order_by(asc(sort_column))
#         else:
#             # Default sorting
#             query = query.order_by(desc(StockNotice.publish_time))
#
#         # Get total count
#         total = query.count()
#
#         # Apply pagination
#         offset = (page - 1) * per_page
#         notices = query.offset(offset).limit(per_page).all()
#
#         # Format results - Fix the datetime handling
#         formatted_notices = []
#         for notice in notices:
#             # Handle published_time properly
#             published_time_str = ""
#             if notice.publish_time:
#                 if hasattr(notice.publish_time, 'isoformat'):
#                     # It's a datetime object
#                     published_time_str = notice.publish_time.isoformat()
#                 else:
#                     # It's already a string
#                     published_time_str = str(notice.publish_time)
#
#             formatted_notices.append({
#                 "notice_id": notice.id,
#                 "symbol": notice.symbol or "",
#                 "company_name": notice.company_name or "",
#                 "title": notice.title or "",
#                 "notice_type": extract_period_type(notice.title),
#                 "date_in_title": extract_date_from_title(notice.title),
#                 "published_time": published_time_str
#             })
#
#         return {
#             "notices": formatted_notices,
#             "total": total,
#             "page": page,
#             "per_page": per_page,
#             "total_pages": (total + per_page - 1) // per_page
#         }
#
#     except Exception as e:
#         print(f"Search error: {str(e)}")  # Add logging
#         import traceback
#         print(traceback.format_exc())  # Print full traceback
#         raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@router.get("/search")
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

        # Map frontend field names to backend column names
        field_mapping = {
            'notice_id': 'id',
            'symbol': 'symbol',
            'company_name': 'company_name',
            'title': 'title',
            'notice_type': 'title',  # This is derived from title
            'date_in_title': 'title',  # This is derived from title
            'published_time': 'publish_time'  # Map to actual column name
        }

        # Apply sorting
        if sort_field and sort_field in field_mapping:
            # Get the actual database column name
            db_field = field_mapping[sort_field]

            # Special handling for derived fields
            if sort_field in ['notice_type', 'date_in_title']:
                # For derived fields, sort by title
                sort_column = StockNotice.title
            else:
                sort_column = getattr(StockNotice, db_field)

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

        # Format results
        formatted_notices = []
        for notice in notices:
            # Handle published_time properly
            published_time_str = ""
            if notice.publish_time:
                if hasattr(notice.publish_time, 'isoformat'):
                    published_time_str = notice.publish_time.isoformat()
                else:
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
            "total_pages": (total + per_page - 1) // per_page,
            "sort_field": sort_field,
            "sort_direction": sort_direction
        }

    except Exception as e:
        print(f"Search error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
@router.post("/search")
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

@router.post("/batch-extract")
async def batch_extract_financial_statements(
        request: BatchExtractRequest,
        db: Session = Depends(get_db)
):
    """Extract financial statements from multiple notices with PostgreSQL storage"""
    return await financial_service.batch_extract(
        request.notice_ids, request.output_format, db
    )


@router.get("/available-metrics")
async def get_available_metrics():
    """Get list of available metrics for comparison"""

    # Get ALL financial columns dynamically
    direct_metrics = get_all_direct_metrics()

    featured_direct_metrics = [
        "operating_revenue",
        "cost_of_goods_sold",
        "gross_profit",
        "selling_admin_expenses",
        "other_income",
        "non_operating_income",
        "operating_profit",
        "financial_expenses",
        "net_profit",
        "basic_eps",
        "diluted_eps",
        "capital",
        # ...
    ]

    # Calculated metrics with descriptions
    calculated_metrics = [
        {
            "key": "revenue_plus_cogs",
            "name": "درآمد + بهای تمام شده",
            "formula": "operating_revenue + cost_of_goods_sold",
            "description": "مجموع درآمد عملیاتی و بهای تمام شده کالای فروخته شده"
        },
        {
            "key": "total_expenses",
            "name": "کل هزینه‌های عملیاتی",
            "formula": "cost_of_goods_sold + selling_admin_expenses",
            "description": "مجموع بهای تمام شده و هزینه‌های فروش و اداری"
        },
        {
            "key": "total_other_income",
            "name": "کل سایر درآمدها",
            "formula": "other_income + non_operating_income",
            "description": "مجموع سایر درآمدها و درآمدهای غیرعملیاتی"
        },
        {
            "key": "net_operating_result",
            "name": "نتیجه عملیاتی خالص",
            "formula": "operating_profit - financial_expenses",
            "description": "سود عملیاتی منهای هزینه‌های مالی"
        },
        {
            "key": "gross_profit_margin",
            "name": "حاشیه سود ناخالص (درصد)",
            "formula": "(gross_profit / operating_revenue) * 100",
            "description": "درصد حاشیه سود ناخالص نسبت به درآمد"
        },
        {
            "key": "net_profit_margin",
            "name": "حاشیه سود خالص (درصد)",
            "formula": "(net_profit / operating_revenue) * 100",
            "description": "درصد حاشیه سود خالص نسبت به درآمد"
        }
    ]

    return {
        "all_direct_metrics": direct_metrics,  # All available columns
        "featured_direct_metrics": featured_direct_metrics,  # Curated list
        "calculated_metrics": calculated_metrics
    }


@router.post("/bulk-extract-all")
async def bulk_extract_all_financial_statements(
        background_tasks: BackgroundTasks,
        force_refresh: bool = Query(False, description="Force refresh existing data"),
        batch_size: int = Query(50, description="Number of notices to process in each batch"),
        max_concurrent: int = Query(2, description="Maximum concurrent extractions"),
        symbol_filter: Optional[str] = Query(None, description="Filter by specific symbol"),
        db: Session = Depends(get_db)
):
    """Extract financial statements for all eligible notices in background"""

    try:
        # Use the instance, not the class
        background_tasks.add_task(
            financial_service.bulk_extract_all_task,
            force_refresh=force_refresh,
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            symbol_filter=symbol_filter
        )

        return {
            "message": "Bulk financial statement extraction started in background",
            "parameters": {
                "force_refresh": force_refresh,
                "batch_size": batch_size,
                "max_concurrent": max_concurrent,
                "symbol_filter": symbol_filter
            }
        }

    except Exception as e:
        logger.error(f"Error starting bulk extraction: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start bulk extraction: {str(e)}")
@router.get("/bulk-extract-status")
async def get_bulk_extract_status(db: Session = Depends(get_db)):
    """Get status of bulk extraction process and database statistics"""

    from sqlalchemy import func, distinct, or_

    # Financial statement keywords
    financial_conditions = or_(
        StockNotice.title.ilike('%اطلاعات و صورت های مالی%'),
        StockNotice.title.ilike('%اطلاعات و صورتهای مالی%'),
        StockNotice.title.ilike('%صورت های سال مالی%'),
        StockNotice.title.ilike('%صورتهای سال مالی%'),
        StockNotice.title.ilike('%صورت سود و زیان%'),
        StockNotice.title.ilike('%ترازنامه%'),
        StockNotice.title.ilike('%صورت جریان وجوه نقد%')
    )

    # Count total eligible notices
    total_eligible = db.query(StockNotice).filter(
        financial_conditions,
        StockNotice.html_link.isnot(None)
    ).count()

    # Count how many already have financial data
    processed_count = db.query(FinancialStatementData).count()

    # Count unique notices with financial data
    unique_processed = db.query(
        func.count(distinct(FinancialStatementData.notice_id))
    ).scalar()

    # Get symbol breakdown
    symbol_stats = db.query(
        StockNotice.symbol,
        func.count(StockNotice.id).label('total_notices'),
        func.count(FinancialStatementData.notice_id).label('processed_notices')
    ).outerjoin(
        FinancialStatementData, StockNotice.id == FinancialStatementData.notice_id
    ).filter(
        financial_conditions,
        StockNotice.html_link.isnot(None)
    ).group_by(StockNotice.symbol).all()

    return {
        "database_status": {
            "total_eligible_notices": total_eligible,
            "total_processed_records": processed_count,
            "unique_notices_processed": unique_processed,
            "remaining_notices": max(0, total_eligible - unique_processed),
            "completion_percentage": round((unique_processed / total_eligible * 100), 2) if total_eligible > 0 else 0
        },
        "symbol_breakdown": [
            {
                "symbol": stat.symbol,
                "total_notices": stat.total_notices,
                "processed_notices": stat.processed_notices or 0,
                "completion_rate": round(((stat.processed_notices or 0) / stat.total_notices * 100), 1)
            }
            for stat in symbol_stats[:20]  # Top 20 symbols
        ],
        "last_updated": func.now()
    }


@router.get("/compare")
async def compare_financial_statements(
        symbols: str = Query(..., description="Comma-separated symbols (e.g., 'شپاکسا,شپنا')"),
        period_type: str = Query(..., description="Period type (e.g., 'سال مالی', 'سه ماهه')"),
        metrics: str = Query(..., description="Comma-separated metrics/columns"),
        start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
        end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
        limit: int = Query(10, description="Number of periods to return"),
        db: Session = Depends(get_db)
):
    """Compare financial statements across multiple symbols with flexible metrics"""
    try:
        symbol_list = [s.strip() for s in symbols.split(',')]
        metric_list = [m.strip() for m in metrics.split(',')]

        comparison_data = {}

        for symbol in symbol_list:
            # Build query filters
            query_filters = [
                FinancialStatementData.company_symbol == symbol,
                FinancialStatementData.period_type.ilike(f"%{period_type}%"),
                FinancialStatementData.period_order == 0,
                FinancialStatementData.audit_status.ilike("%حسابرسی شده%")
            ]

            # Add date filters if provided
            if start_date:
                try:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
                    query_filters.append(FinancialStatementData.period_date >= start_dt)
                except ValueError:
                    pass

            if end_date:
                try:
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
                    query_filters.append(FinancialStatementData.period_date <= end_dt)
                except ValueError:
                    pass

            # Get financial data
            financial_records = db.query(FinancialStatementData).filter(
                and_(*query_filters)
            ).order_by(
                desc(FinancialStatementData.period_date)
            ).limit(limit * 2).all()  # Get more records to account for filtering

            if financial_records:
                # 🔧 NEW: Apply amendment filter
                filtered_records = filter_amendments(financial_records)

                # Apply final limit after filtering
                filtered_records = filtered_records[:limit]

                # Process metrics for this symbol
                symbol_data = {
                    'company_name': filtered_records[0].company_name if filtered_records else None,
                    'symbol': symbol,
                    'periods': [],
                    'metrics': {}
                }

                # Initialize metrics structure
                for metric in metric_list:
                    symbol_data['metrics'][metric] = {
                        'values': [],
                        'formatted_values': [],
                        'periods': []
                    }

                # Process each financial record
                for record in filtered_records:
                    # Handle period_date safely
                    period_date_str = None
                    if record.period_date:
                        if isinstance(record.period_date, str):
                            period_date_str = record.period_date
                        elif hasattr(record.period_date, 'isoformat'):
                            period_date_str = record.period_date.isoformat()
                        else:
                            period_date_str = str(record.period_date)

                    period_info = {
                        'period_date': period_date_str,
                        'period_name': record.period_name,
                        'period_type': record.period_type,
                        'audit_status': record.audit_status,
                        'extracted_date': extract_date_from_title(record.period_name or "")
                    }
                    symbol_data['periods'].append(period_info)

                    # Extract metric values
                    for metric in metric_list:
                        value, formatted_value = extract_metric_value(record, metric)

                        symbol_data['metrics'][metric]['values'].append(value)
                        symbol_data['metrics'][metric]['formatted_values'].append(formatted_value)
                        symbol_data['metrics'][metric]['periods'].append(period_date_str)

                comparison_data[symbol] = symbol_data
            else:
                comparison_data[symbol] = {
                    'company_name': None,
                    'symbol': symbol,
                    'periods': [],
                    'metrics': {},
                    'error': f'No financial data found for {symbol} with period type {period_type}'
                }

        return {
            "comparison": comparison_data,
            "request_params": {
                "symbols": symbol_list,
                "period_type": period_type,
                "metrics": metric_list,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit
            }
        }

    except Exception as e:
        logger.error(f"Error in financial comparison: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Comparison failed: {str(e)}")


@router.get("/{notice_id}")
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