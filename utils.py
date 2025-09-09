import re
from typing import Dict, Optional, Tuple, List
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, distinct
from models import FinancialStatementData, StockNotice
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Updated mapping from Persian item names to shorter column names
ITEM_COLUMN_MAPPING = {
    "درآمدهاي عملياتي": "operating_revenue",
    "بهاى تمام شده درآمدهاي عملياتي": "cost_of_goods_sold",
    "سود(زيان) ناخالص": "gross_profit",
    "هزينه هاى فروش، ادارى و عمومى": "selling_admin_expenses",
    "هزينه کاهش ارزش دريافتني ها (هزينه استثنايي)": "impairment_expense",
    "ساير درآمدها": "other_income",
    "ساير هزينه‌ها": "other_expenses",
    "سود(زيان) عملياتى": "operating_profit",
    "هزينه هاى مالى": "financial_expenses",
    "ساير درآمدها و هزينه هاى غيرعملياتى": "non_operating_income",
    "سود(زيان) عمليات در حال تداوم قبل از ماليات": "profit_before_tax",
    "سال جاري": "current_year_tax",
    "سال‌هاي قبل": "prior_years_tax",
    "سود(زيان) خالص عمليات در حال تداوم": "net_profit_continuing",
    "سود (زيان) خالص عمليات متوقف شده": "net_profit_discontinued",
    "سود(زيان) خالص": "net_profit",
    "عملياتي (ريال)": "operational_eps",
    "غيرعملياتي (ريال)": "non_operational_eps",
    "ناشي از عمليات در حال تداوم": "eps_continuing",
    "ناشي از عمليات متوقف شده": "eps_discontinued",
    "سود(زيان) پايه هر سهم": "basic_eps",
    "سود (زيان) خالص هر سهم – ريال": "diluted_eps",
    "سرمايه": "capital"
}


def extract_period_info(title: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract period type, audit status, and date from title"""

    # Extract period type
    period_type = None
    if "3 ماهه" in title:
        period_type = "3 ماهه"
    elif "6 ماهه" in title:
        period_type = "6 ماهه"
    elif "9 ماهه" in title:
        period_type = "9 ماهه"
    elif "سال مالی" in title:
        period_type = "سال مالی"

    # Extract audit status
    audit_status = None
    if "حسابرسی شده" in title:
        audit_status = "حسابرسی شده"
    elif "حسابرسی نشده" in title:
        audit_status = "حسابرسی نشده"

    # Extract date (Persian date patterns)
    date_patterns = [
        r'(\d{4}/\d{1,2}/\d{1,2})',  # YYYY/MM/DD
        r'(\d{4}-\d{1,2}-\d{1,2})',  # YYYY-MM-DD
        r'(\d{2}/\d{1,2}/\d{4})',  # DD/MM/YYYY
        r'(\d{4}\.\d{1,2}\.\d{1,2})',  # YYYY.MM.DD
    ]

    period_date = None
    for pattern in date_patterns:
        date_match = re.search(pattern, title)
        if date_match:
            period_date = date_match.group(1)
            break

    return period_type, audit_status, period_date


def is_financial_statement(title: str) -> bool:
    """Check if notice is a financial statement"""
    financial_keywords = [
        "اطلاعات و صورت‌های مالی",
        "اطلاعات و صورتهای مالی",
        "صورت های سال مالی",
        "صورتهای سال مالی",
        "اطلاعات مالی",
        "گزارش مالی"
    ]

    return any(keyword in title for keyword in financial_keywords)


def get_stored_financial_data(notice_id: int, db: Session) -> Optional[dict]:
    """Get stored financial data from wide table and reconstruct JSON format"""

    try:
        # Get all period records for this notice
        records = db.query(FinancialStatementData).filter(
            FinancialStatementData.notice_id == notice_id
        ).order_by(FinancialStatementData.period_order).all()

        if not records:
            return None

        # Get basic info from first record
        first_record = records[0]

        # Reconstruct the JSON format
        formatted_data = reconstruct_financial_json_from_wide_table(records)

        return {
            "notice_id": first_record.notice_id,
            "symbol": first_record.company_symbol,
            "company_name": first_record.company_name,
            "title": first_record.raw_title,
            "period_type": first_record.period_type,
            "audit_status": first_record.audit_status,
            "period_date": first_record.period_date,
            "formatted_data": formatted_data,
            "extraction_time": 0.1,  # Already processed
            "extraction_date": first_record.extraction_date.isoformat() if first_record.extraction_date else None,
            "from_database": True,
            "sheet_name": first_record.sheet_name or "صورت سود و زیان"
        }

    except Exception as e:
        logger.error(f"Error getting stored data for notice {notice_id}: {str(e)}")
        return None


def reconstruct_financial_json_from_wide_table(records: List[FinancialStatementData]) -> dict:
    """Reconstruct the original JSON format from wide table records"""

    try:
        if not records:
            return {}

        # Build periods list (exclude audit status periods)
        periods_list = []
        for record in records:
            if record.period_name and record.period_name not in ["حسابرسی شده", "حسابرسی نشده"]:
                periods_list.append(record.period_name)

        # Build items list
        items_list = []

        # Get all item columns from the mapping
        for persian_name, column_name in ITEM_COLUMN_MAPPING.items():
            values_list = []

            # Get values for each period
            for record in records:
                if record.period_name and record.period_name not in ["حسابرسی شده", "حسابرسی نشده"]:
                    # Get amount and formatted value
                    amount = getattr(record, column_name, None)
                    formatted_value = getattr(record, f"{column_name}_fmt", "۰")

                    values_list.append({
                        "amount": float(amount) if amount else None,
                        "formatted": formatted_value or "۰"
                    })

            # Only add item if we have values
            if values_list:
                items_list.append({
                    "name": persian_name,
                    "values": values_list,
                    "is_total": persian_name in ["سود(زيان) ناخالص", "سود(زيان) عملياتى",
                                                 "سود(زيان) عمليات در حال تداوم قبل از ماليات",
                                                 "سود(زيان) خالص عمليات در حال تداوم", "سود(زيان) خالص",
                                                 "ناشي از عمليات در حال تداوم", "سود(زيان) پايه هر سهم",
                                                 "سود (زيان) خالص هر سهم – ريال"],
                    "row_index": list(ITEM_COLUMN_MAPPING.keys()).index(persian_name) + 1
                })

        # Build key metrics
        key_metrics = {}
        for item in items_list:
            item_name = item['name'].lower()
            if "درآمد" in item_name and "عملياتي" in item_name:
                key_metrics["operating_revenue"] = {
                    "name": item['name'],
                    "values": item['values']
                }
            elif "سود" in item_name and "ناخالص" in item_name:
                key_metrics["gross_profit"] = {
                    "name": item['name'],
                    "values": item['values']
                }
            elif "سود" in item_name and "عملياتى" in item_name:
                key_metrics["operating_profit"] = {
                    "name": item['name'],
                    "values": item['values']
                }
            elif "سود" in item_name and "خالص" in item_name and "تداوم" in item_name:
                key_metrics["net_profit"] = {
                    "name": item['name'],
                    "values": item['values']
                }
            elif "سهم" in item_name and "پايه" in item_name:
                key_metrics["eps"] = {
                    "name": item['name'],
                    "values": item['values']
                }
            elif "سرمايه" in item_name:
                key_metrics["capital"] = {
                    "name": item['name'],
                    "values": item['values']
                }

        return {
            "periods": periods_list,
            "items": items_list,
            "key_metrics": key_metrics,
            "summary": {
                "total_items": len(items_list),
                "total_periods": len(periods_list),
                "key_metrics_found": len(key_metrics)
            }
        }

    except Exception as e:
        logger.error(f"Error reconstructing financial JSON: {str(e)}")
        return {}


async def save_financial_data(
        notice: StockNotice,
        financial_data: dict,
        db: Session
) -> bool:
    """Save financial data to wide PostgreSQL table"""

    try:
        period_type, audit_status, period_date = extract_period_info(notice.title)

        # Delete existing records for this notice
        db.query(FinancialStatementData).filter(
            FinancialStatementData.notice_id == notice.id
        ).delete()

        periods = financial_data.get('periods', [])
        items = financial_data.get('items', [])

        # Create item lookup by name
        items_by_name = {}
        for item in items:
            items_by_name[item.get('name', '')] = item

        # Create one record per period (excluding audit status periods)
        records_to_insert = []

        for period_index, period_name in enumerate(periods):
            # Skip audit status periods
            if period_name in ["حسابرسی شده", "حسابرسی نشده"]:
                continue

            # Create base record for this period
            record_data = {
                'notice_id': notice.id,
                'company_symbol': notice.symbol,
                'company_name': notice.company_name,
                'raw_title': notice.title,
                'sheet_name': "صورت سود و زیان",
                'period_type': period_type,
                'audit_status': audit_status,
                'period_date': period_date,
                'period_name': period_name,
                'period_order': period_index
            }

            # Add all item values as columns
            for persian_name, column_name in ITEM_COLUMN_MAPPING.items():
                if persian_name in items_by_name:
                    item = items_by_name[persian_name]
                    values = item.get('values', [])

                    # Get value for this period
                    if period_index < len(values):
                        value_data = values[period_index]
                        record_data[column_name] = value_data.get('amount')
                        record_data[f"{column_name}_fmt"] = value_data.get('formatted', '۰')
                    else:
                        record_data[column_name] = None
                        record_data[f"{column_name}_fmt"] = '۰'
                else:
                    record_data[column_name] = None
                    record_data[f"{column_name}_fmt"] = '۰'

            # Create record
            record = FinancialStatementData(**record_data)
            records_to_insert.append(record)

        # Batch insert all records
        if records_to_insert:
            db.add_all(records_to_insert)
            db.commit()
            logger.info(f"Saved {len(records_to_insert)} financial data records (wide format) for notice {notice.id}")

        return True

    except Exception as e:
        logger.error(f"Error saving financial data for notice {notice.id}: {str(e)}")
        db.rollback()
        return False


def search_stored_financial_statements(
        symbol: Optional[str] = None,
        period_type: Optional[str] = None,
        audit_status: Optional[str] = None,
        limit: int = 50,
        db: Session = None
) -> List[dict]:
    """Search stored financial statements in wide table"""

    try:
        # Get unique financial statements (by notice_id)
        query = db.query(
            FinancialStatementData.notice_id,
            FinancialStatementData.company_symbol,
            FinancialStatementData.company_name,
            FinancialStatementData.period_type,
            FinancialStatementData.audit_status,
            FinancialStatementData.period_date,
            FinancialStatementData.raw_title,
            func.max(FinancialStatementData.extraction_date).label('extraction_date'),
            func.max(FinancialStatementData.updated_at).label('updated_at'),
            func.count(FinancialStatementData.id).label('periods_count')
        ).group_by(
            FinancialStatementData.notice_id,
            FinancialStatementData.company_symbol,
            FinancialStatementData.company_name,
            FinancialStatementData.period_type,
            FinancialStatementData.audit_status,
            FinancialStatementData.period_date,
            FinancialStatementData.raw_title
        )

        # Build filter conditions
        conditions = []

        if symbol:
            symbol_condition = or_(
                FinancialStatementData.company_symbol.ilike(f'%{symbol}%'),
                FinancialStatementData.company_name.ilike(f'%{symbol}%')
            )
            conditions.append(symbol_condition)

        if period_type:
            conditions.append(FinancialStatementData.period_type == period_type)

        if audit_status:
            conditions.append(FinancialStatementData.audit_status == audit_status)

        # Apply conditions
        if conditions:
            query = query.filter(and_(*conditions))

        # Order and limit
        query = query.order_by(func.max(FinancialStatementData.extraction_date).desc())
        results = query.limit(limit).all()

        result = []
        for data in results:
            result.append({
                "notice_id": data.notice_id,
                "company_symbol": data.company_symbol,
                "company_name": data.company_name,
                "period_type": data.period_type,
                "audit_status": data.audit_status,
                "period_date": data.period_date,
                "periods_count": data.periods_count,
                "extraction_date": data.extraction_date.isoformat() if data.extraction_date else None,
                "updated_date": data.updated_at.isoformat() if data.updated_at else None,
                "title": data.raw_title
            })

        return result

    except Exception as e:
        logger.error(f"Error searching stored financial statements: {str(e)}")
        return []


def get_financial_summary_stats(db: Session) -> dict:
    """Get summary statistics from wide table"""

    try:
        # Count unique financial statements
        total_statements = db.query(
            distinct(FinancialStatementData.notice_id)
        ).count()

        # Total period records
        total_records = db.query(FinancialStatementData).count()

        # Count by period type
        period_stats = db.query(
            FinancialStatementData.period_type,
            func.count(distinct(FinancialStatementData.notice_id)).label('count')
        ).group_by(FinancialStatementData.period_type).all()

        # Count by audit status
        audit_stats = db.query(
            FinancialStatementData.audit_status,
            func.count(distinct(FinancialStatementData.notice_id)).label('count')
        ).group_by(FinancialStatementData.audit_status).all()

        # Count unique companies
        unique_companies = db.query(
            distinct(FinancialStatementData.company_symbol)
        ).count()

        # Count unique periods
        unique_periods = db.query(
            distinct(FinancialStatementData.period_name)
        ).count()

        # Recent activity
        recent_records = db.query(
            FinancialStatementData.notice_id,
            FinancialStatementData.company_symbol,
            FinancialStatementData.period_type,
            func.max(FinancialStatementData.extraction_date).label('extraction_date')
        ).group_by(
            FinancialStatementData.notice_id,
            FinancialStatementData.company_symbol,
            FinancialStatementData.period_type
        ).order_by(
            func.max(FinancialStatementData.extraction_date).desc()
        ).limit(30).all()

        recent_activity = [
            {
                "notice_id": r.notice_id,
                "company_symbol": r.company_symbol,
                "period_type": r.period_type,
                "extraction_date": r.extraction_date.isoformat() if r.extraction_date else None
            }
            for r in recent_records
        ]

        return {
            "total_statements": total_statements,
            "total_period_records": total_records,
            "unique_companies": unique_companies,
            "unique_periods": unique_periods,
            "total_items_per_statement": len(ITEM_COLUMN_MAPPING),
            "period_distribution": {stat.period_type: stat.count for stat in period_stats if stat.period_type},
            "audit_distribution": {stat.audit_status: stat.count for stat in audit_stats if stat.audit_status},
            "recent_activity": recent_activity
        }

    except Exception as e:
        logger.error(f"Error getting financial summary stats: {str(e)}")
        return {
            "total_statements": 0,
            "total_period_records": 0,
            "unique_companies": 0,
            "unique_periods": 0,
            "total_items_per_statement": 0,
            "period_distribution": {},
            "audit_distribution": {},
            "recent_activity": []
        }


def check_data_exists(notice_id: int, db: Session) -> bool:
    """Check if financial data exists for a notice"""
    try:
        exists = db.query(FinancialStatementData).filter(
            FinancialStatementData.notice_id == notice_id
        ).first() is not None
        return exists
    except Exception as e:
        logger.error(f"Error checking data existence for notice {notice_id}: {str(e)}")
        return False


def get_companies_with_financial_data(db: Session) -> List[dict]:
    """Get list of companies that have financial data stored"""
    try:
        companies = db.query(
            FinancialStatementData.company_symbol,
            FinancialStatementData.company_name,
            func.count(distinct(FinancialStatementData.notice_id)).label('statements_count'),
            func.count(FinancialStatementData.id).label('period_records_count'),
            func.max(FinancialStatementData.extraction_date).label('latest_extraction')
        ).group_by(
            FinancialStatementData.company_symbol,
            FinancialStatementData.company_name
        ).order_by(
            func.count(distinct(FinancialStatementData.notice_id)).desc()
        ).all()

        return [
            {
                "symbol": c.company_symbol,
                "name": c.company_name,
                "statements_count": c.statements_count,
                "period_records_count": c.period_records_count,
                "latest_extraction": c.latest_extraction.isoformat() if c.latest_extraction else None
            }
            for c in companies
        ]

    except Exception as e:
        logger.error(f"Error getting companies with financial data: {str(e)}")
        return []