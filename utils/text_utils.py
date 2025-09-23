from models import FinancialStatementData
from datetime import datetime
from typing import List, Optional, Dict, Any

import logging

logger = logging.getLogger(__name__)


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


def is_financial_statement(title: str) -> bool:
    """Check if notice is a financial statement"""
    financial_keywords = [
        "اطلاعات و صورت‌های مالی",
        "اطلاعات و صورتهای مالی",
        "صورت های سال مالی",
        "صورتهای سال مالی",
        "اطلاعات مالی",
        "گزارش مالی",
        "صورت‌های مالی سال مالی",
        "صورت‌های مالی تلفیقی سال مالی"
    ]

    return any(keyword in title for keyword in financial_keywords)


def filter_amendments(records: List[FinancialStatementData]) -> List[FinancialStatementData]:
    """
    Filter out original records if amendments exist for the same period_type and extracted date

    Logic:
    - If a record contains 'اصلاحیه' in raw_title, it's an amendment
    - Remove original records that have amendments for the same period_type + extracted_date
    - Keep only the amendment (most recent correction)
    """
    if not records:
        return records

    # Group records by period_type and extracted date
    period_groups = {}
    amendments = {}

    for record in records:
        # Extract date from raw_title using your existing function
        extracted_date = extract_date_from_title(record.raw_title or "")

        # Create a key for grouping: period_type + extracted_date
        period_key = f"{record.period_type}_{extracted_date}"

        if period_key not in period_groups:
            period_groups[period_key] = []
        period_groups[period_key].append(record)

        # Check if this record is an amendment (check raw_title)
        if record.raw_title and 'اصلاحیه' in record.raw_title:
            amendments[period_key] = record
            logger.info(f"Found amendment for {period_key}: {record.raw_title}")

    # Filter logic
    filtered_records = []

    for period_key, group_records in period_groups.items():
        if period_key in amendments:
            # If there's an amendment for this period, only keep the amendment
            filtered_records.append(amendments[period_key])
            logger.info(f"Keeping amendment for {period_key}, removing {len(group_records) - 1} original(s)")
        else:
            # No amendment exists, keep all original records for this period
            filtered_records.extend(group_records)

    # Sort by period_date descending to maintain original order
    filtered_records.sort(key=lambda x: x.period_date if x.period_date else datetime.min.date(), reverse=True)

    return filtered_records


def extract_metric_value(record: FinancialStatementData, metric: str):
    """Extract metric value from financial record, supporting both direct and calculated metrics"""

    # Check if it's a calculated metric FIRST
    calculated_metrics = get_calculated_metrics()
    if metric in calculated_metrics:
        return calculate_metric_value(record, metric)

    # Handle direct metrics (your existing logic)
    if hasattr(record, metric):
        raw_value = getattr(record, metric)

        if raw_value is None:
            return 0, "N/A"

        try:
            # Convert to float if it's a string number
            if isinstance(raw_value, str):
                # Remove commas and convert
                cleaned_value = raw_value.replace(',', '').replace('٬', '')
                numeric_value = float(cleaned_value)
            else:
                numeric_value = float(raw_value)

            # Format with commas for display
            formatted_value = f"{numeric_value:,.0f}"
            return numeric_value, formatted_value

        except (ValueError, TypeError):
            return 0, "N/A"

    # If metric doesn't exist
    return 0, "N/A"



def get_calculated_metrics():
    """Get dictionary of calculated metrics definitions"""
    return {
        "revenue_plus_cogs": {
            "formula": lambda r: get_field_value(r, "operating_revenue") + get_field_value(r, "cost_of_goods_sold"),
            "name": "درآمد عملیاتی + بهای تمام شده"
        },
        "total_expenses": {
            "formula": lambda r: get_field_value(r, "cost_of_goods_sold") + get_field_value(r,
                                                                                            "selling_admin_expenses"),
            "name": "کل هزینه‌ها"
        },
        "total_other_income": {
            "formula": lambda r: get_field_value(r, "other_income") + get_field_value(r, "non_operating_income"),
            "name": "کل سایر درآمدها"
        },
        "net_operating_result": {
            "formula": lambda r: get_field_value(r, "operating_profit") - get_field_value(r, "financial_expenses"),
            "name": "نتیجه عملیاتی خالص"
        },
        "revenue_to_capital_ratio": {
            "formula": lambda r: get_field_value(r, "operating_revenue") / get_field_value(r,
                                                                                           "capital") if get_field_value(
                r, "capital") != 0 else 0,
            "name": "نسبت درآمد به سرمایه"
        }
    }


def calculate_metric_value(record: FinancialStatementData, metric_key: str):
    """Calculate value for calculated metrics"""
    calculated_metrics = get_calculated_metrics()

    if metric_key not in calculated_metrics:
        return 0, "0"

    try:
        # Execute the formula
        numeric_value = calculated_metrics[metric_key]["formula"](record)

        # Format with commas for display
        formatted_value = f"{numeric_value:,.2f}" if metric_key == "revenue_to_capital_ratio" else f"{numeric_value:,.0f}"

        return numeric_value, formatted_value

    except (ValueError, TypeError, ZeroDivisionError) as e:
        logger.warning(f"Error calculating {metric_key}: {str(e)}")
        return 0, "0"


def get_field_value(record: FinancialStatementData, field_name: str) -> float:
    """Safely get numeric value from a field"""
    if not hasattr(record, field_name):
        return 0.0

    raw_value = getattr(record, field_name)

    if raw_value is None:
        return 0.0

    try:
        if isinstance(raw_value, str):
            cleaned_value = raw_value.replace(',', '')
            return float(cleaned_value)
        else:
            return float(raw_value)
    except (ValueError, TypeError):
        return 0.0


# Get all available columns from your model
def get_all_direct_metrics():
    """Get all available columns from FinancialStatementData model"""
    from sqlalchemy import inspect

    inspector = inspect(FinancialStatementData)
    columns = [column.name for column in inspector.columns]

    # Filter out system columns
    excluded_columns = ['id', 'notice_id', 'company_symbol', 'company_name',
                        'period_date', 'period_name', 'period_type', 'audit_status',
                        'period_order', 'created_at', 'updated_at']

    financial_columns = [col for col in columns if col not in excluded_columns]
    return financial_columns
