from models import FinancialStatementData
import logging

logger = logging.getLogger(__name__)


def extract_metric_value(record: FinancialStatementData, metric: str) -> tuple:
    """
    Extract metric value from financial record
    Supports both direct columns and calculated metrics

    Returns: (numeric_value, formatted_value)
    """
    try:
        # Direct column access
        if hasattr(record, metric):
            numeric_value = getattr(record, metric)
            formatted_value = getattr(record, f"{metric}_fmt", None)

            return numeric_value, formatted_value

        # Calculated metrics
        calculated_value, formatted_value = calculate_metric(record, metric)
        return calculated_value, formatted_value

    except Exception as e:
        logger.warning(f"Could not extract metric {metric}: {str(e)}")
        return None, "N/A"


def calculate_metric(record: FinancialStatementData, metric: str) -> tuple:
    """
    Calculate derived metrics from financial data
    Easily extensible for new calculations
    """
    try:
        if metric == "calculated_roe":  # Return on Equity
            if record.net_profit and record.capital and record.capital != 0:
                roe = (float(record.net_profit) / float(record.capital)) * 100
                return roe, f"{roe:.2f}%"

        elif metric == "calculated_profit_margin":  # Profit Margin
            if record.net_profit and record.operating_revenue and record.operating_revenue != 0:
                margin = (float(record.net_profit) / float(record.operating_revenue)) * 100
                return margin, f"{margin:.2f}%"

        elif metric == "calculated_gross_margin":  # Gross Margin
            if record.gross_profit and record.operating_revenue and record.operating_revenue != 0:
                margin = (float(record.gross_profit) / float(record.operating_revenue)) * 100
                return margin, f"{margin:.2f}%"

        elif metric == "calculated_operating_margin":  # Operating Margin
            if record.operating_profit and record.operating_revenue and record.operating_revenue != 0:
                margin = (float(record.operating_profit) / float(record.operating_revenue)) * 100
                return margin, f"{margin:.2f}%"

        elif metric == "calculated_revenue_growth":  # Would need previous period data
            # This would require comparing with previous period
            # Implementation depends on your specific needs
            pass

        elif metric == "calculated_total_income":  # Combined income
            total = 0
            if record.operating_revenue:
                total += float(record.operating_revenue)
            if record.other_income:
                total += float(record.other_income)
            if record.non_operating_income:
                total += float(record.non_operating_income)
            return total, f"{total:,.0f}"

        elif metric == "calculated_total_expenses":  # Combined expenses
            total = 0
            if record.cost_of_goods_sold:
                total += float(record.cost_of_goods_sold)
            if record.selling_admin_expenses:
                total += float(record.selling_admin_expenses)
            if record.financial_expenses:
                total += float(record.financial_expenses)
            if record.other_expenses:
                total += float(record.other_expenses)
            return total, f"{total:,.0f}"

        # Add more calculated metrics here as needed

        return None, "N/A"

    except Exception as e:
        logger.warning(f"Error calculating metric {metric}: {str(e)}")
        return None, "N/A"