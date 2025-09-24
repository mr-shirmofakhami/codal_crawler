from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, JSON, Index, Numeric, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
from sqlalchemy.sql import func

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    is_superadmin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login = Column(DateTime, nullable=True)



class StockNotice(Base):
    __tablename__ = "stock_notices"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(100), index=True)  # Increased from 50 to 100
    company_name = Column(String(500))  # Increased from 200 to 500
    title = Column(Text)  # Already Text, good
    letter_code = Column(String(100))  # Increased from 20 to 100
    send_time = Column(String(100))  # Increased from 50 to 100
    publish_time = Column(String(100))  # Increased from 50 to 100
    tracking_number = Column(String(100))  # Increased from 50 to 100

    # Links
    html_link = Column(Text, nullable=True)
    pdf_link = Column(Text, nullable=True)
    excel_link = Column(Text, nullable=True)

    # Flags
    has_html = Column(Boolean, default=False)
    has_pdf = Column(Boolean, default=False)
    has_excel = Column(Boolean, default=False)
    has_xbrl = Column(Boolean, default=False)
    has_attachment = Column(Boolean, default=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # # FIXED: Relationship name matches back_populates
    # financial_data = relationship("FinancialStatementData", back_populates="notice")


class FinancialStatementData(Base):
    """Wide table: Each period = 1 row, Each item = 1 column"""
    __tablename__ = "financial_statement_data"

    id = Column(Integer, primary_key=True, index=True)

    # Basic information
    notice_id = Column(Integer, ForeignKey('stock_notices.id'), index=True)
    company_symbol = Column(String(100), index=True)
    company_name = Column(String(500))
    raw_title = Column(Text)
    sheet_name = Column(String(200))  # "صورت سود و زیان"

    # Period information (one row per period)
    period_name = Column(String(200), index=True)  # "دوره منتهي به ۱۴۰۴/۰۳/۳۱"
    period_order = Column(Integer)  # 0, 1, 2
    period_type = Column(String(50))  # "3 ماهه", "6 ماهه", "9 ماهه", "سال مالی"
    audit_status = Column(String(50))  # "حسابرسی شده", "حسابرسی نشده"
    period_date = Column(String(100))  # Date from title

    # Financial items as separate columns (amounts) - SHORTENED NAMES
    operating_revenue = Column(Numeric(20, 2), nullable=True)  # درآمدهاي عملياتي
    cost_of_goods_sold = Column(Numeric(20, 2), nullable=True)  # بهاى تمام شده درآمدهاي عملياتي
    gross_profit = Column(Numeric(20, 2), nullable=True)  # سود(زيان) ناخالص
    selling_admin_expenses = Column(Numeric(20, 2), nullable=True)  # هزينه هاى فروش، ادارى و عمومى
    impairment_expense = Column(Numeric(20, 2), nullable=True)  # هزينه کاهش ارزش دريافتني ها
    other_income = Column(Numeric(20, 2), nullable=True)  # ساير درآمدها
    other_expenses = Column(Numeric(20, 2), nullable=True)  # ساير هزينه‌ها
    operating_profit = Column(Numeric(20, 2), nullable=True)  # سود(زيان) عملياتى
    financial_expenses = Column(Numeric(20, 2), nullable=True)  # هزينه هاى مالى

    investment_income = Column(Numeric(20, 2), nullable=True)  # درآمد سرمایه‌گذاری‌ها
    miscellaneous_income = Column(Numeric(20, 2), nullable=True)  # اقلام متفرقهاقلام متفرقه

    non_operating_income = Column(Numeric(20, 2), nullable=True)  # ساير درآمدها و هزينه هاى غيرعملياتى
    profit_before_tax = Column(Numeric(20, 2), nullable=True)  # سود(زيان) عمليات در حال تداوم قبل از ماليات
    current_year_tax = Column(Numeric(20, 2), nullable=True)  # سال جاري
    prior_years_tax = Column(Numeric(20, 2), nullable=True)  # سال‌هاي قبل
    net_profit_continuing = Column(Numeric(20, 2), nullable=True)  # سود(زيان) خالص عمليات در حال تداوم
    net_profit_discontinued = Column(Numeric(20, 2), nullable=True)  # سود (زيان) خالص عمليات متوقف شده
    net_profit = Column(Numeric(20, 2), nullable=True)  # سود(زيان) خالص
    operational_eps = Column(Numeric(20, 2), nullable=True)  # عملياتي (ريال)
    non_operational_eps = Column(Numeric(20, 2), nullable=True)  # غيرعملياتي (ريال)
    eps_continuing = Column(Numeric(20, 2), nullable=True)  # ناشي از عمليات در حال تداوم
    eps_discontinued = Column(Numeric(20, 2), nullable=True)  # ناشي از عمليات متوقف شده
    basic_eps = Column(Numeric(20, 2), nullable=True)  # سود(زيان) پايه هر سهم
    diluted_eps = Column(Numeric(20, 2), nullable=True)  # سود (زيان) خالص هر سهم – ريال
    capital = Column(Numeric(20, 2), nullable=True)  # سرمايه

    # Financial items as formatted text columns - SHORTENED NAMES
    operating_revenue_fmt = Column(String(100))
    cost_of_goods_sold_fmt = Column(String(100))
    gross_profit_fmt = Column(String(100))
    selling_admin_expenses_fmt = Column(String(100))
    impairment_expense_fmt = Column(String(100))
    other_income_fmt = Column(String(100))
    other_expenses_fmt = Column(String(100))
    operating_profit_fmt = Column(String(100))
    financial_expenses_fmt = Column(String(100))

    investment_income_fmt = Column(String(100))
    miscellaneous_income_fmt = Column(String(100))

    non_operating_income_fmt = Column(String(100))
    profit_before_tax_fmt = Column(String(100))
    current_year_tax_fmt = Column(String(100))
    prior_years_tax_fmt = Column(String(100))
    net_profit_continuing_fmt = Column(String(100))
    net_profit_discontinued_fmt = Column(String(100))
    net_profit_fmt = Column(String(100))
    operational_eps_fmt = Column(String(100))
    non_operational_eps_fmt = Column(String(100))
    eps_continuing_fmt = Column(String(100))
    eps_discontinued_fmt = Column(String(100))
    basic_eps_fmt = Column(String(100))
    diluted_eps_fmt = Column(String(100))
    capital_fmt = Column(String(100))

    # Metadata
    extraction_date = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Add indexes for better performance
    __table_args__ = (
        Index('idx_notice_period', 'notice_id', 'period_name'),
        Index('idx_company_period', 'company_symbol', 'period_name'),
        Index('idx_period_order', 'notice_id', 'period_order'),
    )

    def to_dict(self):
        """Convert model instance to dictionary"""
        return {column.name: getattr(self, column.name) for column in self.__table__.columns}