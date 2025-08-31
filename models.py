from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

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