import logging
import time
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.orm import Session
from database import SessionLocal
from models import StockNotice
from scraper_selenium import CodalSeleniumScraper

logger = logging.getLogger(__name__)

content_executor = ThreadPoolExecutor(max_workers=3)


def ultra_fast_scrape(symbol: str, start_page: int, end_page: int, force_refresh: bool = False):
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
        all_notices = scraper.scrape_multiple_pages(symbol, start_page=start_page, end_page=end_page)

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
