from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
import time

from notice_content_scraper import NoticeContentScraper
import asyncio
from concurrent.futures import ThreadPoolExecutor

from database import get_db, engine
from models import Base, StockNotice
from scraper_selenium import CodalSeleniumScraper

# Create tables
Base.metadata.create_all(bind=engine)

content_executor = ThreadPoolExecutor(max_workers=3)


app = FastAPI(title="Ultra-Fast Codal Scraper")


@app.post("/scrape/{symbol}")
async def scrape_symbol(
        symbol: str,
        background_tasks: BackgroundTasks,
        max_pages: Optional[int] = 1,
        force_refresh: Optional[bool] = False,
        db: Session = Depends(get_db)
):
    """Ultra-fast scraping with targeted data extraction"""
    current_count = db.query(StockNotice).filter(StockNotice.symbol == symbol).count()

    background_tasks.add_task(ultra_fast_scrape, symbol, max_pages, force_refresh)

    return {
        "message": f"Started ULTRA-FAST scraping for symbol: {symbol}",
        "current_records": current_count,
        "max_pages": max_pages,
        "mode": "refresh" if force_refresh else "append",
        "estimated_time": f"{max_pages * 2} seconds"
    }


@app.post("/refresh/{symbol}")
async def refresh_symbol(
        symbol: str,
        background_tasks: BackgroundTasks,
        max_pages: Optional[int] = 1,
        db: Session = Depends(get_db)
):
    """Delete all existing records and scrape fresh data"""
    background_tasks.add_task(ultra_fast_scrape, symbol, max_pages, force_refresh=True)

    return {
        "message": f"Started ULTRA-FAST refresh for symbol: {symbol}",
        "action": "All existing records will be deleted and replaced"
    }


@app.post("/append/{symbol}")
async def append_symbol(
        symbol: str,
        background_tasks: BackgroundTasks,
        max_pages: Optional[int] = 1,
        db: Session = Depends(get_db)
):
    """Keep existing records and add only new ones"""
    background_tasks.add_task(ultra_fast_scrape, symbol, max_pages, force_refresh=False)

    return {
        "message": f"Started ULTRA-FAST append for symbol: {symbol}",
        "action": "New records will be added, duplicates skipped based on publish_time"
    }


@app.delete("/symbol/{symbol}")
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


@app.delete("/notice/{notice_id}")
def delete_notice(notice_id: int, db: Session = Depends(get_db)):
    """Delete a specific notice by ID"""
    notice = db.query(StockNotice).filter(StockNotice.id == notice_id).first()

    if not notice:
        raise HTTPException(status_code=404, detail="Notice not found")

    db.delete(notice)
    db.commit()
    return {"message": "Notice deleted successfully"}


@app.get("/count")
def get_count(symbol: Optional[str] = None, db: Session = Depends(get_db)):
    """Get total count of records"""
    query = db.query(StockNotice)

    if symbol:
        query = query.filter(StockNotice.symbol == symbol)

    count = query.count()
    return {"count": count, "symbol": symbol}


@app.get("/symbols")
def get_symbols(db: Session = Depends(get_db)):
    """Get list of all unique symbols"""
    symbols = db.query(StockNotice.symbol).distinct().all()
    return {"symbols": [s[0] for s in symbols if s[0]]}


@app.get("/symbol/{symbol}")
def get_symbol_info(
        symbol: str,
        limit: Optional[int] = 50,
        offset: Optional[int] = 0,
        db: Session = Depends(get_db)
):
    """Get complete information for a symbol"""

    total_count = db.query(StockNotice).filter(StockNotice.symbol == symbol).count()

    if total_count == 0:
        return {
            "symbol": symbol,
            "summary": {"total_records": 0, "message": "No records found"},
            "records": []
        }

    latest = db.query(StockNotice).filter(StockNotice.symbol == symbol) \
        .order_by(StockNotice.created_at.desc()).first()

    oldest = db.query(StockNotice).filter(StockNotice.symbol == symbol) \
        .order_by(StockNotice.created_at.asc()).first()

    records = db.query(StockNotice).filter(StockNotice.symbol == symbol) \
        .order_by(StockNotice.created_at.desc()) \
        .offset(offset).limit(limit).all()

    return {
        "symbol": symbol,
        "summary": {
            "total_records": total_count,
            "company_name": latest.company_name if latest else None,
            "latest_record_date": latest.created_at if latest else None,
            "oldest_record_date": oldest.created_at if oldest else None,
            "has_html_count": db.query(StockNotice).filter(
                StockNotice.symbol == symbol,
                StockNotice.has_html == True
            ).count()
        },
        "records": [
            {
                "id": r.id,
                "title": r.title,
                "company_name": r.company_name,
                "symbol": r.symbol,
                "publish_time": r.publish_time,
                "html_link": r.html_link,
                "has_html": r.has_html,
                "created_at": r.created_at
            }
            for r in records
        ]
    }


def ultra_fast_scrape(symbol: str, max_pages: int, force_refresh: bool = False):
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
                print(f"REFRESH: Deleting {existing_count} existing records for '{symbol}'")
                deleted_count = db.query(StockNotice).filter(StockNotice.symbol == symbol).delete()
                db.commit()
                print(f"REFRESH: Deleted {deleted_count} records")
        else:
            existing_count = db.query(StockNotice).filter(StockNotice.symbol == symbol).count()
            print(f"APPEND: Found {existing_count} existing records for '{symbol}'")

        # Create ultra-fast scraper
        scraper = CodalSeleniumScraper()

        # Get all notices
        all_notices = scraper.scrape_multiple_pages(symbol, max_pages)

        if not all_notices:
            print(f"No notices found for symbol: {symbol}")
            return

        print(f"Processing {len(all_notices)} notices for database...")

        # CHANGED: Ultra-fast duplicate checking based on PUBLISH_TIME instead of title
        existing_publish_times = set()
        if not force_refresh:
            # Get existing publish_times for this symbol
            existing_records = db.query(StockNotice.publish_time).filter(
                StockNotice.symbol == symbol,
                StockNotice.publish_time.isnot(None),
                StockNotice.publish_time != ''
            ).all()
            existing_publish_times = {record[0] for record in existing_records if record[0]}
            print(f"Loaded {len(existing_publish_times)} existing publish_times for duplicate checking")

        # Process notices with proper column mapping
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
                    print(f"Skipping record: title too short or empty")
                    continue

                # CHANGED: Fast duplicate check based on PUBLISH_TIME for this symbol
                if publish_time and publish_time in existing_publish_times:
                    duplicates_count += 1
                    print(f"Skipped DUPLICATE publish_time: {symbol} - {publish_time}")
                    continue

                # CORRECT COLUMN MAPPING based on your requirements
                db_notice_data = {
                    'symbol': safe_truncate(notice_data.get('symbol', ''), 100),
                    'company_name': safe_truncate(notice_data.get('company_name', ''), 500),
                    'title': title,  # Title goes to title column (correct)
                    'letter_code': '',  # Not extracted as per requirements
                    'send_time': '',  # Not extracted as per requirements
                    'publish_time': safe_truncate(publish_time, 100),
                    # Publish time goes to publish_time column (correct)
                    'tracking_number': '',  # Not extracted as per requirements
                    'html_link': notice_data.get('detail_link', ''),  # Link goes to html_link column (correct)
                    'has_html': bool(notice_data.get('detail_link')),
                }

                notice = StockNotice(**db_notice_data)
                new_notices.append(notice)

                # CHANGED: Add publish_time to set to prevent duplicates within this batch
                if publish_time:
                    existing_publish_times.add(publish_time)

                print(f"Added NEW: {symbol} - {publish_time} - {title[:30]}...")

            except Exception as e:
                print(f"Error processing notice: {e}")
                continue

        # Ultra-fast batch insert
        if new_notices:
            print(f"Ultra-fast batch inserting {len(new_notices)} new records...")
            db.add_all(new_notices)
            db.commit()

        total_time = time.time() - total_start_time

        print(f"\nULTRA-FAST scraping completed for '{symbol}' in {total_time:.2f} seconds:")
        print(f"- Total notices scraped: {len(all_notices)}")
        print(f"- New records added: {len(new_notices)}")
        print(f"- Duplicates skipped (based on publish_time): {duplicates_count}")
        print(f"- Speed: {len(all_notices) / total_time:.1f} notices/second")

        final_count = db.query(StockNotice).filter(StockNotice.symbol == symbol).count()
        print(f"- Final total records: {final_count}")

    except Exception as e:
        print(f"Error in ultra-fast scraping: {e}")
        if db:
            db.rollback()
    finally:
        if scraper:
            scraper.close()
        if db:
            db.close()


@app.get("/notice/{notice_id}")
async def get_notice_by_id(
        notice_id: int,
        scrape_content: bool = False,
        db: Session = Depends(get_db)
):
    """Get notice by ID with optional content scraping"""

    notice = db.query(StockNotice).filter(StockNotice.id == notice_id).first()

    if not notice:
        raise HTTPException(status_code=404, detail="Notice not found")

    result = {
        "id": notice.id,
        "symbol": notice.symbol,
        "company_name": notice.company_name,
        "title": notice.title,
        "publish_time": notice.publish_time,
        "html_link": notice.html_link,
        "has_html": notice.has_html,
        "created_at": notice.created_at
    }

    # Optionally scrape content from the link
    if scrape_content and notice.html_link:
        scraper = NoticeContentScraper()
        try:
            loop = asyncio.get_event_loop()
            content = await loop.run_in_executor(
                content_executor,
                scraper.scrape_notice_content,
                notice.html_link,
                True  # scrape_all_sheets
            )
            result["scraped_content"] = content
        finally:
            scraper.close()

    return result


@app.get("/notices/search")
async def search_notices_by_title(
        keyword: str,
        limit: int = 10,
        offset: int = 0,
        scrape_content: bool = False,
        db: Session = Depends(get_db)
):
    """Search notices by keyword in title"""

    # Search in database
    query = db.query(StockNotice).filter(
        StockNotice.title.ilike(f"%{keyword}%")
    )

    total = query.count()
    notices = query.offset(offset).limit(limit).all()

    results = []
    for notice in notices:
        notice_data = {
            "id": notice.id,
            "symbol": notice.symbol,
            "company_name": notice.company_name,
            "title": notice.title,
            "publish_time": notice.publish_time,
            "html_link": notice.html_link,
            "has_html": notice.has_html
        }

        # Optionally scrape content
        if scrape_content and notice.html_link and len(results) < 3:  # Limit to 3 for performance
            scraper = NoticeContentScraper()
            try:
                loop = asyncio.get_event_loop()
                content = await loop.run_in_executor(
                    content_executor,
                    scraper.scrape_notice_content,
                    notice.html_link,
                    False  # Don't scrape all sheets for search results
                )
                notice_data["scraped_content"] = content
            finally:
                scraper.close()

        results.append(notice_data)

    return {
        "keyword": keyword,
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": results
    }


@app.post("/notice/{notice_id}/scrape")
async def scrape_notice_content(
        notice_id: int,
        scrape_all_sheets: bool = True,
        db: Session = Depends(get_db)
):
    """Scrape content from a specific notice"""

    notice = db.query(StockNotice).filter(StockNotice.id == notice_id).first()

    if not notice:
        raise HTTPException(status_code=404, detail="Notice not found")

    if not notice.html_link:
        raise HTTPException(status_code=400, detail="Notice has no HTML link")

    scraper = NoticeContentScraper()
    try:
        loop = asyncio.get_event_loop()
        content = await loop.run_in_executor(
            content_executor,
            scraper.scrape_notice_content,
            notice.html_link,
            scrape_all_sheets
        )

        # Extract summary statistics
        summary = {
            "total_sheets": len(content.get('sheets', [])),
            "total_tables": len(content.get('all_tables', [])),
            "total_numbers": len(content.get('all_text', [])),
            "has_error": content.get('error') is not None
        }

        return {
            "notice_id": notice_id,
            "symbol": notice.symbol,
            "title": notice.title,
            "summary": summary,
            "content": content
        }

    finally:
        scraper.close()


@app.post("/notices/batch-scrape")
async def batch_scrape_notice_content(
        notice_ids: List[int],
        scrape_all_sheets: bool = False,
        db: Session = Depends(get_db)
):
    """Scrape content from multiple notices"""

    notices = db.query(StockNotice).filter(
        StockNotice.id.in_(notice_ids),
        StockNotice.html_link.isnot(None)
    ).all()

    if not notices:
        raise HTTPException(status_code=404, detail="No notices found with HTML links")

    results = []

    # Process in parallel but limit concurrency
    semaphore = asyncio.Semaphore(3)  # Max 3 concurrent scrapes

    async def scrape_with_semaphore(notice):
        async with semaphore:
            scraper = NoticeContentScraper()
            try:
                loop = asyncio.get_event_loop()
                content = await loop.run_in_executor(
                    content_executor,
                    scraper.scrape_notice_content,
                    notice.html_link,
                    scrape_all_sheets
                )

                return {
                    "notice_id": notice.id,
                    "symbol": notice.symbol,
                    "title": notice.title,
                    "status": "success",
                    "sheets_count": len(content.get('sheets', [])),
                    "tables_count": len(content.get('all_tables', [])),
                    "error": content.get('error')
                }
            except Exception as e:
                return {
                    "notice_id": notice.id,
                    "symbol": notice.symbol,
                    "title": notice.title,
                    "status": "error",
                    "error": str(e)
                }
            finally:
                scraper.close()

    # Create tasks for all notices
    tasks = [scrape_with_semaphore(notice) for notice in notices]
    results = await asyncio.gather(*tasks)

    return {
        "total_requested": len(notice_ids),
        "total_processed": len(results),
        "results": results
    }


@app.get("/notices/filter")
async def filter_notices(
        symbol: Optional[str] = None,
        title_contains: Optional[str] = None,
        has_html: Optional[bool] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        db: Session = Depends(get_db)
):
    """Advanced filtering for notices"""

    query = db.query(StockNotice)

    if symbol:
        query = query.filter(StockNotice.symbol == symbol)

    if title_contains:
        query = query.filter(StockNotice.title.ilike(f"%{title_contains}%"))

    if has_html is not None:
        query = query.filter(StockNotice.has_html == has_html)

    if start_date:
        query = query.filter(StockNotice.publish_time >= start_date)

    if end_date:
        query = query.filter(StockNotice.publish_time <= end_date)

    total = query.count()
    notices = query.order_by(StockNotice.publish_time.desc()).offset(offset).limit(limit).all()

    return {
        "total": total,
        "filters": {
            "symbol": symbol,
            "title_contains": title_contains,
            "has_html": has_html,
            "start_date": start_date,
            "end_date": end_date
        },
        "results": [
            {
                "id": n.id,
                "symbol": n.symbol,
                "company_name": n.company_name,
                "title": n.title,
                "publish_time": n.publish_time,
                "html_link": n.html_link,
                "has_html": n.has_html
            } for n in notices
        ]
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)