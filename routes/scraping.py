from selenium.webdriver.support import expected_conditions as EC
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from fastapi import APIRouter, Depends, BackgroundTasks
from pydantic import BaseModel
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from sqlalchemy import distinct, func
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional
from sqlalchemy.orm import Session
from typing import Optional
from database import get_db
from models import StockNotice
from services.scraping_service import ultra_fast_scrape
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait



router = APIRouter()

@router.post("/scrape/{symbol}")
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

@router.post("/refresh/{symbol}")
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

@router.post("/append/{symbol}")
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


class MultiSymbolScrapingRequest(BaseModel):
    symbols: Optional[List[str]] = None  # If None or empty, scrape all symbols
    max_pages: Optional[int] = 1
    force_refresh: Optional[bool] = False
    max_workers: Optional[int] = 3


# @router.post("/scrape-symbols")
# async def scrape_multiple_symbols(
#         background_tasks: BackgroundTasks,
#         db: Session = Depends(get_db),
#         start_page: int = Query(1, ge=1, le=20, description="Starting page number"),
#         end_page: int = Query(10, ge=1, le=20, description="Ending page number"),
#         symbols: Optional[List[str]] = Query(None, description="List of symbols to scrape"),
#         force_refresh: bool = Query(False, description="Force refresh existing data"),
#         max_workers: int = Query(2, ge=1, le=10, description="Maximum concurrent workers")
# ):
#     """
#     Ultra-fast scraping for multiple symbols or all symbols in database
#     """
#
#     try:
#         # Calculate max_pages from start_page and end_page
#         max_pages = end_page - start_page + 1
#
#         # Get symbols to scrape
#         if not symbols:
#             db_symbols = db.query(distinct(StockNotice.symbol)).filter(
#                 StockNotice.symbol.isnot(None),
#                 StockNotice.symbol != ""
#             ).all()
#             symbols_to_scrape = [symbol[0] for symbol in db_symbols if symbol[0]]
#
#             if not symbols_to_scrape:
#                 raise HTTPException(
#                     status_code=400,
#                     detail="No symbols found in database. Please add some notices first or provide specific symbols."
#                 )
#         else:
#             symbols_to_scrape = symbols
#
#         # Validate parameters
#         if max_pages < 1 or max_pages > 20:
#             raise HTTPException(
#                 status_code=400,
#                 detail="Page range must result in 1-20 pages total"
#             )
#
#         if max_workers < 1 or max_workers > 10:
#             raise HTTPException(
#                 status_code=400,
#                 detail="max_workers must be between 1 and 10"
#             )
#
#         # Get current record counts
#         current_counts = {}
#         for symbol in symbols_to_scrape:
#             count = db.query(StockNotice).filter(StockNotice.symbol == symbol).count()
#             current_counts[symbol] = count
#
#         # Add background task
#         background_tasks.add_task(
#             ultra_fast_scrape_multiple,
#             symbols_to_scrape,
#             max_pages,
#             force_refresh,
#             max_workers
#         )
#
#         total_records = sum(current_counts.values())
#         estimated_time = len(symbols_to_scrape) * max_pages * 2 / max_workers
#
#         return {
#             "message": f"Started ULTRA-FAST scraping for {len(symbols_to_scrape)} symbols",
#             "symbols_count": len(symbols_to_scrape),
#             "symbols": symbols_to_scrape[:10] if len(symbols_to_scrape) > 10 else symbols_to_scrape,
#             "showing_first": min(10, len(symbols_to_scrape)),
#             "current_total_records": total_records,
#             "current_records_per_symbol": current_counts if len(symbols_to_scrape) <= 10 else "Too many to display",
#             "page_range": f"{start_page}-{end_page}",
#             "max_pages_per_symbol": max_pages,
#             "max_workers": max_workers,
#             "mode": "refresh" if force_refresh else "append",
#             "estimated_time_seconds": int(estimated_time),
#             "estimated_time_minutes": round(estimated_time / 60, 1),
#             "status": "processing"
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to start multi-symbol scraping: {str(e)}")



@router.post("/scrape-symbols")
async def scrape_multiple_symbols(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    start_page: int = Query(1, ge=1, le=20, description="Starting page number"),
    end_page: int = Query(10, ge=1, le=20, description="Ending page number"),
    symbols: Optional[List[str]] = Query(None, description="List of symbols to scrape"),
    force_refresh: bool = Query(False, description="Force refresh existing data"),
    max_workers: int = Query(2, ge=1, le=10, description="Maximum concurrent workers")
):
    """
    Ultra-fast scraping for multiple symbols or all symbols in database
    """

    try:
        # Validate page range
        if start_page > end_page:
            raise HTTPException(
                status_code=400,
                detail="start_page must be less than or equal to end_page"
            )

        # Calculate total pages to scrape
        total_pages = end_page - start_page + 1

        # Get symbols to scrape
        if not symbols:
            db_symbols = db.query(distinct(StockNotice.symbol)).filter(
                StockNotice.symbol.isnot(None),
                StockNotice.symbol != ""
            ).all()
            symbols_to_scrape = [symbol[0] for symbol in db_symbols if symbol[0]]

            if not symbols_to_scrape:
                raise HTTPException(
                    status_code=400,
                    detail="No symbols found in database. Please add some notices first or provide specific symbols."
                )
        else:
            symbols_to_scrape = symbols

        # Validate parameters
        if total_pages < 1 or total_pages > 20:
            raise HTTPException(
                status_code=400,
                detail="Page range must result in 1-20 pages total"
            )

        if max_workers < 1 or max_workers > 10:
            raise HTTPException(
                status_code=400,
                detail="max_workers must be between 1 and 10"
            )

        # Get current record counts
        current_counts = {}
        for symbol in symbols_to_scrape:
            count = db.query(StockNotice).filter(StockNotice.symbol == symbol).count()
            current_counts[symbol] = count

        # Add background task
        background_tasks.add_task(
            ultra_fast_scrape_multiple,
            symbols_to_scrape,
            start_page,  # Pass start_page
            end_page,    # Pass end_page
            force_refresh,
            max_workers
        )

        total_records = sum(current_counts.values())
        estimated_time = len(symbols_to_scrape) * total_pages * 2 / max_workers

        return {
            "message": f"Started ULTRA-FAST scraping for {len(symbols_to_scrape)} symbols",
            "symbols_count": len(symbols_to_scrape),
            "symbols": symbols_to_scrape[:10] if len(symbols_to_scrape) > 10 else symbols_to_scrape,
            "showing_first": min(10, len(symbols_to_scrape)),
            "current_total_records": total_records,
            "current_records_per_symbol": current_counts if len(symbols_to_scrape) <= 10 else "Too many to display",
            "page_range": f"{start_page}-{end_page}",
            "max_pages_per_symbol": total_pages,
            "max_workers": max_workers,
            "mode": "refresh" if force_refresh else "append",
            "estimated_time_seconds": int(estimated_time),
            "estimated_time_minutes": round(estimated_time / 60, 1),
            "status": "processing"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start multi-symbol scraping: {str(e)}")

# # Background task function for multiple symbols
# async def ultra_fast_scrape_multiple(
#         symbols: List[str],
#         max_pages: int,
#         force_refresh: bool,
#         max_workers: int
# ):
#     """Background task to scrape multiple symbols"""
#
#     print(f"🚀 Starting ULTRA-FAST multi-symbol scraping")
#     print(f"📊 Symbols: {len(symbols)}")
#     print(f"📄 Pages per symbol: {max_pages}")
#     print(f"🔄 Mode: {'Refresh' if force_refresh else 'Append'}")
#     print(f"👥 Workers: {max_workers}")
#
#     start_time = time.time()
#
#     def scrape_single_symbol_wrapper(symbol: str):
#         """Wrapper to call your existing ultra_fast_scrape function (NOT ASYNC)"""
#         try:
#             print(f"🎯 Starting scraping for symbol: {symbol}")
#             symbol_start = time.time()
#
#             # Call your existing ultra_fast_scrape function WITHOUT await
#             # Since ultra_fast_scrape is NOT async
#             result = ultra_fast_scrape(symbol, max_pages, force_refresh)
#
#             symbol_time = time.time() - symbol_start
#             print(f"✅ Completed {symbol} in {symbol_time:.1f} seconds")
#
#             return {"symbol": symbol, "status": "success", "time": symbol_time, "result": result}
#
#         except Exception as e:
#             print(f"❌ Failed to scrape symbol {symbol}: {e}")
#             return {"symbol": symbol, "status": "failed", "error": str(e)}
#
#     # Use ThreadPoolExecutor for concurrent execution (since ultra_fast_scrape is sync)
#     results = []
#     total_symbols = len(symbols)
#
#     # Process symbols in batches
#     for i in range(0, total_symbols, max_workers):
#         batch = symbols[i:i + max_workers]
#         batch_num = (i // max_workers) + 1
#         total_batches = (total_symbols + max_workers - 1) // max_workers
#
#         print(f"📦 Processing batch {batch_num}/{total_batches}: {batch}")
#
#         # Use ThreadPoolExecutor to run sync functions concurrently
#         import concurrent.futures
#
#         with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
#             # Submit tasks
#             futures = {executor.submit(scrape_single_symbol_wrapper, symbol): symbol for symbol in batch}
#
#             # Collect results
#             for future in concurrent.futures.as_completed(futures):
#                 symbol = futures[future]
#                 try:
#                     result = future.result(timeout=300)  # 5 minute timeout per symbol
#                     results.append(result)
#                 except concurrent.futures.TimeoutError:
#                     print(f"⏰ Timeout for symbol: {symbol}")
#                     results.append({"symbol": symbol, "status": "failed", "error": "Timeout"})
#                 except Exception as exc:
#                     print(f"💥 Exception for symbol {symbol}: {exc}")
#                     results.append({"symbol": symbol, "status": "failed", "error": str(exc)})
#
#         # Add delay between batches
#         if i + max_workers < total_symbols:
#             print(f"⏳ Waiting 5 seconds before next batch...")
#             time.sleep(5)
#
#     # Calculate final statistics
#     end_time = time.time()
#     total_time = end_time - start_time
#
#     successful = [r for r in results if r.get("status") == "success"]
#     failed = [r for r in results if r.get("status") == "failed"]
#
#     print(f"\n🎉 ULTRA-FAST Multi-Symbol Scraping Completed!")
#     print(f"⏱️  Total time: {total_time:.1f} seconds ({total_time / 60:.1f} minutes)")
#     print(f"📊 Total symbols processed: {total_symbols}")
#     print(f"✅ Successful: {len(successful)}")
#     print(f"❌ Failed: {len(failed)}")
#     print(f"📈 Average time per symbol: {total_time / total_symbols:.1f} seconds")
#
#     if successful:
#         success_symbols = [r['symbol'] for r in successful]
#         print(f"✅ Successfully scraped: {success_symbols}")
#
#     if failed:
#         failed_symbols = [r['symbol'] for r in failed]
#         print(f"❌ Failed symbols: {failed_symbols}")
#
#     return {
#         "total_time": total_time,
#         "successful_count": len(successful),
#         "failed_count": len(failed),
#         "total_symbols": total_symbols,
#         "successful_symbols": [r['symbol'] for r in successful],
#         "failed_symbols": [r['symbol'] for r in failed]
#     }



async def ultra_fast_scrape_multiple(
    symbols: List[str],
    start_page: int,
    end_page: int,
    force_refresh: bool,
    max_workers: int
):
    """Background task to scrape multiple symbols"""

    print(f"🚀 Starting ULTRA-FAST multi-symbol scraping")
    print(f"📊 Symbols: {len(symbols)}")
    print(f"📄 Pages per symbol: {start_page}-{end_page}")
    print(f"🔄 Mode: {'Refresh' if force_refresh else 'Append'}")
    print(f"👥 Workers: {max_workers}")

    start_time = time.time()

    def scrape_single_symbol_wrapper(symbol: str):
        """Wrapper to call your existing ultra_fast_scrape function (NOT ASYNC)"""
        try:
            print(f"🎯 Starting scraping for symbol: {symbol}")
            symbol_start = time.time()

            # Call your existing ultra_fast_scrape function WITHOUT await
            # Since ultra_fast_scrape is NOT async
            result = ultra_fast_scrape(symbol, start_page, end_page, force_refresh)

            symbol_time = time.time() - symbol_start
            print(f"✅ Completed {symbol} in {symbol_time:.1f} seconds")

            return {"symbol": symbol, "status": "success", "time": symbol_time, "result": result}

        except Exception as e:
            print(f"❌ Failed to scrape symbol {symbol}: {e}")
            return {"symbol": symbol, "status": "failed", "error": str(e)}

    # Use ThreadPoolExecutor for concurrent execution (since ultra_fast_scrape is sync)
    results = []
    total_symbols = len(symbols)

    # Process symbols in batches
    for i in range(0, total_symbols, max_workers):
        batch = symbols[i:i + max_workers]
        batch_num = (i // max_workers) + 1
        total_batches = (total_symbols + max_workers - 1) // max_workers

        print(f"📦 Processing batch {batch_num}/{total_batches}: {batch}")

        # Use ThreadPoolExecutor to run sync functions concurrently
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks
            futures = {executor.submit(scrape_single_symbol_wrapper, symbol): symbol for symbol in batch}

            # Collect results
            for future in concurrent.futures.as_completed(futures):
                symbol = futures[future]
                try:
                    result = future.result(timeout=300)  # 5 minute timeout per symbol
                    results.append(result)
                except concurrent.futures.TimeoutError:
                    print(f"⏰ Timeout for symbol: {symbol}")
                    results.append({"symbol": symbol, "status": "failed", "error": "Timeout"})
                except Exception as exc:
                    print(f"💥 Exception for symbol {symbol}: {exc}")
                    results.append({"symbol": symbol, "status": "failed", "error": str(exc)})

        # Add delay between batches
        if i + max_workers < total_symbols:
            print(f"⏳ Waiting 5 seconds before next batch...")
            time.sleep(5)

    # Calculate final statistics
    end_time = time.time()
    total_time = end_time - start_time

    successful = [r for r in results if r.get("status") == "success"]
    failed = [r for r in results if r.get("status") == "failed"]

    print(f"\n🎉 ULTRA-FAST Multi-Symbol Scraping Completed!")
    print(f"⏱️  Total time: {total_time:.1f} seconds ({total_time / 60:.1f} minutes)")
    print(f"📊 Total symbols processed: {total_symbols}")
    print(f"✅ Successful: {len(successful)}")
    print(f"❌ Failed: {len(failed)}")
    print(f"📈 Average time per symbol: {total_time / total_symbols:.1f} seconds")

    return {
        "total_time": total_time,
        "successful_count": len(successful),
        "failed_count": len(failed),
        "total_symbols": total_symbols,
        "successful_symbols": [r['symbol'] for r in successful],
        "failed_symbols": [r['symbol'] for r in failed]
    }


# GET endpoint to check scraping status (optional)
@router.get("/scrape-symbols/status")
async def get_multi_scraping_status(db: Session = Depends(get_db)):
    """Get current database statistics"""
    try:
        total_notices = db.query(StockNotice).count()
        unique_symbols = db.query(distinct(StockNotice.symbol)).count()

        # Get top 10 symbols by notice count
        top_symbols = db.query(
            StockNotice.symbol,
            func.count(StockNotice.id).label('count')
        ).group_by(StockNotice.symbol).order_by(
            func.count(StockNotice.id).desc()
        ).limit(10).all()

        return {
            "total_notices": total_notices,
            "unique_symbols": unique_symbols,
            "top_symbols": [{"symbol": s.symbol, "count": s.count} for s in top_symbols],
            "status": "Database statistics retrieved"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")


def setup_chrome_driver():
    """Setup Chrome driver with enhanced stability and timeout handling"""
    options = webdriver.ChromeOptions()

    # Existing options...
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    # Enhanced timeout and stability options
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--disable-background-timer-throttling')
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--disable-field-trial-config')
    options.add_argument('--disable-back-forward-cache')
    options.add_argument('--disable-background-networking')
    options.add_argument('--disable-default-apps')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-sync')
    options.add_argument('--disable-translate')
    options.add_argument('--hide-scrollbars')
    options.add_argument('--metrics-recording-only')
    options.add_argument('--mute-audio')
    options.add_argument('--no-first-run')
    options.add_argument('--safebrowsing-disable-auto-update')
    options.add_argument('--disable-ipc-flooding-protection')

    # Memory and performance optimization
    options.add_argument('--memory-pressure-off')
    options.add_argument('--max_old_space_size=4096')

    # Page load strategy
    options.page_load_strategy = 'eager'  # Don't wait for all resources

    try:
        driver = webdriver.Chrome(options=options)

        # Set timeouts - IMPORTANT!
        driver.set_page_load_timeout(30)  # Increase from default
        driver.implicitly_wait(10)  # Increase implicit wait

        # Additional timeouts
        driver.set_script_timeout(30)

        print("Chrome driver initialized with enhanced stability optimizations")
        return driver

    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        raise


def scrape_page_with_retry(driver, url, retries=3, delay=5):
    """Scrape page with retry logic for timeout issues"""
    for attempt in range(retries):
        try:
            print(f"Loading page (attempt {attempt + 1}/{retries}): {url}")
            driver.get(url)

            # Wait for page to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            print(f"✅ Page loaded successfully on attempt {attempt + 1}")
            return True

        except TimeoutException as e:
            print(f"⏰ Timeout on attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                print(f"⏳ Waiting {delay} seconds before retry...")
                time.sleep(delay)
            else:
                print(f"❌ Failed to load page after {retries} attempts")
                return False
        except Exception as e:
            print(f"❌ Error loading page on attempt {attempt + 1}: {e}")
            if attempt < retries - 1:
                print(f"⏳ Waiting {delay} seconds before retry...")
                time.sleep(delay)
            else:
                print(f"❌ Failed to load page after {retries} attempts")
                return False

    return False


