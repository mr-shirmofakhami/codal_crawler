import asyncio
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from fastapi import HTTPException
import logging
from concurrent.futures import ThreadPoolExecutor

from models import StockNotice, FinancialStatementData
from utils import (
    is_financial_statement,
    get_stored_financial_data,
    save_financial_data,
    check_data_exists
)

logger = logging.getLogger(__name__)


class FinancialStatementService:
    """Service class for financial statement operations with PostgreSQL storage"""

    def __init__(self, scraper_class, content_executor: ThreadPoolExecutor):
        self.scraper_class = scraper_class
        self.content_executor = content_executor

    async def process_financial_statement(
            self,
            notice: StockNotice,
            output_format: str = "json",
            db: Session = None,
            force_refresh: bool = False
    ) -> dict:
        """Process financial statement data with PostgreSQL storage"""

        # Validate notice
        if not is_financial_statement(notice.title):
            raise HTTPException(
                status_code=400,
                detail="This notice is not a financial statement"
            )

        if not notice.html_link:
            raise HTTPException(
                status_code=400,
                detail="Notice has no HTML link"
            )

        # Check for stored data first (unless force refresh)
        if not force_refresh and db and output_format == "json":
            stored_data = get_stored_financial_data(notice.id, db)
            if stored_data:
                logger.info(f"Returning stored data for notice {notice.id}")
                return stored_data

        # Extract fresh data from source
        logger.info(f"Extracting fresh data for notice {notice.id}")
        return await self._scrape_and_process(notice, output_format, db)

    async def _scrape_and_process(
            self,
            notice: StockNotice,
            output_format: str,
            db: Session
    ) -> dict:
        """Scrape and process financial statement data"""

        scraper = self.scraper_class()

        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.content_executor,
                scraper.scrape_income_statement,
                notice.html_link
            )

            if result.get('error'):
                raise HTTPException(
                    status_code=500,
                    detail=f"Scraping error: {result['error']}"
                )

            # Save to PostgreSQL if JSON format and database available
            if output_format == "json" and db and result.get('formatted_data'):
                save_success = await save_financial_data(
                    notice,
                    result.get('formatted_data'),
                    db
                )
                if save_success:
                    logger.info(f"Successfully stored data for notice {notice.id} in PostgreSQL")

            # Format output based on requested format
            return self._format_output(notice, result, output_format)

        except Exception as e:
            logger.error(f"Error processing financial statement {notice.id}: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error processing financial statement: {str(e)}"
            )
        finally:
            scraper.close()

    def _format_output(self, notice: StockNotice, result: dict, output_format: str) -> dict:
        """Format the output based on requested format"""

        base_response = {
            "notice_id": notice.id,
            "symbol": notice.symbol,
            "company_name": notice.company_name,
            "title": notice.title,
            "sheet_name": result.get('sheet_name'),
            "extraction_time": result.get('extraction_time'),
            "from_database": False
        }

        if output_format == "code":
            scraper = self.scraper_class()
            try:
                code_output = scraper.generate_code_output(result)
                base_response["code"] = code_output
            finally:
                scraper.close()

        elif output_format == "dataframe":
            base_response["dataframe"] = (
                result['table_data']['dataframe']
                if result.get('table_data') else None
            )

        else:  # json format (default)
            base_response.update({
                "formatted_data": result.get('formatted_data'),
                "table_data": result.get('table_data')
            })

        return base_response

    async def get_by_notice_id(
            self,
            notice_id: int,
            output_format: str,
            db: Session,
            force_refresh: bool = False
    ) -> dict:
        """Get financial statement by notice ID with PostgreSQL storage"""

        notice = db.query(StockNotice).filter(StockNotice.id == notice_id).first()

        if not notice:
            raise HTTPException(status_code=404, detail="Notice not found")

        return await self.process_financial_statement(
            notice, output_format, db, force_refresh
        )

    async def get_by_exact_title(
            self,
            title: str,
            symbol: Optional[str],
            output_format: str,
            db: Session,
            force_refresh: bool = False
    ) -> dict:
        """Get financial statement by exact title with PostgreSQL storage"""

        query = db.query(StockNotice).filter(StockNotice.title == title)

        if symbol:
            query = query.filter(StockNotice.symbol == symbol)

        notice = query.first()

        if not notice:
            raise HTTPException(
                status_code=404,
                detail="Notice not found with exact title"
            )

        return await self.process_financial_statement(
            notice, output_format, db, force_refresh
        )

    async def batch_extract(
            self,
            notice_ids: List[int],
            output_format: str,
            db: Session
    ) -> dict:
        """Extract financial statements from multiple notices with PostgreSQL storage"""

        from sqlalchemy import or_

        # Get notices that are financial statements
        notices = db.query(StockNotice).filter(
            StockNotice.id.in_(notice_ids),
            or_(
                StockNotice.title.ilike('%اطلاعات و صورت های مالی%'),
                StockNotice.title.ilike('%اطلاعات و صورتهای مالی%'),
                StockNotice.title.ilike('%صورت های سال مالی%'),
                StockNotice.title.ilike('%صورتهای سال مالی%')
            ),
            StockNotice.html_link.isnot(None)
        ).all()

        if not notices:
            raise HTTPException(
                status_code=404,
                detail="No financial statement notices found"
            )

        # Check which notices already have data stored
        stored_notice_ids = set()
        for notice in notices:
            if check_data_exists(notice.id, db):
                stored_notice_ids.add(notice.id)

        logger.info(f"Found {len(stored_notice_ids)} notices with existing data in PostgreSQL")

        async def extract_statement(notice: StockNotice) -> dict:
            """Extract single financial statement"""
            try:
                # Check if data exists in PostgreSQL
                if notice.id in stored_notice_ids and output_format == "json":
                    stored_data = get_stored_financial_data(notice.id, db)
                    if stored_data:
                        return {
                            "notice_id": notice.id,
                            "symbol": notice.symbol,
                            "title": notice.title,
                            "status": "success",
                            "data": stored_data.get('formatted_data'),
                            "from_database": True,
                            "message": "Retrieved from PostgreSQL storage"
                        }

                # Extract fresh data
                result = await self.process_financial_statement(
                    notice, output_format, db, force_refresh=False
                )

                return {
                    "notice_id": notice.id,
                    "symbol": notice.symbol,
                    "title": notice.title,
                    "status": "success",
                    "data": result.get('formatted_data') if output_format == "json" else result.get('code'),
                    "from_database": result.get('from_database', False),
                    "message": "Extracted and stored in PostgreSQL"
                }

            except Exception as e:
                logger.error(f"Error extracting financial statement for notice {notice.id}: {str(e)}")
                return {
                    "notice_id": notice.id,
                    "symbol": notice.symbol,
                    "title": notice.title,
                    "status": "error",
                    "error": str(e),
                    "from_database": False
                }

        # Process with concurrency control
        semaphore = asyncio.Semaphore(2)  # Max 2 concurrent extractions

        async def extract_with_semaphore(notice: StockNotice):
            async with semaphore:
                return await extract_statement(notice)

        tasks = [extract_with_semaphore(notice) for notice in notices]
        results = await asyncio.gather(*tasks)

        # Summary statistics
        success_count = sum(1 for result in results if result['status'] == 'success')
        error_count = len(results) - success_count
        from_db_count = sum(1 for result in results if result.get('from_database', False))

        return {
            "total_requested": len(notice_ids),
            "total_processed": len(results),
            "success_count": success_count,
            "error_count": error_count,
            "from_database_count": from_db_count,
            "output_format": output_format,
            "storage": "PostgreSQL",
            "results": results
        }

    async def get_cached_summary(self, db: Session) -> dict:
        """Get summary of cached financial data in PostgreSQL"""

        try:
            from utils import get_financial_summary_stats
            return get_financial_summary_stats(db)
        except Exception as e:
            logger.error(f"Error getting cached summary: {str(e)}")
            return {
                "error": str(e),
                "total_records": 0,
                "unique_companies": 0
            }