from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import re
from typing import Dict, List, Any
from bs4 import BeautifulSoup
import pandas as pd


class NoticeContentScraper:
    def __init__(self):
        self.driver = None
        self.setup_driver()

    def setup_driver(self):
        """Setup Chrome driver optimized for content scraping"""
        chrome_options = Options()

        # Performance optimizations
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

        # Don't disable images for content scraping
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.implicitly_wait(5)
            self.driver.set_page_load_timeout(30)
            print("Content scraper driver initialized")
        except Exception as e:
            print(f"Error initializing driver: {e}")
            raise

    def extract_sheet_ids(self, url: str) -> List[str]:
        """Extract all available sheet IDs from the page"""
        sheet_ids = []

        try:
            self.driver.get(url)
            time.sleep(2)

            # Look for sheet navigation elements
            # Common patterns for sheet links
            sheet_selectors = [
                "a[href*='sheetId=']",
                "button[onclick*='sheetId']",
                "div.sheet-tab",
                "ul.nav-tabs a",
                "div.tab-content a"
            ]

            for selector in sheet_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        # Extract sheetId from href or onclick
                        href = elem.get_attribute('href') or ''
                        onclick = elem.get_attribute('onclick') or ''
                        text = href + onclick

                        # Find sheetId pattern
                        matches = re.findall(r'sheetId=([^&\'"]+)', text)
                        sheet_ids.extend(matches)
                except:
                    continue

            # Remove duplicates and empty values
            sheet_ids = list(set(filter(None, sheet_ids)))

            # If no sheet IDs found, check if there's a default sheet
            if not sheet_ids and 'sheetId=' not in url:
                # Try common sheet IDs
                sheet_ids = ['0', '1']  # Default sheets

            print(f"Found sheet IDs: {sheet_ids}")

        except Exception as e:
            print(f"Error extracting sheet IDs: {e}")

        return sheet_ids

    def scrape_notice_content(self, url: str, scrape_all_sheets: bool = True) -> Dict[str, Any]:
        """Scrape content from a notice URL including all sheets"""
        result = {
            'url': url,
            'sheets': [],
            'all_tables': [],
            'all_text': [],
            'metadata': {},
            'error': None
        }

        try:
            # First, get the main page content
            main_content = self.scrape_single_page(url)
            result['sheets'].append({
                'sheet_id': 'main',
                'url': url,
                'content': main_content
            })

            if scrape_all_sheets:
                # Extract available sheet IDs
                sheet_ids = self.extract_sheet_ids(url)

                # Scrape each sheet
                for sheet_id in sheet_ids:
                    sheet_url = self.build_sheet_url(url, sheet_id)
                    if sheet_url != url:  # Avoid duplicating main page
                        print(f"Scraping sheet: {sheet_id}")
                        sheet_content = self.scrape_single_page(sheet_url)
                        result['sheets'].append({
                            'sheet_id': sheet_id,
                            'url': sheet_url,
                            'content': sheet_content
                        })
                        time.sleep(1)  # Be polite between requests

            # Aggregate all content
            for sheet in result['sheets']:
                if sheet['content']:
                    result['all_tables'].extend(sheet['content'].get('tables', []))
                    result['all_text'].append(sheet['content'].get('text', ''))

        except Exception as e:
            result['error'] = str(e)
            print(f"Error scraping notice: {e}")

        return result

    def build_sheet_url(self, base_url: str, sheet_id: str) -> str:
        """Build URL for a specific sheet"""
        if 'sheetId=' in base_url:
            # Replace existing sheetId
            return re.sub(r'sheetId=[^&]*', f'sheetId={sheet_id}', base_url)
        else:
            # Add sheetId parameter
            separator = '&' if '?' in base_url else '?'
            return f"{base_url}{separator}sheetId={sheet_id}"

    def scrape_single_page(self, url: str) -> Dict[str, Any]:
        """Scrape content from a single page"""
        content = {
            'tables': [],
            'text': '',
            'numbers': [],
            'dates': [],
            'metadata': {}
        }

        try:
            self.driver.get(url)
            time.sleep(3)  # Wait for dynamic content

            # Get page source for BeautifulSoup
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            # Extract tables
            tables = self.extract_tables(soup)
            content['tables'] = tables

            # Extract text content
            text = self.extract_text_content(soup)
            content['text'] = text

            # Extract numbers and financial data
            numbers = self.extract_numbers(text)
            content['numbers'] = numbers

            # Extract dates
            dates = self.extract_dates(text)
            content['dates'] = dates

            # Extract metadata (title, etc.)
            metadata = self.extract_metadata(soup)
            content['metadata'] = metadata

        except Exception as e:
            print(f"Error scraping single page: {e}")

        return content

    def extract_tables(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract all tables from the page"""
        tables = []

        for i, table in enumerate(soup.find_all('table')):
            try:
                # Convert to pandas DataFrame
                df = pd.read_html(str(table))[0]

                # Clean column names
                df.columns = [str(col).strip() for col in df.columns]

                # Convert to dictionary
                table_data = {
                    'table_index': i,
                    'columns': df.columns.tolist(),
                    'data': df.to_dict('records'),
                    'shape': df.shape,
                    'html': str(table)[:500]  # First 500 chars of HTML
                }

                tables.append(table_data)

            except Exception as e:
                print(f"Error parsing table {i}: {e}")
                continue

        return tables

    def extract_text_content(self, soup: BeautifulSoup) -> str:
        """Extract all meaningful text from the page"""
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()

        # Get text
        text = soup.get_text()

        # Clean up text
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)

        return text

    def extract_numbers(self, text: str) -> List[Dict]:
        """Extract numbers and financial figures"""
        numbers = []

        # Pattern for numbers with thousand separators
        number_pattern = r'[\d,]+\.?\d*'

        # Pattern for numbers with context (e.g., "1,234,567 ریال")
        context_pattern = r'([\d,]+\.?\d*)\s*([^\d\s]{1,20})'

        matches = re.finditer(context_pattern, text)

        for match in matches:
            number_str = match.group(1)
            unit = match.group(2).strip()

            # Clean and convert number
            try:
                clean_number = number_str.replace(',', '')
                value = float(clean_number)

                numbers.append({
                    'original': match.group(0),
                    'value': value,
                    'unit': unit,
                    'formatted': number_str
                })
            except:
                continue

        return numbers

    def extract_dates(self, text: str) -> List[str]:
        """Extract dates in various formats"""
        dates = []

        # Persian date pattern (YYYY/MM/DD)
        persian_date_pattern = r'\d{4}/\d{1,2}/\d{1,2}'

        # Find all dates
        found_dates = re.findall(persian_date_pattern, text)
        dates.extend(found_dates)

        return list(set(dates))  # Remove duplicates

    def extract_metadata(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract metadata like title, company name, etc."""
        metadata = {}

        # Try to find title
        title_elem = soup.find('title')
        if title_elem:
            metadata['page_title'] = title_elem.text.strip()

        # Look for specific metadata elements
        meta_selectors = {
            'company_name': ['span.company-name', 'div.company-name', 'td:contains("نام شرکت")'],
            'report_type': ['span.report-type', 'div.report-type', 'td:contains("نوع گزارش")'],
            'period': ['span.period', 'div.period', 'td:contains("دوره")']
        }

        for key, selectors in meta_selectors.items():
            for selector in selectors:
                try:
                    elem = soup.select_one(selector)
                    if elem:
                        metadata[key] = elem.text.strip()
                        break
                except:
                    continue

        return metadata

    def close(self):
        """Close the browser driver"""
        if self.driver:
            try:
                self.driver.quit()
                print("Content scraper driver closed")
            except Exception as e:
                print(f"Error closing driver: {e}")