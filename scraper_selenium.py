from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import time
import urllib.parse


class CodalSeleniumScraper:
    def __init__(self):
        self.driver = None
        self.setup_driver()

    def setup_driver(self):
        """Setup Chrome driver with maximum speed optimizations"""
        chrome_options = Options()

        # Maximum performance optimizations
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--window-size=1024,768")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

        # Disable unnecessary features
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")

        # Set page load strategy to normal for stability
        chrome_options.page_load_strategy = 'normal'

        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.implicitly_wait(3)  # Slightly longer wait for stability

            # Set reasonable timeouts
            self.driver.set_page_load_timeout(15)
            self.driver.set_script_timeout(10)

            print("Chrome driver initialized with stability optimizations")
        except Exception as e:
            print(f"Error initializing Chrome driver: {e}")
            raise

    def scrape_with_selenium(self, symbol, page_number=1):
        """Stable scraping with stale element handling"""
        if not self.driver:
            raise Exception("Driver not initialized")

        try:
            encoded_symbol = urllib.parse.quote(symbol)
            url = f"https://www.codal.ir/ReportList.aspx?search&Symbol={encoded_symbol}&LetterType=-1&AuditorRef=-1&PageNumber={page_number}&Audited&NotAudited&IsNotAudited=false&Childs&Mains&Publisher=false&CompanyState=0&ReportingType=-1&Category=-1&CompanyType=1&Consolidatable&NotConsolidatable"

            print(f"Scraping page {page_number} for {symbol}...")

            start_time = time.time()
            self.driver.get(url)

            # Wait for page to load properly
            time.sleep(3)

            # Wait for table to be present
            try:
                wait = WebDriverWait(self.driver, 10)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tr")))
                print("Table loaded successfully")
            except TimeoutException:
                print("Timeout waiting for table to load")

            # Parse with robust element handling
            notices = self.extract_data_robust(symbol)

            total_time = time.time() - start_time
            print(f"Page {page_number} completed in {total_time:.2f}s - Found {len(notices)} notices")

            return notices

        except Exception as e:
            print(f"Error during scraping: {e}")
            return []

    def extract_data_robust(self, symbol):
        """Robust data extraction with stale element handling"""
        notices = []

        try:
            # Wait a bit more for dynamic content
            time.sleep(2)

            # Find table rows with retry mechanism
            rows = []
            max_retries = 3

            for attempt in range(max_retries):
                try:
                    # Try different selectors
                    selectors = [
                        "tr.table__row.ng-scope",
                        "tr.table__row",
                        "tbody tr",
                        "table tr"
                    ]

                    for selector in selectors:
                        try:
                            found_rows = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            if found_rows and len(found_rows) > 1:  # More than just header
                                rows = found_rows
                                print(f"Found {len(rows)} rows using selector: {selector}")
                                break
                        except:
                            continue

                    if rows:
                        break

                    if attempt < max_retries - 1:
                        print(f"Retry {attempt + 1}: waiting for rows to stabilize...")
                        time.sleep(2)

                except Exception as e:
                    print(f"Error finding rows on attempt {attempt + 1}: {e}")

            if not rows:
                print("No table rows found")
                return []

            print(f"Processing {len(rows)} rows...")

            # Process each row with individual error handling
            for i, row in enumerate(rows):
                try:
                    # Skip header row
                    if i == 100:
                        continue

                    # Extract data from this row with retry mechanism
                    notice_data = self.extract_row_data_safe(row, symbol, i)

                    if notice_data:
                        notices.append(notice_data)
                        print(f"Row {i}: Extracted - {notice_data.get('title', '')[:50]}...")

                except Exception as e:
                    print(f"Error processing row {i}: {e}")
                    continue

            print(f"Robust extraction completed: {len(notices)} notices")
            return notices

        except Exception as e:
            print(f"Robust extraction failed: {e}")
            return self.fallback_extraction(symbol)

    def extract_row_data_safe(self, row, symbol, row_index):
        """Safely extract data from a single row with stale element handling"""
        max_retries = 2

        for attempt in range(max_retries):
            try:
                # Re-find the row element to avoid stale reference
                if attempt > 0:
                    time.sleep(1)
                    # Re-find all rows and get the specific one
                    all_rows = self.driver.find_elements(By.CSS_SELECTOR, "tr")
                    if row_index < len(all_rows):
                        row = all_rows[row_index]
                    else:
                        return None

                # Get all cells in this row
                cells = row.find_elements(By.TAG_NAME, "td")

                if len(cells) < 4:
                    return None

                # Extract data safely
                symbol_text = ""
                company_name = ""
                title = ""
                detail_link = ""
                publish_time = ""

                # Column 0: Symbol (نماد)
                try:
                    if len(cells) > 0:
                        cell_text = cells[0].text.strip()
                        # Try to find strong element
                        try:
                            strong_elem = cells[0].find_element(By.TAG_NAME, "strong")
                            symbol_text = strong_elem.text.strip()
                        except:
                            symbol_text = cell_text
                except Exception as e:
                    print(f"Error extracting symbol: {e}")

                # Column 1: Company Name (نام شرکت)
                try:
                    if len(cells) > 1:
                        cell_text = cells[1].text.strip()
                        # Try to find span element
                        try:
                            span_elem = cells[1].find_element(By.TAG_NAME, "span")
                            company_name = span_elem.text.strip()
                        except:
                            company_name = cell_text
                except Exception as e:
                    print(f"Error extracting company name: {e}")

                # Column 3: Title and Link (4th column)
                try:
                    if len(cells) > 3:
                        # Try to find link first
                        try:
                            link_elem = cells[3].find_element(By.TAG_NAME, "a")
                            title = link_elem.text.strip()
                            detail_link = link_elem.get_attribute("href")

                            # Convert relative URL to absolute if needed
                            if detail_link and not detail_link.startswith('http'):
                                detail_link = f"https://www.codal.ir{detail_link}"

                        except:
                            # Fallback to cell text
                            title = cells[3].text.strip()
                except Exception as e:
                    print(f"Error extracting title: {e}")

                # Column 6: Publish Time (زمان انتشار)
                try:
                    if len(cells) > 6:
                        cell_text = cells[6].text.strip()
                        # Try to find span element
                        try:
                            span_elem = cells[6].find_element(By.TAG_NAME, "span")
                            publish_time = span_elem.text.strip()
                        except:
                            publish_time = cell_text
                except Exception as e:
                    print(f"Error extracting publish time: {e}")

                # Return data if we have essential information
                if title and len(title) > 5:
                    return {
                        'symbol': symbol_text or symbol,
                        'company_name': company_name,
                        'title': title,
                        'letter_code': '',
                        'send_time': '',
                        'publish_date': publish_time,
                        'tracking_number': '',
                        'detail_link': detail_link,
                        'link': detail_link
                    }

                return None

            except StaleElementReferenceException as e:
                print(f"Stale element on attempt {attempt + 1}, retrying...")
                if attempt == max_retries - 1:
                    print(f"Failed to extract row {row_index} after {max_retries} attempts")
                    return None
                continue

            except Exception as e:
                print(f"Error extracting row data on attempt {attempt + 1}: {e}")
                return None

        return None

    def fallback_extraction(self, symbol):
        """Fallback extraction method"""
        notices = []

        try:
            print("Using fallback extraction method...")

            # Get all links that look like reports
            links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='Decision.aspx'], a[href*='ReportView.aspx']")

            print(f"Fallback: Found {len(links)} potential report links")

            for i, link in enumerate(links[:10]):  # Limit to 10 for speed
                try:
                    href = link.get_attribute("href")
                    title = link.text.strip()

                    if title and len(title) > 5:
                        if href and not href.startswith('http'):
                            href = f"https://www.codal.ir{href}"

                        notices.append({
                            'symbol': symbol,
                            'company_name': '',
                            'title': title,
                            'letter_code': '',
                            'send_time': '',
                            'publish_date': '',
                            'tracking_number': '',
                            'detail_link': href,
                            'link': href
                        })

                        print(f"Fallback extracted: {title[:30]}...")

                except Exception as e:
                    print(f"Error in fallback extraction for link {i}: {e}")
                    continue

            return notices

        except Exception as e:
            print(f"Fallback extraction failed: {e}")
            return []

    def scrape_multiple_pages(self, symbol, max_pages=1):
        """Stable multi-page scraping"""
        all_notices = []

        print(f"Starting stable scraping for {symbol} - {max_pages} pages")
        total_start = time.time()

        for page in range(1, max_pages + 1):
            page_start = time.time()

            notices = self.scrape_with_selenium(symbol, page)

            page_time = time.time() - page_start

            if len(notices) == 0:
                print(f"No notices found on page {page}. Stopping.")
                break

            all_notices.extend(notices)
            print(f"Page {page}: {page_time:.1f}s - {len(notices)} notices - Total: {len(all_notices)}")

            # Small delay between pages for stability
            if page < max_pages:
                time.sleep(1)

        total_time = time.time() - total_start
        print(f"Stable scraping completed in {total_time:.2f}s - {len(all_notices)} total notices")

        return all_notices

    def close(self):
        """Close the browser driver"""
        if self.driver:
            try:
                self.driver.quit()
                print("Driver closed successfully")
            except Exception as e:
                print(f"Error closing driver: {e}")