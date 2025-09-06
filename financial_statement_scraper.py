from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import time
import pandas as pd
from typing import Dict, List, Any, Optional, Union
import json
import re


class FinancialStatementScraper:
    def __init__(self):
        self.driver = None
        self.setup_driver()

    def setup_driver(self):
        """Setup Chrome driver for financial statement scraping"""
        chrome_options = Options()

        # Optimizations for financial data scraping
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.implicitly_wait(5)
            self.driver.set_page_load_timeout(30)
            print("Financial statement scraper initialized")
        except Exception as e:
            print(f"Error initializing driver: {e}")
            raise

    def make_json_safe(self, obj: Any) -> Any:
        """Ensure all objects are JSON serializable"""
        if obj is None:
            return None
        elif isinstance(obj, (str, int, float, bool)):
            return obj
        elif isinstance(obj, list):
            return [self.make_json_safe(item) for item in obj]
        elif isinstance(obj, dict):
            safe_dict = {}
            for key, value in obj.items():
                # Ensure key is string and safe
                safe_key = str(key) if not isinstance(key, str) else key
                safe_dict[safe_key] = self.make_json_safe(value)
            return safe_dict
        else:
            # Convert any other type to string
            return str(obj)

    def scrape_income_statement(self, url: str) -> Dict[str, Any]:
        """Scrape income statement (صورت سود و زیان) from the given URL"""
        result = {
            'url': url,
            'sheet_name': 'صورت سود و زیان',
            'table_data': None,
            'formatted_data': None,
            'raw_html': None,
            'error': None,
            'extraction_time': None
        }

        start_time = time.time()

        try:
            # Navigate to the URL
            print(f"Loading URL: {url}")
            self.driver.get(url)

            # Wait for page to fully load
            time.sleep(5)

            # Wait for any dynamic content to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Try to select income statement sheet
            sheet_selected = self.select_income_statement_sheet()

            if not sheet_selected:
                # Try to extract table anyway (maybe it's already showing)
                print("Sheet selection failed, trying to extract current table...")
                table_data = self.extract_income_statement_table()
                if table_data:
                    result['table_data'] = self.make_json_safe(table_data)
                    result['formatted_data'] = self.make_json_safe(self.format_table_data(table_data))
                    result['raw_html'] = str(table_data.get('html', ''))[:1000]
                else:
                    result['error'] = "Could not select sheet and no table found"
                    return result
            else:
                # Wait for table to load after sheet selection
                time.sleep(3)

                # Extract the table data
                table_data = self.extract_income_statement_table()

                if table_data:
                    result['table_data'] = self.make_json_safe(table_data)
                    result['formatted_data'] = self.make_json_safe(self.format_table_data(table_data))
                    result['raw_html'] = str(table_data.get('html', ''))[:1000]
                else:
                    result['error'] = "Could not extract table data"

            result['extraction_time'] = time.time() - start_time

        except Exception as e:
            result['error'] = str(e)
            print(f"Error scraping income statement: {e}")

        # Ensure entire result is JSON safe
        return self.make_json_safe(result)

    def select_income_statement_sheet(self) -> bool:
        """Select the income statement sheet from dropdown with multiple fallback methods"""

        # Method 1: Try direct URL manipulation first (most reliable)
        try:
            current_url = self.driver.current_url

            # Try different sheetId values that commonly represent income statement
            sheet_ids_to_try = ['1', '0', '2']  # 1 is most common for income statement

            for sheet_id in sheet_ids_to_try:
                try:
                    if 'sheetId=' in current_url:
                        new_url = re.sub(r'sheetId=[^&]*', f'sheetId={sheet_id}', current_url)
                    else:
                        separator = '&' if '?' in current_url else '?'
                        new_url = f"{current_url}{separator}sheetId={sheet_id}"

                    print(f"Trying URL with sheetId={sheet_id}: {new_url}")
                    self.driver.get(new_url)
                    time.sleep(4)

                    # Check if we can find a table
                    if self.check_for_income_statement_table():
                        print(f"Successfully loaded income statement with sheetId={sheet_id}")
                        return True

                except Exception as e:
                    print(f"Failed with sheetId={sheet_id}: {e}")
                    continue

        except Exception as e:
            print(f"URL manipulation method failed: {e}")

        # Method 2: Try JavaScript execution (more reliable than Selenium Select)
        try:
            print("Trying JavaScript method...")

            # Wait for dropdown to be present
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "ddlTable"))
            )

            # Use JavaScript to change the dropdown and trigger change event
            success = self.driver.execute_script("""
                try {
                    var dropdown = document.getElementById('ddlTable');
                    if (!dropdown) return false;

                    // Try to find income statement option
                    var options = dropdown.options;
                    for (var i = 0; i < options.length; i++) {
                        var optionText = options[i].text;
                        if (optionText.includes('صورت سود و زیان') && !optionText.includes('جامع')) {
                            dropdown.selectedIndex = i;
                            dropdown.value = options[i].value;

                            // Trigger change event
                            if (typeof changeSheet === 'function') {
                                changeSheet(options[i].value);
                            } else {
                                var event = new Event('change', { bubbles: true });
                                dropdown.dispatchEvent(event);
                            }
                            return true;
                        }
                    }

                    // Fallback: try value '1'
                    dropdown.value = '1';
                    if (typeof changeSheet === 'function') {
                        changeSheet('1');
                    } else {
                        var event = new Event('change', { bubbles: true });
                        dropdown.dispatchEvent(event);
                    }
                    return true;
                } catch (e) {
                    console.error('JavaScript method error:', e);
                    return false;
                }
            """)

            if success:
                print("JavaScript method successful")
                time.sleep(4)  # Wait for content to load
                return True

        except Exception as e:
            print(f"JavaScript method failed: {e}")

        print("All sheet selection methods failed")
        return False

    def check_for_income_statement_table(self) -> bool:
        """Check if the current page has an income statement table"""
        try:
            # Look for table with financial data
            tables = self.driver.find_elements(By.CSS_SELECTOR, "table")

            for table in tables:
                table_text = table.text
                # Check for income statement indicators
                if any(indicator in table_text for indicator in [
                    "درآمدهاي عملياتي", "سود", "زیان", "هزينه", "درآمد"
                ]):
                    return True
            return False
        except:
            return False

    def extract_income_statement_table(self) -> Optional[Dict[str, Any]]:
        """Extract the income statement table data with improved error handling"""
        try:
            # Wait for any table to be present
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
            )

            # Try different table selectors
            table_selectors = [
                "table.rayanDynamicStatement",
                "table[id]",
                "table",
                ".table-responsive table",
                "div.table-container table"
            ]

            table = None
            for selector in table_selectors:
                try:
                    tables = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for t in tables:
                        # Check if this table contains financial data
                        table_text = t.text
                        if any(keyword in table_text for keyword in [
                            "درآمدهاي عملياتي", "سود", "زیان", "هزينه", "بهاى تمام شده"
                        ]):
                            table = t
                            print(f"Found financial table with selector: {selector}")
                            break
                    if table:
                        break
                except Exception as e:
                    print(f"Error with selector {selector}: {e}")
                    continue

            if not table:
                print("No financial table found")
                return None

            # Get table HTML
            table_html = table.get_attribute('outerHTML')

            # Extract table structure safely
            result = {
                'headers': [],
                'rows': [],
                'html': table_html[:2000],  # Limit HTML size
                'row_count': 0,
                'column_count': 0
            }

            # Extract headers safely
            try:
                header_rows = table.find_elements(By.CSS_SELECTOR, "thead tr")
                for header_row in header_rows:
                    header_cells = header_row.find_elements(By.TAG_NAME, "th")
                    header_data = []
                    for cell in header_cells:
                        if not cell.get_attribute('hidden'):
                            text = cell.text.strip()
                            colspan = cell.get_attribute('colspan') or '1'
                            rowspan = cell.get_attribute('rowspan') or '1'

                            # Ensure all values are JSON serializable
                            header_data.append({
                                'text': str(text),
                                'colspan': int(colspan) if colspan.isdigit() else 1,
                                'rowspan': int(rowspan) if rowspan.isdigit() else 1
                            })
                    if header_data:
                        result['headers'].append(header_data)
            except Exception as e:
                print(f"Error extracting headers: {e}")
                result['headers'] = []

            # Extract body rows safely
            try:
                body_rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                for row in body_rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    row_data = []

                    for cell in cells:
                        if cell.get_attribute('hidden'):
                            continue

                        text = cell.text.strip()
                        original_text = str(text)  # Ensure string

                        # Clean numeric values
                        cleaned_text = text.replace(',', '').replace('٬', '')

                        # Handle negative numbers in parentheses
                        is_negative = False
                        if cleaned_text.startswith('(') and cleaned_text.endswith(')'):
                            cleaned_text = cleaned_text[1:-1]
                            is_negative = True

                        # Try to convert to number
                        numeric_value = None
                        is_number = False

                        if cleaned_text and cleaned_text not in ['', ' ', '--', '۰']:
                            try:
                                numeric_value = float(cleaned_text)
                                if is_negative:
                                    numeric_value = -numeric_value
                                is_number = True
                            except:
                                numeric_value = None
                                is_number = False

                        # Get cell classes for styling info
                        classes = cell.get_attribute('class') or ''

                        # Ensure all data is JSON serializable
                        cell_data = {
                            'text': original_text,
                            'value': numeric_value,
                            'is_number': is_number,
                            'classes': str(classes),
                            'is_header': 'right-aligne' in classes or 'dynamic_desc' in classes,
                            'is_total': 'dynamic_comp' in classes
                        }

                        row_data.append(cell_data)

                    if row_data:
                        result['rows'].append(row_data)

                result['row_count'] = len(result['rows'])
                result['column_count'] = len(result['rows'][0]) if result['rows'] else 0

            except Exception as e:
                print(f"Error extracting rows: {e}")
                result['rows'] = []

            # Skip pandas extraction to avoid issues
            result['dataframe'] = None

            return result

        except Exception as e:
            print(f"Error extracting table: {e}")
            return None

    def format_table_data(self, table_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format the table data into a structured, JSON-safe format"""
        if not table_data or not table_data.get('rows'):
            return {
                'periods': [],
                'items': [],
                'key_metrics': {},
                'summary': {
                    'total_items': 0,
                    'total_periods': 0,
                    'key_metrics_found': 0
                }
            }

        formatted = {
            'periods': [],
            'items': [],
            'key_metrics': {},
            'summary': {}
        }

        # Extract periods from headers safely
        if table_data.get('headers'):
            for header_row in table_data['headers']:
                if isinstance(header_row, list):
                    for cell in header_row:
                        if isinstance(cell, dict) and 'text' in cell:
                            text = str(cell['text']).strip()
                            if text and ('دوره' in text or 'حسابرسی' in text or '1404' in text or '1403' in text):
                                if text not in formatted['periods']:
                                    formatted['periods'].append(text)

        # Extract financial items safely
        rows = table_data.get('rows', [])
        for row_index, row in enumerate(rows):
            if not row or not isinstance(row, list):
                continue

            # First cell is usually the item description
            if len(row) > 0 and isinstance(row[0], dict) and 'text' in row[0]:
                item_name = str(row[0]['text']).strip()
            else:
                continue

            # Skip empty rows, section headers, or rows with colons
            if not item_name or item_name == '' or ':' in item_name or item_name == '-':
                continue

            # Skip rows that are just numbers
            if item_name.replace(',', '').replace('(', '').replace(')', '').replace('-', '').isdigit():
                continue

            # Create item data safely
            item_data = {
                'name': item_name,
                'values': [],
                'is_total': bool(row[0].get('is_total', False)) if isinstance(row[0], dict) else False,
                'row_index': row_index
            }

            # Extract values for each period (skip first column which is the name)
            for i in range(1, len(row)):
                if i < len(row) and isinstance(row[i], dict):
                    cell = row[i]

                    if cell.get('is_number') and cell.get('value') is not None:
                        item_data['values'].append({
                            'amount': float(cell['value']),
                            'formatted': str(cell.get('text', ''))
                        })
                    else:
                        item_data['values'].append({
                            'amount': None,
                            'formatted': str(cell.get('text', ''))
                        })

            formatted['items'].append(item_data)

        # Extract key metrics safely
        key_metric_patterns = {
            'operating_revenue': ['درآمدهاي عملياتي', 'درآمدهای عملیاتی', 'فروش'],
            'gross_profit': ['سود(زيان) ناخالص', 'سود ناخالص', 'سود(زیان) ناخالص'],
            'operating_profit': ['سود(زيان) عملياتى', 'سود عملیاتی', 'سود(زیان) عملیاتی'],
            'net_profit': ['سود(زيان) خالص', 'سود خالص', 'سود(زیان) خالص'],
            'eps': ['سود(زيان) پايه هر سهم', 'سود پایه هر سهم', 'سود (زیان) خالص هر سهم'],
            'capital': ['سرمايه', 'سرمایه']
        }

        for metric_key, patterns in key_metric_patterns.items():
            for item in formatted['items']:
                item_name = item['name']
                for pattern in patterns:
                    if pattern in item_name:
                        formatted['key_metrics'][metric_key] = {
                            'name': item_name,
                            'values': item['values'][:]  # Create a copy to avoid reference issues
                        }
                        break
                if metric_key in formatted['key_metrics']:
                    break

        # Add summary information safely
        formatted['summary'] = {
            'total_items': len(formatted['items']),
            'total_periods': len(formatted['periods']),
            'key_metrics_found': len(formatted['key_metrics'])
        }

        return formatted

    def generate_code_output(self, table_data: Dict[str, Any]) -> str:
        """Generate code representation of the table data"""
        if not table_data or not table_data.get('formatted_data'):
            return "# No data available"

        try:
            formatted_data = table_data['formatted_data']

            code = "# Income Statement Data (صورت سود و زیان)\n\n"
            code += "income_statement_data = {\n"

            # Add periods
            periods = formatted_data.get('periods', [])
            code += f"    'periods': {periods},\n"
            code += "    'items': [\n"

            # Add each item
            for item in formatted_data.get('items', []):
                code += "        {\n"
                item_name = str(item.get('name', '')).replace("'", "\\'")
                code += f"            'name': '{item_name}',\n"
                code += f"            'is_total': {bool(item.get('is_total', False))},\n"
                code += "            'values': [\n"

                for value in item.get('values', []):
                    amount = value.get('amount')
                    if amount is not None:
                        amount = float(amount)
                    else:
                        amount = 0

                    formatted_text = str(value.get('formatted', '')).replace("'", "\\'")
                    code += f"                {{'amount': {amount}, 'formatted': '{formatted_text}'}},\n"

                code += "            ]\n"
                code += "        },\n"

            code += "    ],\n"

            # Add key metrics
            code += "    'key_metrics': {\n"
            for metric_key, metric_data in formatted_data.get('key_metrics', {}).items():
                if isinstance(metric_data, dict):
                    metric_name = str(metric_data.get('name', '')).replace("'", "\\'")
                    code += f"        '{metric_key}': {{\n"
                    code += f"            'name': '{metric_name}',\n"
                    code += f"            'values': {metric_data.get('values', [])}\n"
                    code += f"        }},\n"
            code += "    },\n"

            # Add summary
            summary = formatted_data.get('summary', {})
            code += "    'summary': {\n"
            code += f"        'total_items': {summary.get('total_items', 0)},\n"
            code += f"        'total_periods': {summary.get('total_periods', 0)},\n"
            code += f"        'key_metrics_found': {summary.get('key_metrics_found', 0)}\n"
            code += "    }\n"
            code += "}\n\n"

            # Add example usage
            code += "# Example usage:\n"
            code += "# Get operating revenue for first period\n"
            code += "# operating_revenue = income_statement_data['key_metrics']['operating_revenue']['values'][0]['amount']\n\n"

            code += "# Summary Statistics\n"
            code += f"total_items = {summary.get('total_items', 0)}\n"
            code += f"total_periods = {summary.get('total_periods', 0)}\n"
            code += f"key_metrics_count = {summary.get('key_metrics_found', 0)}\n"

            return code

        except Exception as e:
            return f"# Error generating code: {str(e)}"

    def close(self):
        """Close the browser driver"""
        if self.driver:
            try:
                self.driver.quit()
                print("Financial scraper driver closed")
            except Exception as e:
                print(f"Error closing driver: {e}")