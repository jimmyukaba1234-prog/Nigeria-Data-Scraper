import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime
import json
import base64
import os
from urllib.parse import urljoin, urlparse
import numpy as np
import concurrent.futures
import io
import threading
from queue import Queue
import pickle
import socket
import ssl
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import html5lib

# Try to import PDF libraries with error handling
PDF_LIBRARIES_AVAILABLE = False
try:
    # Basic PDF libraries (should work without issues)
    import PyPDF2
    import pdfplumber
    from pdfminer.high_level import extract_text as pdfminer_extract
    
    # Try importing PyMuPDF (fitz) with error handling
    try:
        import fitz  # PyMuPDF
        FITZ_AVAILABLE = True
    except RuntimeError as e:
        if "static/" in str(e):
            # Create the missing directory
            os.makedirs("static", exist_ok=True)
            import fitz
            FITZ_AVAILABLE = True
        else:
            FITZ_AVAILABLE = False
            print(f"PyMuPDF not available: {e}")
    except ImportError:
        FITZ_AVAILABLE = False
    
    # Try importing textract
    try:
        import textract
        TEXTTRACT_AVAILABLE = True
    except ImportError:
        TEXTTRACT_AVAILABLE = False
    
    PDF_LIBRARIES_AVAILABLE = True
    
except ImportError as e:
    print(f"Some PDF libraries not available: {e}")
    PDF_LIBRARIES_AVAILABLE = False

# Google Drive API libraries
GOOGLE_DRIVE_AVAILABLE = False
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
    import mimetypes
    GOOGLE_DRIVE_AVAILABLE = True
except ImportError as e:
    print(f"Google Drive libraries not available: {e}")
    GOOGLE_DRIVE_AVAILABLE = False

# For handling different content types
try:
    from docx import Document
    DOCX_AVAILABLE = True
except ImportError:
    DOCX_AVAILABLE = False

# For handling JavaScript-heavy sites (optional)
SELENIUM_AVAILABLE = False
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    print("Selenium not available. JavaScript-heavy sites will use fallback methods.")

# For API requests
import xml.etree.ElementTree as ET

# Page configuration
st.set_page_config(
    page_title="Nigeria Stats Table Scraper Pro",
    page_icon="📊",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 1rem;
    }
    .success-box {
        padding: 1rem;
        background-color: #D1FAE5;
        border-radius: 0.5rem;
        border-left: 5px solid #10B981;
        margin: 1rem 0;
    }
    .info-box {
        padding: 1rem;
        background-color: #DBEAFE;
        border-radius: 0.5rem;
        border-left: 5px solid #3B82F6;
        margin: 1rem 0;
    }
    .warning-box {
        padding: 1rem;
        background-color: #FEF3C7;
        border-radius: 0.5rem;
        border-left: 5px solid #F59E0B;
        margin: 1rem 0;
    }
    .data-table {
        font-size: 0.85rem;
    }
    .stButton > button {
        width: 100%;
    }
    .source-badge {
        display: inline-block;
        padding: 0.25rem 0.5rem;
        margin: 0.1rem;
        background-color: #e0e0e0;
        border-radius: 0.25rem;
        font-size: 0.75rem;
    }
    .google-drive-btn {
        background: linear-gradient(45deg, #4285F4, #34A853, #FBBC05, #EA4335);
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 5px;
        cursor: pointer;
        font-weight: bold;
    }
    .table-counter {
        background-color: #3B82F6;
        color: white;
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
        font-weight: bold;
    }
    </style>
""", unsafe_allow_html=True)

# Thread-safe logging queue
class ThreadSafeLogger:
    def __init__(self):
        self.log_queue = Queue()
        self.logs = []
    
    def add_log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        self.log_queue.put(log_message)
        self.logs.append(log_message)
        print(log_message)  # Also print to console
    
    def get_logs(self):
        # Get all logs from queue
        logs = []
        while not self.log_queue.empty():
            logs.append(self.log_queue.get())
        return logs
    
    def get_all_logs(self):
        return self.logs

# Initialize thread-safe logger
logger = ThreadSafeLogger()

class GoogleDriveManager:
    """Manage Google Drive integration"""
    
    def __init__(self):
        self.creds = None
        self.service = None
        self.SCOPES = ['https://www.googleapis.com/auth/drive.file']
        self.token_file = 'token.pickle'
        self.credentials_file = 'credentials.json'
    
    def authenticate(self):
        """Authenticate with Google Drive API"""
        try:
            # Check if credentials file exists
            if not os.path.exists(self.credentials_file):
                st.warning(f"⚠️ Please create a `{self.credentials_file}` file with your Google Cloud credentials.")
                st.info("""
                **Steps to get credentials:**
                1. Go to [Google Cloud Console](https://console.cloud.google.com/)
                2. Create a new project or select existing one
                3. Enable Google Drive API
                4. Create OAuth 2.0 credentials (Desktop app)
                5. Download credentials as `credentials.json`
                6. Place in the same directory as this app
                """)
                return False
            
            # Load or get new credentials
            if os.path.exists(self.token_file):
                with open(self.token_file, 'rb') as token:
                    self.creds = pickle.load(token)
            
            # If credentials are invalid or don't exist, get new ones
            if not self.creds or not self.creds.valid:
                if self.creds and self.creds.expired and self.creds.refresh_token:
                    self.creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_file, self.SCOPES)
                    self.creds = flow.run_local_server(port=0)
                
                # Save credentials for next run
                with open(self.token_file, 'wb') as token:
                    pickle.dump(self.creds, token)
            
            # Build the Drive service
            self.service = build('drive', 'v3', credentials=self.creds)
            return True
            
        except Exception as e:
            st.error(f"Google Drive authentication failed: {e}")
            return False
    
    def create_folder(self, folder_name, parent_id=None):
        """Create a folder in Google Drive"""
        try:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_id:
                file_metadata['parents'] = [parent_id]
            
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            
            return folder.get('id')
        except Exception as e:
            st.error(f"Error creating folder: {e}")
            return None
    
    def upload_file(self, file_path, file_name, folder_id=None, mime_type=None):
        """Upload a file to Google Drive"""
        try:
            if not mime_type:
                mime_type, _ = mimetypes.guess_type(file_path)
                if not mime_type:
                    mime_type = 'application/octet-stream'
            
            file_metadata = {'name': file_name}
            if folder_id:
                file_metadata['parents'] = [folder_id]
            
            media = MediaFileUpload(
                file_path,
                mimetype=mime_type,
                resumable=True
            )
            
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink'
            ).execute()
            
            return {
                'file_id': file.get('id'),
                'web_link': file.get('webViewLink'),
                'file_name': file_name
            }
            
        except Exception as e:
            st.error(f"Error uploading file: {e}")
            return None
    
    def upload_dataframe(self, df, file_name, folder_id=None, format='csv'):
        """Upload a DataFrame directly to Google Drive"""
        try:
            # Convert DataFrame to bytes
            if format.lower() == 'csv':
                data = df.to_csv(index=False).encode('utf-8')
                mime_type = 'text/csv'
                file_name = f"{file_name}.csv"
            elif format.lower() == 'json':
                data = df.to_json(orient='records', indent=2).encode('utf-8')
                mime_type = 'application/json'
                file_name = f"{file_name}.json"
            elif format.lower() == 'excel':
                # Save to temporary file
                temp_file = f"temp_{file_name}.xlsx"
                df.to_excel(temp_file, index=False)
                return self.upload_file(temp_file, f"{file_name}.xlsx", folder_id)
            else:
                st.error(f"Unsupported format: {format}")
                return None
            
            file_metadata = {'name': file_name}
            if folder_id:
                file_metadata['parents'] = [folder_id]
            
            media = MediaIoBaseUpload(
                io.BytesIO(data),
                mimetype=mime_type
            )
            
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink'
            ).execute()
            
            return {
                'file_id': file.get('id'),
                'web_link': file.get('webViewLink'),
                'file_name': file_name
            }
            
        except Exception as e:
            st.error(f"Error uploading DataFrame: {e}")
            return None
    
    def list_files(self, folder_id=None):
        """List files in Google Drive"""
        try:
            query = "trashed=false"
            if folder_id:
                query = f"'{folder_id}' in parents and trashed=false"
            
            results = self.service.files().list(
                q=query,
                pageSize=100,
                fields="files(id, name, mimeType, createdTime, modifiedTime, size)"
            ).execute()
            
            return results.get('files', [])
        except Exception as e:
            st.error(f"Error listing files: {e}")
            return []
    
    def get_folder_id_by_name(self, folder_name):
        """Get folder ID by name"""
        try:
            results = self.service.files().list(
                q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="files(id, name)"
            ).execute()
            
            files = results.get('files', [])
            if files:
                return files[0]['id']
            return None
        except Exception as e:
            st.error(f"Error getting folder ID: {e}")
            return None

class RobustRequestSession:
    """Enhanced requests session with better error handling and retries"""
    
    def __init__(self, timeout=30, max_retries=3):
        self.session = requests.Session()
        self.timeout = timeout
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        })
    
    def get(self, url, **kwargs):
        """Enhanced GET request with better error handling"""
        try:
            response = self.session.get(url, timeout=self.timeout, **kwargs)
            return response
            
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Network error for {url}: {str(e)}")
        except requests.exceptions.Timeout as e:
            raise TimeoutError(f"Timeout for {url}: {str(e)}")
        except requests.exceptions.SSLError as e:
            raise ConnectionError(f"SSL error for {url}: {str(e)}")
        except Exception as e:
            raise Exception(f"Request error for {url}: {str(e)}")

class TableScraper:
    """Specialized class for extracting and processing tables from websites"""
    
    def __init__(self, logger=None):
        self.logger = logger
    
    def log(self, message):
        """Thread-safe logging"""
        if self.logger:
            self.logger.add_log(message)
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
    
    def extract_all_tables(self, soup, url, search_query=None):
        """Extract all tables from HTML content with multiple methods"""
        tables_data = []
        tables_found = 0
        
        try:
            # Method 1: Use pandas read_html (most reliable for well-formed tables)
            try:
                df_list = pd.read_html(str(soup), flavor='html5lib')
                for i, df in enumerate(df_list):
                    if not df.empty and len(df) > 1:  # At least header + 1 row
                        table_info = self._process_table_dataframe(df, i, url, "pandas")
                        if self._table_matches_search(table_info, search_query):
                            tables_data.append(table_info)
                            tables_found += 1
                            self.log(f"Found table {i+1} via pandas ({len(df)} rows, {len(df.columns)} columns)")
            except Exception as e:
                self.log(f"Pandas table extraction failed: {str(e)[:80]}...")
            
            # Method 2: Manual extraction for complex tables
            html_tables = soup.find_all('table')
            for table_idx, table in enumerate(html_tables):
                try:
                    # Skip if already found by pandas
                    if any(f"Table_{table_idx}" in t['table_name'] for t in tables_data):
                        continue
                    
                    table_data = self._extract_table_manually(table, table_idx, url)
                    if table_data and self._table_matches_search(table_data, search_query):
                        tables_data.append(table_data)
                        tables_found += 1
                        self.log(f"Found table {table_idx+1} via manual extraction")
                except Exception as e:
                    continue
            
            # Method 3: Look for tabular data in divs with CSS grid/table display
            div_tables = self._extract_div_tables(soup, url, search_query)
            for div_table in div_tables:
                if self._table_matches_search(div_table, search_query):
                    tables_data.append(div_table)
                    tables_found += 1
            
            self.log(f"Total tables found: {tables_found}")
            
        except Exception as e:
            self.log(f"Error extracting tables: {str(e)[:80]}...")
        
        return tables_data
    
    def _process_table_dataframe(self, df, table_index, url, method):
        """Process a pandas DataFrame table into structured format"""
        table_name = f"Table_{table_index+1}"
        
        # Clean the DataFrame
        df = df.copy()
        
        # Remove completely empty rows and columns
        df = df.dropna(how='all')
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        # Create metadata
        metadata = {
            'source_url': url,
            'table_name': table_name,
            'extraction_method': method,
            'rows': len(df),
            'columns': len(df.columns),
            'scrape_date': datetime.now().strftime('%Y-%m-%d'),
            'column_names': list(df.columns)
        }
        
        # Convert first few rows for preview
        preview_data = []
        for idx, row in df.head(5).iterrows():
            preview_data.append(row.to_dict())
        
        return {
            'metadata': metadata,
            'dataframe': df,
            'preview': preview_data,
            'raw_html': None
        }
    
    def _extract_table_manually(self, table, table_index, url):
        """Manually extract table data from HTML table element"""
        try:
            rows = table.find_all('tr')
            if len(rows) < 2:  # Need at least header + data row
                return None
            
            data = []
            headers = []
            
            # Extract headers from first row
            header_row = rows[0]
            header_cells = header_row.find_all(['th', 'td'])
            headers = [cell.get_text(strip=True) or f"Column_{i+1}" 
                      for i, cell in enumerate(header_cells)]
            
            # Extract data from remaining rows
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) == len(headers) or len(cells) > 0:
                    row_data = {}
                    for i, cell in enumerate(cells[:len(headers)]):
                        col_name = headers[i] if i < len(headers) else f"Column_{i+1}"
                        row_data[col_name] = cell.get_text(strip=True)
                    data.append(row_data)
            
            if not data:
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Create metadata
            metadata = {
                'source_url': url,
                'table_name': f"Table_{table_index+1}_manual",
                'extraction_method': 'manual',
                'rows': len(df),
                'columns': len(df.columns),
                'scrape_date': datetime.now().strftime('%Y-%m-%d'),
                'column_names': list(df.columns)
            }
            
            # Preview data
            preview_data = []
            for idx, row in df.head(5).iterrows():
                preview_data.append(row.to_dict())
            
            return {
                'metadata': metadata,
                'dataframe': df,
                'preview': preview_data,
                'raw_html': str(table)[:1000]  # Store first 1000 chars of HTML
            }
            
        except Exception as e:
            return None
    
    def _extract_div_tables(self, soup, url, search_query):
        """Extract tabular data from divs with table-like structure"""
        div_tables = []
        
        # Look for divs with table-like classes
        table_patterns = [
            {'class': re.compile(r'table')},
            {'class': re.compile(r'data-table')},
            {'class': re.compile(r'stats-table')},
            {'class': re.compile(r'results')},
            {'id': re.compile(r'table')},
            {'role': 'table'}
        ]
        
        for pattern in table_patterns:
            divs = soup.find_all('div', pattern)
            for div_idx, div in enumerate(divs[:5]):  # Limit to 5 divs per pattern
                try:
                    # Look for rows and cells
                    rows = div.find_all(['div', 'tr'], class_=re.compile(r'row|tr'))
                    if len(rows) > 1:
                        data = []
                        headers = []
                        
                        # Try to extract headers
                        first_row = rows[0]
                        cells = first_row.find_all(['div', 'td', 'th'])
                        headers = [cell.get_text(strip=True) or f"Column_{i+1}" 
                                  for i, cell in enumerate(cells)]
                        
                        # Extract data
                        for row in rows[1:]:
                            cells = row.find_all(['div', 'td'])
                            if cells:
                                row_data = {}
                                for i, cell in enumerate(cells[:len(headers)]):
                                    col_name = headers[i] if i < len(headers) else f"Column_{i+1}"
                                    row_data[col_name] = cell.get_text(strip=True)
                                data.append(row_data)
                        
                        if data:
                            df = pd.DataFrame(data)
                            metadata = {
                                'source_url': url,
                                'table_name': f"DivTable_{div_idx+1}",
                                'extraction_method': 'div_extraction',
                                'rows': len(df),
                                'columns': len(df.columns),
                                'scrape_date': datetime.now().strftime('%Y-%m-%d'),
                                'column_names': list(df.columns)
                            }
                            
                            preview_data = []
                            for idx, row in df.head(3).iterrows():
                                preview_data.append(row.to_dict())
                            
                            table_data = {
                                'metadata': metadata,
                                'dataframe': df,
                                'preview': preview_data,
                                'raw_html': str(div)[:500]
                            }
                            
                            if self._table_matches_search(table_data, search_query):
                                div_tables.append(table_data)
                except:
                    continue
        
        return div_tables
    
    def _table_matches_search(self, table_data, search_query):
        """Check if table content matches search query"""
        if not search_query:
            return True
        
        search_terms = search_query.lower().split()
        
        # Check metadata
        metadata_str = str(table_data['metadata']).lower()
        if any(term in metadata_str for term in search_terms):
            return True
        
        # Check column names
        columns_str = ' '.join(table_data['metadata']['column_names']).lower()
        if any(term in columns_str for term in search_terms):
            return True
        
        # Check first few rows of data
        preview_str = str(table_data['preview']).lower()
        if any(term in preview_str for term in search_terms):
            return True
        
        return False
    
    def save_table_to_file(self, table_data, base_filename, folder="tables"):
        """Save individual table to CSV and Excel files"""
        try:
            df = table_data['dataframe']
            metadata = table_data['metadata']
            
            # Create folder if it doesn't exist
            os.makedirs(folder, exist_ok=True)
            
            # Sanitize table name for filename
            table_name = metadata['table_name'].replace(' ', '_').replace('/', '_')
            filename = f"{base_filename}_{table_name}"
            
            # Save as CSV
            csv_path = os.path.join(folder, f"{filename}.csv")
            df.to_csv(csv_path, index=False)
            
            # Save as Excel
            excel_path = os.path.join(folder, f"{filename}.xlsx")
            df.to_excel(excel_path, index=False)
            
            # Save metadata
            meta_path = os.path.join(folder, f"{filename}_metadata.json")
            with open(meta_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            return {
                'csv_path': csv_path,
                'excel_path': excel_path,
                'metadata_path': meta_path,
                'table_name': table_name,
                'rows': len(df),
                'columns': len(df.columns)
            }
            
        except Exception as e:
            self.log(f"Error saving table: {str(e)[:80]}...")
            return None
    
    def create_table_summary(self, all_tables):
        """Create a summary DataFrame of all extracted tables"""
        summary_data = []
        
        for table in all_tables:
            metadata = table['metadata']
            df = table['dataframe']
            
            summary_data.append({
                'Table_Name': metadata['table_name'],
                'Source_URL': metadata['source_url'],
                'Extraction_Method': metadata['extraction_method'],
                'Rows': metadata['rows'],
                'Columns': metadata['columns'],
                'Column_Names': ', '.join(metadata['column_names'][:3]) + ('...' if len(metadata['column_names']) > 3 else ''),
                'Sample_Data': str(df.iloc[0].to_dict())[:100] + '...' if len(df) > 0 else 'No data'
            })
        
        return pd.DataFrame(summary_data)

class NigerianStatsScraper:
    """Enhanced web scraper for Nigerian statistical data with table extraction"""
    
    def __init__(self, max_workers=5, use_selenium=False, logger=None):
        self.request_session = RobustRequestSession(timeout=30, max_retries=2)
        self.max_workers = max_workers
        self.use_selenium = use_selenium and SELENIUM_AVAILABLE
        self.driver = None
        self.logger = logger
        self.table_scraper = TableScraper(logger=logger)
        
        # Store extracted tables separately
        self.extracted_tables = []
    
    def log(self, message):
        """Thread-safe logging"""
        if self.logger:
            self.logger.add_log(message)
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
    
    def init_selenium(self):
        """Initialize Selenium WebDriver for JavaScript-heavy sites"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            
            self.driver = webdriver.Chrome(
                options=chrome_options
            )
            self.driver.set_page_load_timeout(30)
            self.log("Selenium WebDriver initialized successfully")
        except Exception as e:
            self.log(f"Failed to initialize Selenium: {e}")
            self.use_selenium = False
    
    def close_selenium(self):
        """Close Selenium WebDriver"""
        if self.driver:
            self.driver.quit()
            self.log("Selenium WebDriver closed")
    
    def test_website_accessibility(self, url):
        """Test if a website is accessible before scraping"""
        try:
            # Quick HEAD request to check accessibility
            response = self.request_session.session.head(url, timeout=5)
            return response.status_code < 400
        except:
            return False
    
    def scrape_website(self, website_config, search_query):
        """Scrape a single website based on configuration"""
        website_data = []
        website_tables = []
        
        try:
            url = website_config.get('url', '')
            name = website_config.get('name', 'Unknown')
            scrape_method = website_config.get('scrape_method', 'direct')
            
            # Test website accessibility first
            if not self.test_website_accessibility(url):
                self.log(f"⚠️ Skipping {name} - Website not accessible")
                return website_data, website_tables
            
            self.log(f"Scraping {name}: {url}")
            
            if scrape_method == 'selenium' and self.use_selenium:
                html_content = self.scrape_with_selenium(url)
            else:
                html_content = self.scrape_with_requests(url)
            
            if html_content:
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Extract general data
                website_data = self.extract_general_data(soup, url, search_query)
                
                # Extract tables (main focus)
                website_tables = self.table_scraper.extract_all_tables(soup, url, search_query)
                
                if website_tables:
                    self.log(f"✓ Found {len(website_tables)} tables from {name}")
                
                if website_data:
                    self.log(f"✓ Found {len(website_data)} text records from {name}")
                else:
                    self.log(f"⚠️ No text data found from {name}")
            
        except Exception as e:
            error_msg = str(e)
            if "ConnectionError" in error_msg or "Timeout" in error_msg:
                self.log(f"✗ Network error for {name}: {error_msg[:80]}...")
            else:
                self.log(f"✗ Error scraping {name}: {error_msg[:80]}...")
        
        return website_data, website_tables
    
    def scrape_with_requests(self, url):
        """Scrape website using requests library"""
        try:
            response = self.request_session.get(url)
            if response.status_code == 200:
                return response.content
        except Exception as e:
            self.log(f"✗ Error scraping {url}: {str(e)[:80]}...")
        return None
    
    def scrape_with_selenium(self, url):
        """Scrape JavaScript-heavy websites using Selenium"""
        if not self.driver:
            return self.scrape_with_requests(url)  # Fallback
        
        try:
            self.driver.get(url)
            time.sleep(2)  # Wait for JavaScript to load
            
            # Get page source after JavaScript execution
            return self.driver.page_source.encode('utf-8')
            
        except Exception as e:
            self.log(f"✗ Selenium error for {url}: {str(e)[:80]}...")
            # Fallback to requests
            return self.scrape_with_requests(url)
    
    def extract_general_data(self, soup, url, search_query=None):
        """Extract general data from HTML content"""
        data = []
        
        # Extract all text and look for statistical patterns
        all_text = soup.get_text()
        
        # Look for Nigerian statistical data patterns
        nigeria_patterns = [
            # Economic indicators
            (r'GDP.*?(?:growth|rate|size).*?\d+\.?\d*', 'Economic'),
            (r'inflation.*?(?:rate|%).*?\d+\.?\d*', 'Economic'),
            (r'unemployment.*?(?:rate|%).*?\d+\.?\d*', 'Labor'),
            
            # Population data
            (r'population.*?(?:of|in).*?\d+[\d,]*(?:\s*million|\s*billion)?', 'Demographic'),
            (r'census.*?\d{4}.*?\d+[\d,]*', 'Demographic'),
            
            # Health indicators
            (r'mortality.*?(?:rate|ratio).*?\d+\.?\d*', 'Health'),
            (r'life.*?expectancy.*?\d+\.?\d*', 'Health'),
            
            # Education
            (r'literacy.*?(?:rate|%).*?\d+\.?\d*', 'Education'),
            (r'enrollment.*?(?:rate|%).*?\d+\.?\d*', 'Education'),
            
            # General statistics
            (r'\d+\.?\d*\s*%', 'General'),
            (r'\d{1,3}(?:,\d{3})+', 'General'),
            (r'\d+\s*(?:million|billion|thousand)', 'General')
        ]
        
        for pattern, category in nigeria_patterns:
            matches = re.findall(pattern, all_text, re.IGNORECASE)
            for match in matches[:3]:  # Limit to 3 matches per pattern
                data.append({
                    'Statistical_Match': match,
                    'Category': category,
                    'Source_URL': url,
                    'Pattern_Type': pattern,
                    'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                })
        
        # Filter by search query if provided
        if search_query and data:
            filtered_data = []
            search_terms = search_query.lower().split()
            for item in data:
                item_str = str(item).lower()
                if any(term in item_str for term in search_terms):
                    filtered_data.append(item)
            data = filtered_data
        
        return data
    
    def get_working_websites(self):
        """Get list of working websites with table data"""
        websites = [
            # Websites known to have good tables
            {
                'name': 'World Bank Nigeria Indicators',
                'url': 'https://data.worldbank.org/indicator?locations=NG',
                'scrape_method': 'direct',
                'category': 'International Statistics',
                'priority': 1,
                'has_tables': True
            },
            {
                'name': 'UN Data Nigeria',
                'url': 'https://data.un.org/en/iso/ng.html',
                'scrape_method': 'direct',
                'category': 'International Statistics',
                'priority': 1,
                'has_tables': True
            },
            {
                'name': 'Trading Economics Nigeria',
                'url': 'https://tradingeconomics.com/nigeria/indicators',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 1,
                'has_tables': True
            },
            {
                'name': 'Worldometer Nigeria',
                'url': 'https://www.worldometers.info/world-population/nigeria-population/',
                'scrape_method': 'direct',
                'category': 'Demographic Statistics',
                'priority': 2,
                'has_tables': True
            },
            {
                'name': 'Knoema Nigeria Atlas',
                'url': 'https://knoema.com/atlas/Nigeria',
                'scrape_method': 'direct',
                'category': 'General Statistics',
                'priority': 2,
                'has_tables': True
            },
            {
                'name': 'Index Mundi Nigeria',
                'url': 'https://www.indexmundi.com/nigeria/',
                'scrape_method': 'direct',
                'category': 'General Statistics',
                'priority': 2,
                'has_tables': True
            },
            {
                'name': 'Macrotrends Nigeria',
                'url': 'https://www.macrotrends.net/countries/NGA/nigeria/',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 2,
                'has_tables': True
            },
            {
                'name': 'Statista Nigeria',
                'url': 'https://www.statista.com/topics/6616/nigeria/',
                'scrape_method': 'direct',
                'category': 'General Statistics',
                'priority': 2,
                'has_tables': True
            }
        ]
        
        # Test each website
        working_websites = []
        for website in websites:
            try:
                if self.test_website_accessibility(website['url']):
                    working_websites.append(website)
                    self.log(f"✓ {website['name']} is accessible")
                else:
                    self.log(f"⚠️ Skipping {website['name']} - Not accessible")
            except:
                self.log(f"⚠️ Skipping {website['name']} - Connection test failed")
        
        return working_websites
    
    def smart_scrape_tables(self, search_query, selected_categories=None, max_websites=8):
        """Scrape tables from multiple websites"""
        all_data = []
        all_tables = []
        
        # Get working websites
        websites = self.get_working_websites()
        
        if not websites:
            self.log("⚠️ No accessible websites found")
            return pd.DataFrame(), []
        
        # Filter by categories if specified
        if selected_categories:
            websites = [w for w in websites if w.get('category') in selected_categories]
        
        # Sort by priority and limit number
        websites.sort(key=lambda x: x.get('priority', 99))
        websites_to_scrape = websites[:max_websites]
        
        self.log(f"Starting table scraping: {len(websites_to_scrape)} websites")
        
        # Use ThreadPoolExecutor for concurrent scraping
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(self.max_workers, len(websites_to_scrape))) as executor:
            # Submit scraping tasks
            future_to_website = {
                executor.submit(self.scrape_website, website, search_query): website 
                for website in websites_to_scrape
            }
            
            # Collect results as they complete
            completed = 0
            total = len(websites_to_scrape)
            
            for future in concurrent.futures.as_completed(future_to_website):
                website = future_to_website[future]
                completed += 1
                
                try:
                    website_data, website_tables = future.result()
                    if website_data:
                        all_data.extend(website_data)
                    if website_tables:
                        all_tables.extend(website_tables)
                        self.log(f"({completed}/{total}) ✓ {website['name']}: {len(website_tables)} tables, {len(website_data)} text records")
                    else:
                        self.log(f"({completed}/{total}) ⚠️ {website['name']}: No tables found")
                except Exception as e:
                    self.log(f"({completed}/{total}) ✗ Error scraping {website['name']}: {str(e)[:80]}...")
        
        self.log(f"Table scraping complete. Found {len(all_tables)} tables and {len(all_data)} text records")
        
        # Create combined data
        if all_data:
            df = pd.DataFrame(all_data)
            df = df.drop_duplicates()
        else:
            df = pd.DataFrame()
        
        return df, all_tables
    
    def save_all_tables(self, all_tables, base_filename="nigeria_tables"):
        """Save all extracted tables to files"""
        saved_files = []
        
        if not all_tables:
            return saved_files
        
        # Create main tables directory
        tables_dir = "extracted_tables"
        os.makedirs(tables_dir, exist_ok=True)
        
        # Create timestamp subdirectory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_dir = os.path.join(tables_dir, timestamp)
        os.makedirs(save_dir, exist_ok=True)
        
        # Save each table
        for i, table in enumerate(all_tables):
            try:
                file_info = self.table_scraper.save_table_to_file(
                    table, 
                    f"{base_filename}_{i+1:03d}",
                    folder=save_dir
                )
                
                if file_info:
                    saved_files.append({
                        'index': i + 1,
                        'table_name': table['metadata']['table_name'],
                        'source_url': table['metadata']['source_url'],
                        'rows': file_info['rows'],
                        'columns': file_info['columns'],
                        'csv_path': file_info['csv_path'],
                        'excel_path': file_info['excel_path'],
                        'metadata_path': file_info['metadata_path']
                    })
                    self.log(f"Saved table {i+1}: {file_info['table_name']} ({file_info['rows']}x{file_info['columns']})")
            except Exception as e:
                self.log(f"Error saving table {i+1}: {str(e)[:80]}...")
        
        # Create summary file
        if saved_files:
            summary_df = pd.DataFrame([
                {
                    'Table_Number': f['index'],
                    'Table_Name': f['table_name'],
                    'Source_URL': f['source_url'],
                    'Rows': f['rows'],
                    'Columns': f['columns'],
                    'CSV_File': os.path.basename(f['csv_path']),
                    'Excel_File': os.path.basename(f['excel_path'])
                }
                for f in saved_files
            ])
            
            summary_csv = os.path.join(save_dir, "tables_summary.csv")
            summary_excel = os.path.join(save_dir, "tables_summary.xlsx")
            
            summary_df.to_csv(summary_csv, index=False)
            summary_df.to_excel(summary_excel, index=False)
            
            saved_files.append({
                'type': 'summary',
                'csv_path': summary_csv,
                'excel_path': summary_excel
            })
        
        return saved_files, save_dir

def create_download_link(df, filename, file_type="csv"):
    """Create download link for DataFrame"""
    if file_type == "csv":
        csv = df.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}" download="{filename}" target="_blank">📥 Download {filename}</a>'
        return href
    elif file_type == "json":
        json_str = df.to_json(orient='records', indent=2)
        b64 = base64.b64encode(json_str.encode()).decode()
        href = f'<a href="data:application/json;base64,{b64}" download="{filename}" target="_blank">📥 Download {filename}</a>'
        return href

def create_zip_download_link(folder_path, zip_name):
    """Create download link for a zip file"""
    import zipfile
    
    # Create zip file
    zip_path = f"{zip_name}.zip"
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, os.path.dirname(folder_path))
                zipf.write(file_path, arcname)
    
    # Create download link
    with open(zip_path, 'rb') as f:
        bytes = f.read()
        b64 = base64.b64encode(bytes).decode()
        href = f'<a href="data:application/zip;base64,{b64}" download="{zip_name}.zip" target="_blank">📦 Download All Tables ({zip_name}.zip)</a>'
    
    return href

def main():
    """Main application function"""
    
    # Initialize session state
    if 'scraped_data' not in st.session_state:
        st.session_state.scraped_data = None
    if 'extracted_tables' not in st.session_state:
        st.session_state.extracted_tables = []
    if 'table_files' not in st.session_state:
        st.session_state.table_files = []
    if 'scraping_in_progress' not in st.session_state:
        st.session_state.scraping_in_progress = False
    if 'scraping_log' not in st.session_state:
        st.session_state.scraping_log = []
    if 'google_drive_auth' not in st.session_state:
        st.session_state.google_drive_auth = None
    
    # Header
    st.markdown('<h1 class="main-header">📊 Nigeria Statistics Table Scraper Pro</h1>', unsafe_allow_html=True)
    
    # System info
    with st.expander("ℹ️ System Information", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.write(f"**PDF Libraries:** {'✅ Available' if PDF_LIBRARIES_AVAILABLE else '⚠️ Limited'}")
        with col2:
            st.write(f"**Selenium:** {'✅ Available' if SELENIUM_AVAILABLE else '⚠️ Not available'}")
        with col3:
            st.write(f"**Google Drive:** {'✅ Available' if GOOGLE_DRIVE_AVAILABLE else '⚠️ Not available'}")
        with col4:
            st.write(f"**Streamlit:** Version {st.__version__}")
    
    st.markdown("""
        <div style="text-align: center; margin-bottom: 2rem;">
            <p><strong>Specialized scraper for extracting tables from Nigerian statistical websites</strong></p>
            <p>Automatically finds, extracts, and saves tables as CSV/Excel files</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Data sources selection
        st.subheader("📡 Table Sources")
        
        categories = [
            "International Statistics",
            "Economic Statistics",
            "Demographic Statistics",
            "Health Statistics",
            "Education Statistics",
            "General Statistics"
        ]
        
        selected_categories = st.multiselect(
            "Select categories to search:",
            categories,
            default=["International Statistics", "Economic Statistics", "Demographic Statistics"]
        )
        
        # Table extraction options
        st.subheader("🔧 Table Extraction")
        
        max_websites = st.slider("Maximum websites to scrape", 1, 10, 5)
        max_workers = st.slider("Concurrent scrapers", 1, 5, 2)
        
        extraction_methods = st.multiselect(
            "Extraction methods:",
            ["Pandas (recommended)", "Manual HTML", "Div-based tables"],
            default=["Pandas (recommended)", "Manual HTML"]
        )
        
        min_table_rows = st.slider("Minimum table rows", 2, 100, 5)
        min_table_cols = st.slider("Minimum table columns", 2, 20, 3)
        
        # Google Drive Configuration
        st.subheader("☁️ Google Drive Options")
        
        if GOOGLE_DRIVE_AVAILABLE:
            auto_save_drive = st.checkbox("Auto-save tables to Google Drive", value=False)
            
            if st.button("🔗 Connect to Google Drive", use_container_width=True):
                with st.spinner("Connecting to Google Drive..."):
                    drive_manager = GoogleDriveManager()
                    if drive_manager.authenticate():
                        st.session_state.google_drive_auth = drive_manager
                        st.success("✅ Connected to Google Drive!")
                    else:
                        st.error("❌ Failed to connect to Google Drive")
            
            if st.session_state.google_drive_auth:
                st.success("✅ Google Drive: Connected")
        else:
            st.warning("Google Drive API not available.")
        
        # Export options
        st.subheader("💾 Export Options")
        
        export_format = st.selectbox(
            "Main data format:",
            ["CSV", "JSON", "Excel"]
        )
        
        table_formats = st.multiselect(
            "Table file formats:",
            ["CSV", "Excel", "JSON"],
            default=["CSV", "Excel"]
        )
    
    # Main content
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.subheader("🔍 What tables are you looking for?")
        
        # Search input
        search_query = st.text_input(
            "Enter table search query:",
            placeholder="e.g., 'GDP data', 'population table', 'unemployment statistics'",
            key="search_input"
        )
        
        # Quick search buttons for common tables
        st.subheader("🚀 Common Table Searches")
        
        quick_searches = [
            "GDP Growth Tables",
            "Population Data Tables",
            "Unemployment Statistics",
            "Inflation Rates Table",
            "Trade Statistics",
            "Health Indicators"
        ]
        
        cols = st.columns(3)
        for i, query in enumerate(quick_searches):
            with cols[i % 3]:
                if st.button(f"📊 {query}", use_container_width=True, key=f"quick_{i}"):
                    st.session_state.search_query = query
                    st.rerun()
    
    with col2:
        st.subheader("🎯 Table Sources")
        st.write("**Websites with tables:**")
        st.write("• World Bank Nigeria")
        st.write("• UN Data Nigeria")
        st.write("• Trading Economics")
        st.write("• Worldometer")
        st.write("• Knoema Atlas")
        st.write("• Index Mundi")
        st.write("• Macrotrends")
        
        if st.button("🧪 Test Table Sources", key="test_sources"):
            with st.spinner("Testing table sources..."):
                scraper = NigerianStatsScraper(logger=logger)
                websites = scraper.get_working_websites()
                st.info(f"✅ {len(websites)} table sources available")
                for website in websites[:5]:
                    st.write(f"• {website['name']}")
    
    # Scrape button
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.button("🚀 Extract Tables from Websites", type="primary", use_container_width=True, key="scrape_button"):
            if not search_query and 'search_query' not in st.session_state:
                st.warning("⚠️ Please enter a search query or select a quick search")
            else:
                query = search_query or getattr(st.session_state, 'search_query', '')
                
                st.session_state.scraping_in_progress = True
                st.session_state.scraping_log = []  # Clear previous log
                st.session_state.extracted_tables = []
                st.session_state.table_files = []
                
                with st.spinner(f"🔍 Searching for tables about '{query}'..."):
                    # Initialize scraper
                    scraper = NigerianStatsScraper(
                        max_workers=max_workers,
                        use_selenium=False,  # Disable selenium for reliability
                        logger=logger
                    )
                    
                    # Show scraping progress
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # Start scraping
                    status_text.text("Testing website accessibility...")
                    progress_bar.progress(20)
                    
                    # Perform table scraping
                    status_text.text("Extracting tables from websites...")
                    progress_bar.progress(50)
                    
                    scraped_data, extracted_tables = scraper.smart_scrape_tables(
                        query, 
                        selected_categories, 
                        max_websites
                    )
                    
                    # Save tables to files
                    status_text.text("Saving tables to files...")
                    progress_bar.progress(70)
                    
                    if extracted_tables:
                        table_files, save_dir = scraper.save_all_tables(extracted_tables, f"nigeria_tables_{query.replace(' ', '_')}")
                        st.session_state.table_files = table_files
                        st.session_state.save_dir = save_dir
                    
                    # Update progress
                    progress_bar.progress(90)
                    status_text.text("Processing results...")
                    
                    # Close Selenium if used
                    scraper.close_selenium()
                    
                    if scraped_data is not None and not scraped_data.empty:
                        st.session_state.scraped_data = scraped_data
                        st.session_state.extracted_tables = extracted_tables
                        
                        status_text.text("✅ Table extraction completed!")
                        progress_bar.progress(100)
                        
                        # Show summary
                        if extracted_tables:
                            st.success(f"🎉 Successfully extracted {len(extracted_tables)} tables and {len(scraped_data)} text records!")
                            
                            # Create table summary
                            table_scraper = TableScraper()
                            summary_df = table_scraper.create_table_summary(extracted_tables)
                            st.session_state.table_summary = summary_df
                            
                            # Show table statistics
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Tables", len(extracted_tables))
                            with col2:
                                total_rows = sum(len(t['dataframe']) for t in extracted_tables)
                                st.metric("Total Rows", total_rows)
                            with col3:
                                websites_used = len(set(t['metadata']['source_url'] for t in extracted_tables))
                                st.metric("Sources", websites_used)
                            with col4:
                                st.metric("Files Created", len([f for f in st.session_state.table_files if f.get('type') != 'summary']))
                        else:
                            st.warning("⚠️ No tables found, but some text data was extracted")
                            st.info("💡 Try different search terms or select different categories")
                        
                        # Save to Google Drive if enabled
                        if GOOGLE_DRIVE_AVAILABLE and st.session_state.get('google_drive_auth') and extracted_tables:
                            with st.spinner("📤 Uploading tables to Google Drive..."):
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                folder_name = f"Nigeria_Tables_{query.replace(' ', '_')}_{timestamp}"
                                
                                # Create folder in Google Drive
                                folder_id = st.session_state.google_drive_auth.create_folder(folder_name)
                                
                                if folder_id:
                                    uploaded_files = []
                                    for table_file in st.session_state.table_files:
                                        if table_file.get('type') != 'summary':
                                            # Upload CSV
                                            if 'csv_path' in table_file and os.path.exists(table_file['csv_path']):
                                                result = st.session_state.google_drive_auth.upload_file(
                                                    table_file['csv_path'],
                                                    os.path.basename(table_file['csv_path']),
                                                    folder_id
                                                )
                                                if result:
                                                    uploaded_files.append(result['file_name'])
                                            
                                            # Upload Excel
                                            if 'excel_path' in table_file and os.path.exists(table_file['excel_path']):
                                                result = st.session_state.google_drive_auth.upload_file(
                                                    table_file['excel_path'],
                                                    os.path.basename(table_file['excel_path']),
                                                    folder_id
                                                )
                                    
                                    if uploaded_files:
                                        st.success(f"✅ {len(uploaded_files)} tables uploaded to Google Drive!")
                                        st.info(f"📁 **Folder:** {folder_name}")
                    
                    else:
                        st.error("❌ No data found. Please check your internet connection and try again.")
                    
                    st.session_state.scraping_in_progress = False
    
    with col2:
        if st.button("🔄 Clear Results", use_container_width=True, key="clear_button"):
            st.session_state.scraped_data = None
            st.session_state.extracted_tables = []
            st.session_state.table_files = []
            st.session_state.scraping_log = []
            st.success("Results cleared!")
            st.rerun()
    
    # Update session state with logs from thread-safe logger
    logs_from_threads = logger.get_logs()
    if logs_from_threads:
        st.session_state.scraping_log.extend(logs_from_threads)
    
    # Display scraping log
    if st.session_state.scraping_log:
        with st.expander("📋 Scraping Log", expanded=True):
            log_container = st.container()
            with log_container:
                for log_entry_text in st.session_state.scraping_log[-30:]:
                    if "✓" in log_entry_text or "Found" in log_entry_text:
                        st.success(log_entry_text)
                    elif "✗" in log_entry_text or "Error" in log_entry_text:
                        st.error(log_entry_text)
                    elif "⚠️" in log_entry_text or "Warning" in log_entry_text:
                        st.warning(log_entry_text)
                    else:
                        st.info(log_entry_text)
    
    # Display extracted tables and data
    if st.session_state.extracted_tables or st.session_state.scraped_data is not None:
        st.header("📊 Extracted Tables & Data")
        
        # Create tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Table Summary", "🔍 View Tables", "📈 Text Data", "💾 Download"])
        
        with tab1:
            if hasattr(st.session_state, 'table_summary') and not st.session_state.table_summary.empty:
                st.subheader("📊 Table Extraction Summary")
                st.dataframe(st.session_state.table_summary, use_container_width=True, height=400)
                
                # Show table statistics
                st.subheader("📈 Table Statistics")
                col1, col2, col3 = st.columns(3)
                with col1:
                    avg_rows = st.session_state.table_summary['Rows'].mean()
                    st.metric("Average Rows per Table", f"{avg_rows:.1f}")
                with col2:
                    avg_cols = st.session_state.table_summary['Columns'].mean()
                    st.metric("Average Columns per Table", f"{avg_cols:.1f}")
                with col3:
                    unique_sources = st.session_state.table_summary['Source_URL'].nunique()
                    st.metric("Unique Sources", unique_sources)
            else:
                st.info("No table summary available")
        
        with tab2:
            if st.session_state.extracted_tables:
                st.subheader("🔍 Browse Extracted Tables")
                
                # Table selector
                table_options = [f"Table {i+1}: {t['metadata']['table_name']} ({len(t['dataframe'])} rows, {len(t['dataframe'].columns)} columns)" 
                               for i, t in enumerate(st.session_state.extracted_tables)]
                
                selected_table_idx = st.selectbox(
                    "Select a table to view:",
                    range(len(table_options)),
                    format_func=lambda x: table_options[x]
                )
                
                if selected_table_idx is not None:
                    selected_table = st.session_state.extracted_tables[selected_table_idx]
                    
                    # Show table metadata
                    metadata = selected_table['metadata']
                    st.write(f"**Source:** {metadata['source_url']}")
                    st.write(f"**Extraction Method:** {metadata['extraction_method']}")
                    st.write(f"**Dimensions:** {metadata['rows']} rows × {metadata['columns']} columns")
                    
                    # Show the table
                    st.dataframe(selected_table['dataframe'], use_container_width=True, height=400)
                    
                    # Show column information
                    with st.expander("📊 Column Information"):
                        col_info = []
                        for col in selected_table['dataframe'].columns:
                            col_info.append({
                                'Column': col,
                                'Type': str(selected_table['dataframe'][col].dtype),
                                'Non-Null': selected_table['dataframe'][col].count(),
                                'Unique': selected_table['dataframe'][col].nunique()
                            })
                        st.table(pd.DataFrame(col_info))
            else:
                st.info("No tables extracted")
        
        with tab3:
            if st.session_state.scraped_data is not None and not st.session_state.scraped_data.empty:
                st.subheader("📈 Extracted Text Data")
                st.dataframe(st.session_state.scraped_data, use_container_width=True, height=400)
                
                # Show statistics
                col1, col2 = st.columns(2)
                with col1:
                    categories = st.session_state.scraped_data['Category'].nunique() if 'Category' in st.session_state.scraped_data.columns else 0
                    st.metric("Data Categories", categories)
                with col2:
                    sources = st.session_state.scraped_data['Source_URL'].nunique() if 'Source_URL' in st.session_state.scraped_data.columns else 0
                    st.metric("Data Sources", sources)
            else:
                st.info("No text data extracted")
        
        with tab4:
            st.subheader("💾 Download Options")
            
            # Main data download
            if st.session_state.scraped_data is not None and not st.session_state.scraped_data.empty:
                st.write("**Main Data Export:**")
                export_filename = st.text_input(
                    "Filename for main data:",
                    value=f"nigeria_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    key="main_export_filename"
                )
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("📥 Download CSV", use_container_width=True, key="dl_csv_main"):
                        csv_link = create_download_link(st.session_state.scraped_data, f"{export_filename}.csv", "csv")
                        st.markdown(csv_link, unsafe_allow_html=True)
                with col2:
                    if st.button("📥 Download JSON", use_container_width=True, key="dl_json_main"):
                        json_link = create_download_link(st.session_state.scraped_data, f"{export_filename}.json", "json")
                        st.markdown(json_link, unsafe_allow_html=True)
                with col3:
                    if st.button("📥 Download Excel", use_container_width=True, key="dl_excel_main"):
                        os.makedirs("exports", exist_ok=True)
                        excel_path = f"exports/{export_filename}.xlsx"
                        st.session_state.scraped_data.to_excel(excel_path, index=False)
                        with open(excel_path, "rb") as f:
                            excel_bytes = f.read()
                        st.download_button(
                            label="Download Excel",
                            data=excel_bytes,
                            file_name=f"{export_filename}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
            
            # Table files download
            if st.session_state.table_files:
                st.write("**Table Files Export:**")
                
                # Create zip of all tables
                if hasattr(st.session_state, 'save_dir'):
                    zip_name = f"nigeria_tables_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    zip_link = create_zip_download_link(st.session_state.save_dir, zip_name)
                    st.markdown(zip_link, unsafe_allow_html=True)
                
                # Show individual table files
                st.write("**Individual Table Files:**")
                table_files = [f for f in st.session_state.table_files if f.get('type') != 'summary']
                
                for table_file in table_files:
                    with st.expander(f"Table {table_file['index']}: {table_file['table_name']}"):
                        col1, col2 = st.columns(2)
                        with col1:
                            if 'csv_path' in table_file and os.path.exists(table_file['csv_path']):
                                with open(table_file['csv_path'], 'rb') as f:
                                    csv_bytes = f.read()
                                st.download_button(
                                    label=f"📥 Download CSV ({table_file['rows']}x{table_file['columns']})",
                                    data=csv_bytes,
                                    file_name=os.path.basename(table_file['csv_path']),
                                    mime="text/csv",
                                    use_container_width=True
                                )
                        with col2:
                            if 'excel_path' in table_file and os.path.exists(table_file['excel_path']):
                                with open(table_file['excel_path'], 'rb') as f:
                                    excel_bytes = f.read()
                                st.download_button(
                                    label=f"📥 Download Excel ({table_file['rows']}x{table_file['columns']})",
                                    data=excel_bytes,
                                    file_name=os.path.basename(table_file['excel_path']),
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    use_container_width=True
                                )

# Footer
st.markdown("---")
st.markdown("""
    <div style="text-align: center; color: #666; padding: 2rem 0;">
        <p><strong>Nigeria Statistics Table Scraper Pro</strong> • Version 4.0</p>
        <p>📊 <strong>Specialized table extraction</strong> from statistical websites</p>
        <p>💾 <strong>Automatic file export:</strong> CSV and Excel formats for all tables</p>
        <p>🔍 <strong>Smart table detection:</strong> Multiple extraction methods for maximum coverage</p>
        <p>☁️ <strong>Google Drive integration</strong> for cloud storage</p>
    </div>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    # Create necessary directories
    os.makedirs("static", exist_ok=True)
    os.makedirs("extracted_tables", exist_ok=True)
    os.makedirs("exports", exist_ok=True)
    main()