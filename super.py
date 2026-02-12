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
    page_title="Nigeria Stats Web Scraper Pro",
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
            # Check if URL is accessible before making request
            if not self._is_url_accessible(url):
                raise ConnectionError(f"Cannot access {url}")
            
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
    
    def _is_url_accessible(self, url):
        """Check if URL is accessible without downloading content"""
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname
            
            # Skip accessibility check for certain problematic domains
            problematic_domains = ['covid19.ncdc.gov.ng', 'npf.gov.ng', 'health.gov.ng']
            if hostname in problematic_domains:
                return True  # Try anyway, but expect it might fail
            
            # Try DNS resolution first
            try:
                socket.gethostbyname(hostname)
            except socket.gaierror:
                return False
            
            return True
            
        except Exception:
            return True  # If check fails, still try the request

class NigerianStatsScraper:
    """Enhanced web scraper for Nigerian statistical data with multi-website support"""
    
    def __init__(self, max_workers=5, use_selenium=False, logger=None):
        self.request_session = RobustRequestSession(timeout=30, max_retries=2)
        self.max_workers = max_workers
        self.use_selenium = use_selenium and SELENIUM_AVAILABLE
        self.driver = None
        self.logger = logger
        
        # Test URLs before full scraping
        self.working_urls = {}
    
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
    
    def scrape_pdf(self, url):
        """Scrape data from PDF files using available libraries"""
        pdf_data = []
        
        try:
            self.log(f"Attempting to scrape PDF: {url}")
            
            # Check if we can access this URL first
            if not self.test_website_accessibility(url):
                self.log(f"⚠️ PDF URL not accessible: {url}")
                return pdf_data
            
            # Download PDF
            response = self.request_session.get(url)
            
            if response.status_code == 200 and 'application/pdf' in response.headers.get('content-type', '').lower():
                pdf_content = response.content
                
                # List of available PDF parsing methods
                methods = []
                
                # Add available methods
                if 'pdfplumber' in globals():
                    methods.append(self._parse_pdf_with_pdfplumber)
                
                if 'PyPDF2' in globals():
                    methods.append(self._parse_pdf_with_pypdf2)
                
                if 'pdfminer_extract' in globals():
                    methods.append(self._parse_pdf_with_pdfminer)
                
                if FITZ_AVAILABLE:
                    methods.append(self._parse_pdf_with_pymupdf)
                
                if TEXTTRACT_AVAILABLE:
                    methods.append(self._parse_pdf_with_textract)
                
                # Try methods in order
                for method in methods:
                    try:
                        data = method(pdf_content, url)
                        if data and len(data) > 0:
                            pdf_data.extend(data)
                            self.log(f"✓ Successfully parsed PDF with {method.__name__}")
                            break
                    except Exception as e:
                        self.log(f"✗ PDF parsing method {method.__name__} failed: {e}")
                        continue
                
                # If no method worked, try basic text extraction
                if not pdf_data:
                    basic_text = self._extract_basic_pdf_text(pdf_content)
                    if basic_text:
                        pdf_data.append({
                            'PDF_URL': url,
                            'Content_Type': 'PDF',
                            'Extracted_Text': basic_text[:500] + '...' if len(basic_text) > 500 else basic_text,
                            'Note': 'Basic text extraction',
                            'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                        })
            
        except Exception as e:
            self.log(f"Error scraping PDF {url}: {str(e)[:100]}...")
        
        return pdf_data
    
    def _parse_pdf_with_pdfplumber(self, pdf_content, url):
        """Parse PDF using pdfplumber"""
        data = []
        try:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                for i, page in enumerate(pdf.pages[:3]):  # Limit to first 3 pages
                    text = page.extract_text()
                    if text and len(text.strip()) > 10:
                        # Extract statistics
                        stats = self._extract_statistics_from_text(text)
                        for stat in stats:
                            data.append({
                                'PDF_URL': url,
                                'Page': i + 1,
                                'Content_Type': 'PDF_statistic',
                                'Extracted_Data': stat,
                                'Parser': 'pdfplumber',
                                'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                            })
        except Exception as e:
            self.log(f"pdfplumber error: {e}")
        return data
    
    def _parse_pdf_with_pypdf2(self, pdf_content, url):
        """Parse PDF using PyPDF2"""
        data = []
        try:
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            for i, page in enumerate(pdf_reader.pages[:3]):
                text = page.extract_text()
                if text:
                    stats = self._extract_statistics_from_text(text)
                    for stat in stats:
                        data.append({
                            'PDF_URL': url,
                            'Page': i + 1,
                            'Content_Type': 'PDF_text',
                            'Extracted_Data': stat[:200],
                            'Parser': 'PyPDF2',
                            'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                        })
        except Exception as e:
            self.log(f"PyPDF2 error: {e}")
        return data
    
    def _parse_pdf_with_pdfminer(self, pdf_content, url):
        """Parse PDF using pdfminer"""
        data = []
        try:
            text = pdfminer_extract(io.BytesIO(pdf_content))
            if text:
                stats = self._extract_statistics_from_text(text)
                for stat in stats[:10]:  # Limit to 10 statistics
                    data.append({
                        'PDF_URL': url,
                        'Content_Type': 'PDF_statistic',
                        'Extracted_Data': stat[:300],
                        'Parser': 'pdfminer',
                        'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                    })
        except Exception as e:
            self.log(f"pdfminer error: {e}")
        return data
    
    def _parse_pdf_with_pymupdf(self, pdf_content, url):
        """Parse PDF using PyMuPDF (fitz)"""
        data = []
        try:
            if FITZ_AVAILABLE:
                doc = fitz.open(stream=pdf_content, filetype="pdf")
                for i, page in enumerate(doc[:3]):
                    text = page.get_text()
                    if text:
                        lines = text.split('\n')
                        for line in lines[:30]:  # First 30 lines per page
                            if re.search(r'\d', line) and len(line.strip()) > 5:
                                data.append({
                                    'PDF_URL': url,
                                    'Page': i + 1,
                                    'Content_Type': 'PDF_text',
                                    'Extracted_Line': line.strip()[:200],
                                    'Parser': 'PyMuPDF',
                                    'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                                })
        except Exception as e:
            self.log(f"PyMuPDF error: {e}")
        return data
    
    def _parse_pdf_with_textract(self, pdf_content, url):
        """Parse PDF using textract"""
        data = []
        try:
            if TEXTTRACT_AVAILABLE:
                # Save temporarily to file
                import tempfile
                with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                    tmp.write(pdf_content)
                    tmp_path = tmp.name
                
                text = textract.process(tmp_path).decode('utf-8')
                os.unlink(tmp_path)
                
                if text:
                    stats = re.findall(r'\b\d+\.?\d*\s*%\b|\b\d{1,3}(?:,\d{3})*\b', text)
                    for stat in stats[:10]:
                        data.append({
                            'PDF_URL': url,
                            'Content_Type': 'PDF_number',
                            'Extracted_Number': stat,
                            'Parser': 'textract',
                            'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                        })
        except Exception as e:
            self.log(f"textract error: {e}")
        return data
    
    def _extract_statistics_from_text(self, text):
        """Extract statistical patterns from text"""
        stats = []
        
        # Statistical patterns to look for
        patterns = [
            r'\b\d+\.?\d*\s*%\b',  # Percentages
            r'\b\d{1,3}(?:,\d{3})+\b',  # Large numbers with commas
            r'\bGDP.*?\d[,\d]*\.?\d*\b',  # GDP references
            r'\bpopulation.*?\d[,\d]*\.?\d*\b',  # Population references
            r'\bunemployment.*?\d[,\d]*\.?\d*\b',  # Unemployment references
            r'\binflation.*?\d[,\d]*\.?\d*\b',  # Inflation references
            r'\b\d+\.?\d*\s*(million|billion|thousand)\b',  # Quantities
            r'\b(?:rate|ratio|percentage|proportion).*?\d+\.?\d*\b'  # Rates and ratios
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            stats.extend(matches)
        
        return list(set(stats))[:20]  # Return unique matches, limit to 20
    
    def _extract_basic_pdf_text(self, pdf_content):
        """Basic text extraction from PDF using available libraries"""
        text = ""
        
        # Try PyPDF2 first
        try:
            if 'PyPDF2' in globals():
                pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
                for page in pdf_reader.pages[:2]:  # First 2 pages only
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                return text
        except:
            pass
        
        # Try basic string extraction for PDF headers
        try:
            # PDFs often start with "%PDF-" and have text between parentheses
            pdf_str = pdf_content.decode('latin-1', errors='ignore')
            # Extract text between parentheses (common in PDFs)
            matches = re.findall(r'\((.*?)\)', pdf_str)
            text = ' '.join(matches[:30])  # First 30 matches
        except:
            pass
        
        return text
    
    def scrape_website(self, website_config, search_query):
        """Scrape a single website based on configuration"""
        website_data = []
        
        try:
            url = website_config.get('url', '')
            name = website_config.get('name', 'Unknown')
            scrape_method = website_config.get('scrape_method', 'direct')
            
            # Test website accessibility first
            if not self.test_website_accessibility(url):
                self.log(f"⚠️ Skipping {name} - Website not accessible")
                return website_data
            
            self.log(f"Scraping {name}: {url}")
            
            if scrape_method == 'selenium' and self.use_selenium:
                website_data = self.scrape_with_selenium(url, search_query)
            elif scrape_method == 'api':
                website_data = self.scrape_with_api(url, search_query)
            else:
                website_data = self.scrape_with_requests(url, search_query)
            
            # Add source information to all records
            for item in website_data:
                item['Source_Website'] = name
                item['Source_URL'] = url
                item['Scrape_Method'] = scrape_method
            
            if website_data:
                self.log(f"✓ Found {len(website_data)} records from {name}")
            else:
                self.log(f"⚠️ No data found from {name}")
            
        except Exception as e:
            error_msg = str(e)
            if "ConnectionError" in error_msg or "Timeout" in error_msg:
                self.log(f"✗ Network error for {name}: {error_msg[:80]}...")
            else:
                self.log(f"✗ Error scraping {name}: {error_msg[:80]}...")
        
        return website_data
    
    def scrape_with_requests(self, url, search_query):
        """Scrape website using requests library"""
        data = []
        
        try:
            response = self.request_session.get(url)
            
            if response.status_code == 200:
                # Check content type
                content_type = response.headers.get('content-type', '').lower()
                
                if 'application/pdf' in content_type and PDF_LIBRARIES_AVAILABLE:
                    # Handle PDF files
                    pdf_data = self.scrape_pdf(url)
                    data.extend(pdf_data)
                
                elif 'application/json' in content_type:
                    # Handle JSON APIs
                    json_data = response.json()
                    data.extend(self.parse_json_data(json_data, url))
                
                elif 'application/xml' in content_type or 'text/xml' in content_type:
                    # Handle XML data
                    xml_data = ET.fromstring(response.content)
                    data.extend(self.parse_xml_data(xml_data, url))
                
                elif 'text/html' in content_type:
                    # Handle HTML pages
                    soup = BeautifulSoup(response.content, 'html.parser')
                    data.extend(self.extract_html_data(soup, url, search_query))
                
                elif 'text/plain' in content_type:
                    # Handle plain text
                    text_data = response.text
                    data.extend(self.parse_text_data(text_data, url))
                
                # Check for embedded PDF links
                if PDF_LIBRARIES_AVAILABLE:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    pdf_links = soup.find_all('a', href=lambda x: x and x.lower().endswith('.pdf'))
                    
                    for link in pdf_links[:1]:  # Limit to 1 PDF
                        pdf_url = urljoin(url, link['href'])
                        pdf_data = self.scrape_pdf(pdf_url)
                        data.extend(pdf_data)
        
        except Exception as e:
            self.log(f"✗ Error scraping {url}: {str(e)[:80]}...")
        
        return data
    
    def scrape_with_selenium(self, url, search_query):
        """Scrape JavaScript-heavy websites using Selenium"""
        data = []
        
        if not self.driver:
            return self.scrape_with_requests(url, search_query)  # Fallback
        
        try:
            self.driver.get(url)
            time.sleep(2)  # Wait for JavaScript to load
            
            # Get page source after JavaScript execution
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Extract data
            data.extend(self.extract_html_data(soup, url, search_query))
            
        except Exception as e:
            self.log(f"✗ Selenium error for {url}: {str(e)[:80]}...")
            # Fallback to requests
            data.extend(self.scrape_with_requests(url, search_query))
        
        return data
    
    def scrape_with_api(self, url, search_query):
        """Scrape data from APIs"""
        data = []
        
        try:
            response = self.request_session.get(url)
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                
                if 'application/json' in content_type:
                    json_data = response.json()
                    data.extend(self.parse_json_data(json_data, url))
                elif 'application/xml' in content_type or 'text/xml' in content_type:
                    xml_data = ET.fromstring(response.content)
                    data.extend(self.parse_xml_data(xml_data, url))
        
        except Exception as e:
            self.log(f"✗ API error for {url}: {str(e)[:80]}...")
        
        return data
    
    def extract_html_data(self, soup, url, search_query=None):
        """Extract data from HTML content"""
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
        
        # Extract tables (common in statistical websites)
        tables = soup.find_all('table')
        for i, table in enumerate(tables[:2]):  # First 2 tables
            try:
                # Try to read table with pandas
                df_list = pd.read_html(str(table))
                if df_list:
                    df = df_list[0]
                    # Convert first few rows to dictionary
                    for idx, row in df.head(2).iterrows():
                        row_dict = row.to_dict()
                        row_dict['Table_Index'] = i
                        row_dict['Source_URL'] = url
                        row_dict['Content_Type'] = 'HTML_Table'
                        data.append(row_dict)
            except:
                # Manual table extraction
                rows = table.find_all('tr')
                for row in rows[:3]:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:  # Only if there are data cells
                        row_data = [cell.get_text(strip=True) for cell in cells]
                        if any(re.search(r'\d', text) for text in row_data):
                            data.append({
                                'Table_Data': ' | '.join(row_data),
                                'Source_URL': url,
                                'Content_Type': 'HTML_Table_Raw',
                                'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                            })
        
        # Extract paragraph text with numbers (likely statistics)
        paragraphs = soup.find_all(['p', 'div', 'span'])
        for element in paragraphs[:10]:  # First 10 elements
            text = element.get_text(strip=True)
            if len(text) > 20 and len(text) < 500:  # Reasonable length
                if re.search(r'\d+\.?\d*\s*%|\d+[\d,]*\.?\d*', text):
                    data.append({
                        'Text_Content': text[:300],
                        'Source_URL': url,
                        'Content_Type': 'HTML_Text',
                        'Word_Count': len(text.split()),
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
    
    def parse_json_data(self, json_data, url):
        """Parse JSON data"""
        data = []
        
        try:
            if isinstance(json_data, list):
                for item in json_data[:5]:  # Limit to 5 items
                    if isinstance(item, dict):
                        flat_item = self.flatten_dict(item)
                        flat_item['Source_URL'] = url
                        flat_item['Data_Type'] = 'JSON'
                        data.append(flat_item)
            elif isinstance(json_data, dict):
                flat_item = self.flatten_dict(json_data)
                flat_item['Source_URL'] = url
                flat_item['Data_Type'] = 'JSON'
                data.append(flat_item)
        except Exception as e:
            self.log(f"JSON parsing error: {str(e)[:80]}...")
        
        return data
    
    def parse_xml_data(self, xml_data, url):
        """Parse XML data"""
        data = []
        
        try:
            # Simple XML to dict conversion
            xml_dict = {}
            for child in xml_data:
                if len(child) > 0:
                    xml_dict[child.tag] = self.xml_to_dict(child)
                else:
                    xml_dict[child.tag] = child.text
            
            if xml_dict:
                xml_dict['Source_URL'] = url
                xml_dict['Data_Type'] = 'XML'
                data.append(xml_dict)
        except Exception as e:
            self.log(f"XML parsing error: {str(e)[:80]}...")
        
        return data
    
    def xml_to_dict(self, element):
        """Convert XML element to dictionary"""
        result = {}
        for child in element:
            if len(child) > 0:
                result[child.tag] = self.xml_to_dict(child)
            else:
                result[child.tag] = child.text
        return result
    
    def parse_text_data(self, text_data, url):
        """Parse plain text data"""
        data = []
        
        try:
            lines = text_data.split('\n')
            for line in lines[:30]:  # First 30 lines
                if re.search(r'\d+\.?\d*\s*%|\d+\s*(?:million|billion|thousand)', line, re.IGNORECASE):
                    data.append({
                        'Text_Line': line.strip()[:200],
                        'Source_URL': url,
                        'Data_Type': 'Text',
                        'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                    })
        except Exception as e:
            self.log(f"Text parsing error: {str(e)[:80]}...")
        
        return data
    
    def flatten_dict(self, d, parent_key='', sep='_'):
        """Flatten nested dictionary"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, item in enumerate(v[:2]):  # Limit to 2 items
                    if isinstance(item, dict):
                        items.extend(self.flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
                    else:
                        items.append((f"{new_key}_{i}", str(item)))
            else:
                items.append((new_key, str(v)))
        return dict(items)
    
    def get_working_websites(self, websites):
        """Filter and return only working websites"""
        working = []
        
        self.log("Testing website accessibility...")
        
        for website in websites:
            url = website.get('url', '')
            name = website.get('name', 'Unknown')
            
            try:
                # Test accessibility
                if self.test_website_accessibility(url):
                    working.append(website)
                    self.log(f"✓ {name} is accessible")
                else:
                    self.log(f"⚠️ Skipping {name} - Not accessible")
            except:
                self.log(f"⚠️ Skipping {name} - Connection test failed")
        
        return working
    
    def get_nigerian_statistical_websites(self):
        """Get comprehensive list of Nigerian statistical websites (filtered for accessibility)"""
        # List of reliable websites that are usually accessible
        websites = [
            # Most reliable - International organizations
            {
                'name': 'World Bank Nigeria Data',
                'url': 'https://data.worldbank.org/country/nigeria',
                'scrape_method': 'direct',
                'category': 'International Statistics',
                'priority': 1
            },
            {
                'name': 'IMF Nigeria Economic Indicators',
                'url': 'https://www.imf.org/en/Countries/NGA',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 1
            },
            {
                'name': 'UN Data Nigeria',
                'url': 'https://data.un.org/en/iso/ng.html',
                'scrape_method': 'direct',
                'category': 'International Statistics',
                'priority': 1
            },
            
            # Nigerian Government (most reliable ones)
            {
                'name': 'National Bureau of Statistics (NBS)',
                'url': 'https://www.nigerianstat.gov.ng',
                'scrape_method': 'direct',
                'category': 'Official Statistics',
                'priority': 1
            },
            {
                'name': 'Central Bank of Nigeria',
                'url': 'https://www.cbn.gov.ng',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 2
            },
            
            # Other reliable sources
            {
                'name': 'WHO Nigeria Data',
                'url': 'https://www.who.int/countries/nga',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 1
            },
            {
                'name': 'Knoema Nigeria Data',
                'url': 'https://knoema.com/atlas/Nigeria',
                'scrape_method': 'direct',
                'category': 'General Statistics',
                'priority': 2
            },
            {
                'name': 'Index Mundi Nigeria',
                'url': 'https://www.indexmundi.com/nigeria',
                'scrape_method': 'direct',
                'category': 'General Statistics',
                'priority': 2
            },
            {
                'name': 'NairaMetrics',
                'url': 'https://nairametrics.com',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 2
            }
        ]
        
        # Filter for working websites
        working_websites = self.get_working_websites(websites)
        
        # If no websites are working, return at least the international ones
        if not working_websites:
            self.log("⚠️ No websites accessible, using fallback list")
            return [
                {
                    'name': 'World Bank Nigeria Data',
                    'url': 'https://data.worldbank.org/country/nigeria',
                    'scrape_method': 'direct',
                    'category': 'International Statistics',
                    'priority': 1
                },
                {
                    'name': 'UN Data Nigeria',
                    'url': 'https://data.un.org/en/iso/ng.html',
                    'scrape_method': 'direct',
                    'category': 'International Statistics',
                    'priority': 1
                }
            ]
        
        return working_websites
    
    def smart_scrape_multiple_websites(self, search_query, selected_categories=None, max_websites=8):
        """Scrape multiple websites intelligently based on search query and categories"""
        all_data = []
        
        # Get all websites (already filtered for accessibility)
        all_websites = self.get_nigerian_statistical_websites()
        
        self.log(f"Found {len(all_websites)} accessible websites")
        
        # Filter by categories if specified
        if selected_categories:
            filtered_websites = [w for w in all_websites 
                               if w.get('category') in selected_categories]
        else:
            filtered_websites = all_websites
        
        # Sort by priority and limit number
        filtered_websites.sort(key=lambda x: x.get('priority', 99))
        websites_to_scrape = filtered_websites[:max_websites]
        
        self.log(f"Starting multi-website scrape: {len(websites_to_scrape)} websites")
        
        # Show website list
        website_names = [w['name'] for w in websites_to_scrape]
        self.log(f"Websites to scrape: {', '.join(website_names)}")
        
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
                    website_data = future.result()
                    if website_data:
                        all_data.extend(website_data)
                        self.log(f"({completed}/{total}) ✓ {website['name']}: {len(website_data)} records")
                    else:
                        self.log(f"({completed}/{total}) ⚠️ {website['name']}: No data found")
                except Exception as e:
                    self.log(f"({completed}/{total}) ✗ Error scraping {website['name']}: {str(e)[:80]}...")
        
        self.log(f"Multi-website scraping complete. Total records: {len(all_data)}")
        
        if all_data:
            df = pd.DataFrame(all_data)
            # Clean up the data
            df = df.drop_duplicates()
            return df
        
        # If no data found, return sample data
        self.log("⚠️ No data found from websites, returning sample data")
        sample_data = self.get_sample_data(search_query)
        return pd.DataFrame(sample_data)
    
    def get_sample_data(self, search_query):
        """Return sample data when no real data is found"""
        timestamp = datetime.now().strftime('%Y-%m-%d')
        
        sample_data = [
            {
                'Statistical_Match': 'GDP growth: 2.5%',
                'Category': 'Economic',
                'Source_URL': 'https://data.worldbank.org/country/nigeria',
                'Pattern_Type': 'GDP.*growth',
                'Scrape_Date': timestamp,
                'Source_Website': 'World Bank Nigeria Data',
                'Scrape_Method': 'direct',
                'Note': 'Sample data - actual website scraping failed'
            },
            {
                'Statistical_Match': 'Population: 223 million',
                'Category': 'Demographic',
                'Source_URL': 'https://data.un.org/en/iso/ng.html',
                'Pattern_Type': 'population.*million',
                'Scrape_Date': timestamp,
                'Source_Website': 'UN Data Nigeria',
                'Scrape_Method': 'direct',
                'Note': 'Sample data - actual website scraping failed'
            },
            {
                'Statistical_Match': 'Inflation rate: 28.9%',
                'Category': 'Economic',
                'Source_URL': 'https://www.imf.org/en/Countries/NGA',
                'Pattern_Type': 'inflation.*rate',
                'Scrape_Date': timestamp,
                'Source_Website': 'IMF Nigeria',
                'Scrape_Method': 'direct',
                'Note': 'Sample data - actual website scraping failed'
            }
        ]
        
        # Add search query specific samples
        if 'gdp' in search_query.lower():
            sample_data.append({
                'Statistical_Match': 'GDP 2023: $477 billion',
                'Category': 'Economic',
                'Source_URL': 'https://www.nigerianstat.gov.ng',
                'Pattern_Type': 'GDP.*billion',
                'Scrape_Date': timestamp,
                'Source_Website': 'National Bureau of Statistics',
                'Scrape_Method': 'direct',
                'Note': 'Sample data - actual website scraping failed'
            })
        
        if 'population' in search_query.lower():
            sample_data.append({
                'Statistical_Match': 'Population density: 226/km²',
                'Category': 'Demographic',
                'Source_URL': 'https://data.worldbank.org/country/nigeria',
                'Pattern_Type': 'Population.*density',
                'Scrape_Date': timestamp,
                'Source_Website': 'World Bank',
                'Scrape_Method': 'direct',
                'Note': 'Sample data - actual website scraping failed'
            })
        
        return sample_data

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

def main():
    """Main application function"""
    
    # Initialize session state
    if 'scraped_data' not in st.session_state:
        st.session_state.scraped_data = None
    if 'scraping_in_progress' not in st.session_state:
        st.session_state.scraping_in_progress = False
    if 'scraping_log' not in st.session_state:
        st.session_state.scraping_log = []
    if 'google_drive_auth' not in st.session_state:
        st.session_state.google_drive_auth = None
    if 'google_drive_folder_id' not in st.session_state:
        st.session_state.google_drive_folder_id = None
    
    # Header
    st.markdown('<h1 class="main-header">🌐 Nigeria Statistics Web Scraper Pro</h1>', unsafe_allow_html=True)
    
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
            <p><strong>Advanced web scraper for Nigerian statistical data with robust error handling</strong></p>
            <p>Automatically filters out inaccessible websites and provides fallback data</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Data sources selection
        st.subheader("📡 Data Categories")
        
        categories = [
            "Official Statistics",
            "Economic Statistics",
            "Health Statistics",
            "Education Statistics",
            "International Statistics",
            "General Statistics"
        ]
        
        selected_categories = st.multiselect(
            "Select categories to scrape:",
            categories,
            default=["Official Statistics", "Economic Statistics", "International Statistics"]
        )
        
        # Scraping options
        st.subheader("⚡ Scraping Options")
        
        max_websites = st.slider("Maximum websites to scrape", 1, 20, 8)
        max_workers = st.slider("Concurrent scrapers", 1, 5, 3)
        timeout = st.slider("Timeout per website (seconds)", 10, 60, 30)
        
        enable_fallback = st.checkbox("Enable fallback sample data", value=True, 
                                      help="Show sample data when websites are inaccessible")
        
        # Google Drive Configuration
        st.subheader("☁️ Google Drive Options")
        
        if GOOGLE_DRIVE_AVAILABLE:
            auto_save_drive = st.checkbox("Auto-save to Google Drive", value=False)
            
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
            "Export format:",
            ["CSV", "JSON", "Excel"]
        )
        
        auto_download = st.checkbox("Auto-download after scraping", value=True)
    
    # Main content
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.subheader("🔍 What Nigerian data are you looking for?")
        
        # Search input
        search_query = st.text_input(
            "Enter search query:",
            placeholder="e.g., 'GDP growth 2023', 'population Nigeria', 'unemployment rate'",
            key="search_input"
        )
        
        # Quick search buttons
        st.subheader("🚀 Quick Statistical Searches")
        
        quick_searches = [
            "GDP Nigeria",
            "Population",
            "Unemployment",
            "Inflation",
            "Economic Indicators",
            "Health Statistics"
        ]
        
        cols = st.columns(3)
        for i, query in enumerate(quick_searches):
            with cols[i % 3]:
                if st.button(f"🔎 {query}", use_container_width=True, key=f"quick_{i}"):
                    st.session_state.search_query = query
                    st.rerun()
    
    with col2:
        st.subheader("🎯 Reliable Sources")
        st.write("**Tested and Accessible:**")
        st.write("• World Bank Nigeria")
        st.write("• UN Data Nigeria")
        st.write("• IMF Nigeria")
        st.write("• WHO Nigeria")
        st.write("• NBS (if accessible)")
        
        if st.button("📋 Test Website Access", key="test_access"):
            with st.spinner("Testing website accessibility..."):
                scraper = NigerianStatsScraper(logger=logger)
                websites = scraper.get_nigerian_statistical_websites()
                st.info(f"✅ {len(websites)} websites are accessible")
                for website in websites:
                    st.write(f"• {website['name']}")
    
    # Scrape button
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.button("🚀 Start Robust Scraping", type="primary", use_container_width=True, key="scrape_button"):
            if not search_query and 'search_query' not in st.session_state:
                st.warning("⚠️ Please enter a search query or select a quick search")
            else:
                query = search_query or getattr(st.session_state, 'search_query', '')
                
                st.session_state.scraping_in_progress = True
                st.session_state.scraping_log = []  # Clear previous log
                
                with st.spinner(f"🌐 Testing and scraping accessible websites..."):
                    # Initialize scraper with thread-safe logger
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
                    
                    # Perform the actual scraping
                    status_text.text("Scraping accessible websites...")
                    progress_bar.progress(50)
                    
                    scraped_data = scraper.smart_scrape_multiple_websites(
                        query, 
                        selected_categories, 
                        max_websites
                    )
                    
                    # Update progress
                    progress_bar.progress(90)
                    status_text.text("Processing results...")
                    
                    # Close Selenium if used
                    scraper.close_selenium()
                    
                    if scraped_data is not None and not scraped_data.empty:
                        st.session_state.scraped_data = scraped_data
                        
                        status_text.text("✅ Scraping completed!")
                        progress_bar.progress(100)
                        
                        # Check if we have real data or sample data
                        if 'Note' in scraped_data.columns and 'sample' in scraped_data['Note'].iloc[0].lower():
                            st.warning("⚠️ Using sample data - actual websites were inaccessible")
                            st.info("💡 Try different search terms or check your internet connection")
                        else:
                            st.success(f"🎉 Successfully scraped {len(scraped_data)} records!")
                        
                        # Save to Google Drive if enabled
                        if GOOGLE_DRIVE_AVAILABLE and st.session_state.get('google_drive_auth'):
                            with st.spinner("📤 Uploading to Google Drive..."):
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                file_name = f"nigeria_stats_{query.replace(' ', '_')}_{timestamp}"
                                
                                # Create folder
                                folder_name = f"Nigeria_Stats_{datetime.now().strftime('%Y%m')}"
                                folder_id = st.session_state.google_drive_auth.create_folder(folder_name)
                                
                                if folder_id:
                                    result = st.session_state.google_drive_auth.upload_dataframe(
                                        scraped_data,
                                        file_name,
                                        folder_id,
                                        export_format.lower()
                                    )
                                    
                                    if result:
                                        st.success(f"✅ File uploaded to Google Drive!")
                                        st.info(f"📁 **Folder:** {folder_name}")
                                        st.info(f"📄 **File:** {result['file_name']}")
                                        st.markdown(f"🔗 **Link:** [Open in Google Drive]({result['web_link']})", unsafe_allow_html=True)
                        
                        # Auto-download if enabled
                        if auto_download:
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            filename = f"nigeria_stats_{query.replace(' ', '_')}_{timestamp}"
                            
                            if export_format == "CSV":
                                csv_link = create_download_link(scraped_data, f"{filename}.csv", "csv")
                                st.markdown(csv_link, unsafe_allow_html=True)
                            
                            elif export_format == "JSON":
                                json_link = create_download_link(scraped_data, f"{filename}.json", "json")
                                st.markdown(json_link, unsafe_allow_html=True)
                            
                            elif export_format == "Excel":
                                os.makedirs("scraped_data", exist_ok=True)
                                excel_path = f"scraped_data/{filename}.xlsx"
                                scraped_data.to_excel(excel_path, index=False)
                                with open(excel_path, "rb") as f:
                                    excel_bytes = f.read()
                                st.download_button(
                                    label="📥 Download Excel",
                                    data=excel_bytes,
                                    file_name=f"{filename}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )
                    else:
                        st.error("❌ No data found. Please check your internet connection and try again.")
                    
                    st.session_state.scraping_in_progress = False
    
    with col2:
        if st.button("🔄 Clear Results", use_container_width=True, key="clear_button"):
            st.session_state.scraped_data = None
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
                    if "✓" in log_entry_text or "Success" in log_entry_text:
                        st.success(log_entry_text)
                    elif "✗" in log_entry_text or "Error" in log_entry_text:
                        st.error(log_entry_text)
                    elif "⚠️" in log_entry_text or "Warning" in log_entry_text:
                        st.warning(log_entry_text)
                    else:
                        st.info(log_entry_text)
    
    # Display scraped data
    if st.session_state.scraped_data is not None:
        st.header("📊 Scraped Data Summary")
        
        df = st.session_state.scraped_data
        
        # Show statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", len(df))
        with col2:
            sources = df['Source_Website'].nunique() if 'Source_Website' in df.columns else 0
            st.metric("Sources", sources)
        with col3:
            categories = df['Category'].nunique() if 'Category' in df.columns else 0
            st.metric("Categories", categories)
        
        # Check if it's sample data
        is_sample_data = 'Note' in df.columns and any('sample' in str(note).lower() for note in df['Note'].fillna(''))
        if is_sample_data:
            st.warning("⚠️ This is sample data. Actual websites were inaccessible.")
        
        # Data preview with tabs
        tab1, tab2, tab3 = st.tabs(["📋 Data Preview", "🔍 Filter Data", "📈 Statistics"])
        
        with tab1:
            st.subheader("Data Preview")
            st.dataframe(df, use_container_width=True, height=400)
        
        with tab2:
            st.subheader("Filter Options")
            
            filter_col1, filter_col2 = st.columns(2)
            
            with filter_col1:
                if 'Source_Website' in df.columns:
                    sources = df['Source_Website'].unique()
                    selected_sources = st.multiselect(
                        "Filter by website:",
                        sources,
                        default=sources[:min(3, len(sources))]
                    )
            
            with filter_col2:
                if 'Category' in df.columns:
                    categories = df['Category'].unique()
                    selected_cats = st.multiselect(
                        "Filter by category:",
                        categories,
                        default=categories[:min(3, len(categories))]
                    )
            
            # Apply filters
            filtered_df = df.copy()
            if 'selected_sources' in locals() and selected_sources:
                filtered_df = filtered_df[filtered_df['Source_Website'].isin(selected_sources)]
            if 'selected_cats' in locals() and selected_cats:
                filtered_df = filtered_df[filtered_df['Category'].isin(selected_cats)]
            
            st.dataframe(filtered_df, use_container_width=True, height=300)
        
        with tab3:
            st.subheader("Data Statistics")
            
            # Column information
            st.write("**Column Information:**")
            col_info = []
            for col in df.columns:
                col_info.append({
                    'Column': col,
                    'Type': str(df[col].dtype),
                    'Non-Null': df[col].count(),
                    'Unique': df[col].nunique()
                })
            st.table(pd.DataFrame(col_info))
        
        # Export section
        st.header("💾 Export Data")
        
        export_filename = st.text_input(
            "Export filename:",
            value=f"nigeria_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            key="export_filename"
        )
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📥 Download CSV", use_container_width=True, key="export_csv"):
                csv_link = create_download_link(df, f"{export_filename}.csv", "csv")
                st.markdown(csv_link, unsafe_allow_html=True)
        
        with col2:
            if st.button("📥 Download JSON", use_container_width=True, key="export_json"):
                json_link = create_download_link(df, f"{export_filename}.json", "json")
                st.markdown(json_link, unsafe_allow_html=True)
        
        with col3:
            if st.button("💾 Save Locally", use_container_width=True, key="save_local"):
                os.makedirs("scraped_data", exist_ok=True)
                filepath = f"scraped_data/{export_filename}.csv"
                df.to_csv(filepath, index=False)
                st.success(f"✅ Data saved to: {filepath}")

# Footer
st.markdown("---")
st.markdown("""
    <div style="text-align: center; color: #666; padding: 2rem 0;">
        <p><strong>Nigeria Statistics Web Scraper</strong> • Version 3.1</p>
        <p>🌐 <strong>Robust scraping</strong> with automatic website accessibility testing</p>
        <p>🛡️ <strong>Error handling:</strong> Skips inaccessible websites, provides fallback data</p>
        <p>☁️ <strong>Google Drive integration</strong> for cloud storage</p>
        <p>⚠️ <strong>Note:</strong> Some Nigerian government websites may be inaccessible from your location</p>
    </div>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    # Create necessary directories
    os.makedirs("static", exist_ok=True)
    os.makedirs("scraped_data", exist_ok=True)
    main()