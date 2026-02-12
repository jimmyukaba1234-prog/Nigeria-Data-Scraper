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
import webbrowser

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

class NigerianStatsScraper:
    """Enhanced web scraper for Nigerian statistical data with multi-website support"""
    
    def __init__(self, max_workers=5, use_selenium=False, logger=None):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        self.timeout = 30
        self.max_workers = max_workers
        self.use_selenium = use_selenium and SELENIUM_AVAILABLE
        self.driver = None
        self.logger = logger
    
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
    
    def scrape_pdf(self, url):
        """Scrape data from PDF files using available libraries"""
        pdf_data = []
        
        try:
            self.log(f"Scraping PDF: {url}")
            
            # Download PDF
            response = self.session.get(url, timeout=self.timeout)
            
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
                            self.log(f"Successfully parsed PDF with {method.__name__}")
                            break
                    except Exception as e:
                        self.log(f"PDF parsing method {method.__name__} failed: {e}")
                        continue
                
                # If no method worked, try basic text extraction
                if not pdf_data:
                    basic_text = self._extract_basic_pdf_text(pdf_content)
                    if basic_text:
                        pdf_data.append({
                            'PDF_URL': url,
                            'Content_Type': 'PDF',
                            'Extracted_Text': basic_text[:1000] + '...' if len(basic_text) > 1000 else basic_text,
                            'Note': 'Basic text extraction',
                            'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                        })
            
        except Exception as e:
            self.log(f"Error scraping PDF {url}: {e}")
        
        return pdf_data
    
    def _parse_pdf_with_pdfplumber(self, pdf_content, url):
        """Parse PDF using pdfplumber"""
        data = []
        try:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                for i, page in enumerate(pdf.pages[:5]):  # Limit to first 5 pages
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
            for i, page in enumerate(pdf_reader.pages[:5]):
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
                for stat in stats[:20]:  # Limit to 20 statistics
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
                for i, page in enumerate(doc[:5]):
                    text = page.get_text()
                    if text:
                        lines = text.split('\n')
                        for line in lines[:50]:  # First 50 lines per page
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
                    for stat in stats[:20]:
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
        
        return list(set(stats))[:50]  # Return unique matches, limit to 50
    
    def _extract_basic_pdf_text(self, pdf_content):
        """Basic text extraction from PDF using available libraries"""
        text = ""
        
        # Try PyPDF2 first
        try:
            if 'PyPDF2' in globals():
                pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
                for page in pdf_reader.pages[:3]:  # First 3 pages only
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
            text = ' '.join(matches[:50])  # First 50 matches
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
                self.log(f"Found {len(website_data)} records from {name}")
            else:
                self.log(f"No data found from {name}")
            
        except Exception as e:
            self.log(f"Error scraping {name}: {str(e)}")
        
        return website_data
    
    def scrape_with_requests(self, url, search_query):
        """Scrape website using requests library"""
        data = []
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            
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
                    
                    for link in pdf_links[:2]:  # Limit to 2 PDFs
                        pdf_url = urljoin(url, link['href'])
                        pdf_data = self.scrape_pdf(pdf_url)
                        data.extend(pdf_data)
        
        except Exception as e:
            self.log(f"Error in requests scraping for {url}: {str(e)}")
        
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
            self.log(f"Selenium scraping error for {url}: {str(e)}")
            # Fallback to requests
            data.extend(self.scrape_with_requests(url, search_query))
        
        return data
    
    def scrape_with_api(self, url, search_query):
        """Scrape data from APIs"""
        data = []
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                
                if 'application/json' in content_type:
                    json_data = response.json()
                    data.extend(self.parse_json_data(json_data, url))
                elif 'application/xml' in content_type or 'text/xml' in content_type:
                    xml_data = ET.fromstring(response.content)
                    data.extend(self.parse_xml_data(xml_data, url))
        
        except Exception as e:
            self.log(f"API scraping error for {url}: {str(e)}")
        
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
            for match in matches[:5]:  # Limit to 5 matches per pattern
                data.append({
                    'Statistical_Match': match,
                    'Category': category,
                    'Source_URL': url,
                    'Pattern_Type': pattern,
                    'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                })
        
        # Extract tables (common in statistical websites)
        tables = soup.find_all('table')
        for i, table in enumerate(tables[:3]):  # First 3 tables
            try:
                # Try to read table with pandas
                df_list = pd.read_html(str(table))
                if df_list:
                    df = df_list[0]
                    # Convert first few rows to dictionary
                    for idx, row in df.head(3).iterrows():
                        row_dict = row.to_dict()
                        row_dict['Table_Index'] = i
                        row_dict['Source_URL'] = url
                        row_dict['Content_Type'] = 'HTML_Table'
                        data.append(row_dict)
            except:
                # Manual table extraction
                rows = table.find_all('tr')
                for row in rows[:5]:
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
        for element in paragraphs[:20]:  # First 20 elements
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
                for item in json_data[:10]:  # Limit to 10 items
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
            self.log(f"JSON parsing error for {url}: {str(e)}")
        
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
            self.log(f"XML parsing error for {url}: {str(e)}")
        
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
            for line in lines[:50]:  # First 50 lines
                if re.search(r'\d+\.?\d*\s*%|\d+\s*(?:million|billion|thousand)', line, re.IGNORECASE):
                    data.append({
                        'Text_Line': line.strip()[:200],
                        'Source_URL': url,
                        'Data_Type': 'Text',
                        'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                    })
        except Exception as e:
            self.log(f"Text parsing error: {str(e)}")
        
        return data
    
    def flatten_dict(self, d, parent_key='', sep='_'):
        """Flatten nested dictionary"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, item in enumerate(v[:3]):  # Limit to 3 items
                    if isinstance(item, dict):
                        items.extend(self.flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
                    else:
                        items.append((f"{new_key}_{i}", str(item)))
            else:
                items.append((new_key, str(v)))
        return dict(items)
    
    def get_nigerian_statistical_websites(self):
        """Get comprehensive list of Nigerian statistical websites"""
        websites = [
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
                'priority': 1
            },
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
                'priority': 2
            },
            {
                'name': 'WHO Nigeria Data',
                'url': 'https://www.who.int/countries/nga',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 2
            },
            {
                'name': 'NBS Statistical Reports',
                'url': 'https://nigerianstat.gov.ng/elibrary',
                'scrape_method': 'direct',
                'category': 'Official Statistics',
                'priority': 1
            },
            {
                'name': 'UN Data Nigeria',
                'url': 'https://data.un.org/en/iso/ng.html',
                'scrape_method': 'direct',
                'category': 'International Statistics',
                'priority': 2
            },
            {
                'name': 'NairaMetrics Economic Data',
                'url': 'https://nairametrics.com',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 2
            }
        ]
        
        return websites
    
    def smart_scrape_multiple_websites(self, search_query, selected_categories=None, max_websites=15):
        """Scrape multiple websites intelligently based on search query and categories"""
        all_data = []
        
        # Get all websites
        all_websites = self.get_nigerian_statistical_websites()
        
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
        self.log(f"Websites to scrape: {', '.join(website_names[:5])}..." if len(website_names) > 5 else f"Websites to scrape: {', '.join(website_names)}")
        
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
                        self.log(f"({completed}/{total}) {website['name']}: {len(website_data)} records")
                    else:
                        self.log(f"({completed}/{total}) {website['name']}: No data found")
                except Exception as e:
                    self.log(f"({completed}/{total}) Error scraping {website['name']}: {str(e)}")
        
        self.log(f"Multi-website scraping complete. Total records: {len(all_data)}")
        
        if all_data:
            df = pd.DataFrame(all_data)
            # Clean up the data
            df = df.drop_duplicates()
            return df
        return None

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
            <p><strong>Advanced web scraper for Nigerian statistical data with Google Drive integration</strong></p>
            <p>Extracts real data from Nigerian government agencies and saves directly to Google Drive</p>
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
            default=["Official Statistics", "Economic Statistics"]
        )
        
        # Scraping options
        st.subheader("⚡ Scraping Options")
        
        max_websites = st.slider("Maximum websites to scrape", 1, 20, 8)
        max_workers = st.slider("Concurrent scrapers", 1, 5, 3)
        timeout = st.slider("Timeout per website (seconds)", 10, 60, 30)
        
        if SELENIUM_AVAILABLE:
            use_selenium = st.checkbox("Use Selenium for JavaScript sites", value=False)
        else:
            use_selenium = False
            st.info("Selenium not available")
        
        if PDF_LIBRARIES_AVAILABLE:
            enable_pdf = st.checkbox("Enable PDF scraping", value=True)
        else:
            enable_pdf = False
            st.info("PDF libraries limited")
        
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
                        
                        # Create or get folder
                        folder_name = f"Nigeria_Stats_{datetime.now().strftime('%Y%m')}"
                        folder_id = drive_manager.create_folder(folder_name)
                        if folder_id:
                            st.session_state.google_drive_folder_id = folder_id
                            st.info(f"📁 Created folder: {folder_name}")
                    else:
                        st.error("❌ Failed to connect to Google Drive")
            
            if st.session_state.google_drive_auth:
                st.success("✅ Google Drive: Connected")
                if st.session_state.google_drive_folder_id:
                    st.info(f"📁 Folder ID: {st.session_state.google_drive_folder_id[:20]}...")
        else:
            st.warning("Google Drive API not available. Install: pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client")
        
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
            placeholder="e.g., 'GDP growth 2023', 'population Lagos', 'unemployment rate'",
            key="search_input"
        )
        
        # Quick search buttons
        st.subheader("🚀 Quick Statistical Searches")
        
        quick_searches = [
            "GDP Nigeria 2023",
            "Population Census",
            "Unemployment Rate",
            "Inflation Rate",
            "Crime Statistics",
            "Health Indicators",
            "Education Statistics",
            "Oil Production"
        ]
        
        cols = st.columns(3)
        for i, query in enumerate(quick_searches):
            with cols[i % 3]:
                if st.button(f"🔎 {query}", use_container_width=True, key=f"quick_{i}"):
                    st.session_state.search_query = query
                    st.rerun()
    
    with col2:
        st.subheader("🎯 Available Sources")
        st.write("**Nigerian Data Sources:**")
        st.write("• National Bureau of Statistics")
        st.write("• Central Bank of Nigeria")
        st.write("• World Bank Nigeria Data")
        st.write("• IMF Nigeria Reports")
        st.write("• WHO Nigeria Data")
        st.write("• UN Data Nigeria")
        
        if st.button("📋 View All Sources", key="view_sources"):
            st.session_state.show_sources = True
    
    # Show all sources if requested
    if st.session_state.get('show_sources', False):
        st.subheader("📚 Complete List of Data Sources")
        scraper = NigerianStatsScraper(logger=logger)
        websites = scraper.get_nigerian_statistical_websites()
        
        for website in websites:
            with st.expander(f"{website['name']} - {website['category']}"):
                st.write(f"**URL:** {website['url']}")
                st.write(f"**Method:** {website['scrape_method']}")
                st.write(f"**Priority:** {website['priority']}")
    
    # Scrape button
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.button("🚀 Start Multi-Website Scraping", type="primary", use_container_width=True, key="scrape_button"):
            if not search_query and 'search_query' not in st.session_state:
                st.warning("⚠️ Please enter a search query or select a quick search")
            else:
                query = search_query or getattr(st.session_state, 'search_query', '')
                
                st.session_state.scraping_in_progress = True
                st.session_state.scraping_log = []  # Clear previous log
                
                with st.spinner(f"🌐 Scraping {max_websites} websites for '{query}'..."):
                    # Initialize scraper with thread-safe logger
                    scraper = NigerianStatsScraper(
                        max_workers=max_workers,
                        use_selenium=use_selenium,
                        logger=logger
                    )
                    
                    # Show scraping progress
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # Start scraping
                    status_text.text("Initializing scrapers...")
                    progress_bar.progress(10)
                    
                    # Perform the actual scraping
                    status_text.text("Scraping multiple websites...")
                    progress_bar.progress(30)
                    
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
                        
                        status_text.text("✅ Multi-website scraping completed!")
                        progress_bar.progress(100)
                        
                        st.success(f"🎉 Successfully scraped {len(scraped_data)} records!")
                        
                        # Save to Google Drive if enabled
                        if GOOGLE_DRIVE_AVAILABLE and st.session_state.get('google_drive_auth') and st.session_state.get('google_drive_folder_id'):
                            with st.spinner("📤 Uploading to Google Drive..."):
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                file_name = f"nigeria_stats_{query.replace(' ', '_')}_{timestamp}"
                                
                                result = st.session_state.google_drive_auth.upload_dataframe(
                                    scraped_data,
                                    file_name,
                                    st.session_state.google_drive_folder_id,
                                    export_format.lower()
                                )
                                
                                if result:
                                    st.success(f"✅ File uploaded to Google Drive!")
                                    st.info(f"📁 **File:** {result['file_name']}")
                                    st.info(f"🔗 **Link:** [Open in Google Drive]({result['web_link']})")
                        
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
                        st.warning("⚠️ No data found from the selected websites.")
                        st.info("💡 Try different search terms or select different categories.")
                    
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
                    if "Success" in log_entry_text or "found" in log_entry_text.lower():
                        st.success(log_entry_text)
                    elif "Error" in log_entry_text or "failed" in log_entry_text.lower():
                        st.error(log_entry_text)
                    elif "Warning" in log_entry_text:
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
            st.metric("Websites Scraped", sources)
        with col3:
            categories = df['Category'].nunique() if 'Category' in df.columns else 0
            st.metric("Data Categories", categories)
        
        # Data preview with tabs
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Data Preview", "🔍 Filter Data", "📈 Statistics", "☁️ Google Drive"])
        
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
        
        with tab4:
            st.subheader("Google Drive Upload")
            
            if GOOGLE_DRIVE_AVAILABLE:
                if not st.session_state.get('google_drive_auth'):
                    st.warning("⚠️ Please connect to Google Drive first")
                    if st.button("🔗 Connect to Google Drive", key="connect_drive_tab"):
                        with st.spinner("Connecting..."):
                            drive_manager = GoogleDriveManager()
                            if drive_manager.authenticate():
                                st.session_state.google_drive_auth = drive_manager
                                st.rerun()
                else:
                    st.success("✅ Connected to Google Drive")
                    
                    # File name input
                    drive_filename = st.text_input(
                        "File name for Google Drive:",
                        value=f"nigeria_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        key="drive_filename"
                    )
                    
                    # Format selection
                    drive_format = st.selectbox(
                        "File format:",
                        ["CSV", "JSON", "Excel"],
                        key="drive_format"
                    )
                    
                    # Folder selection
                    folder_name = st.text_input(
                        "Folder name (optional, creates if doesn't exist):",
                        value=f"Nigeria_Stats_{datetime.now().strftime('%Y%m')}",
                        key="drive_folder"
                    )
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if st.button("📤 Upload to Google Drive", use_container_width=True, key="upload_drive"):
                            with st.spinner("Uploading to Google Drive..."):
                                # Get or create folder
                                folder_id = st.session_state.google_drive_auth.get_folder_id_by_name(folder_name)
                                if not folder_id:
                                    folder_id = st.session_state.google_drive_auth.create_folder(folder_name)
                                
                                if folder_id:
                                    result = st.session_state.google_drive_auth.upload_dataframe(
                                        df,
                                        drive_filename,
                                        folder_id,
                                        drive_format.lower()
                                    )
                                    
                                    if result:
                                        st.success(f"✅ File uploaded to Google Drive!")
                                        st.info(f"📁 **Folder:** {folder_name}")
                                        st.info(f"📄 **File:** {result['file_name']}")
                                        st.markdown(f"🔗 **Link:** [Open in Google Drive]({result['web_link']})", unsafe_allow_html=True)
                                    else:
                                        st.error("❌ Failed to upload file")
                                else:
                                    st.error("❌ Failed to create folder")
                    
                    with col2:
                        if st.button("📋 List Drive Files", use_container_width=True, key="list_drive"):
                            with st.spinner("Loading files..."):
                                files = st.session_state.google_drive_auth.list_files()
                                if files:
                                    st.write(f"📁 **Found {len(files)} files:**")
                                    for file in files[:10]:  # Show first 10
                                        st.write(f"- {file['name']} ({file['mimeType']})")
                                else:
                                    st.info("No files found in Google Drive")
            else:
                st.warning("Google Drive API not available. Install required libraries.")
        
        # Export section
        st.header("💾 Export Data")
        
        export_filename = st.text_input(
            "Export filename:",
            value=f"nigeria_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            key="export_filename"
        )
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("📥 Download CSV", use_container_width=True, key="export_csv"):
                csv_link = create_download_link(df, f"{export_filename}.csv", "csv")
                st.markdown(csv_link, unsafe_allow_html=True)
        
        with col2:
            if st.button("📥 Download JSON", use_container_width=True, key="export_json"):
                json_link = create_download_link(df, f"{export_filename}.json", "json")
                st.markdown(json_link, unsafe_allow_html=True)
        
        with col3:
            if st.button("📥 Download Excel", use_container_width=True, key="export_excel"):
                os.makedirs("scraped_data", exist_ok=True)
                excel_path = f"scraped_data/{export_filename}.xlsx"
                df.to_excel(excel_path, index=False)
                with open(excel_path, "rb") as f:
                    excel_bytes = f.read()
                st.download_button(
                    label="Download Excel",
                    data=excel_bytes,
                    file_name=f"{export_filename}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
        
        with col4:
            if st.button("💾 Save Locally", use_container_width=True, key="save_local"):
                os.makedirs("scraped_data", exist_ok=True)
                filepath = f"scraped_data/{export_filename}.csv"
                df.to_csv(filepath, index=False)
                st.success(f"✅ Data saved to: {filepath}")

# Footer
st.markdown("---")
st.markdown("""
    <div style="text-align: center; color: #666; padding: 2rem 0;">
        <p><strong>Nigeria Statistics Web Scraper</strong> • Version 3.0</p>
        <p>🌐 <strong>Multi-website scraping</strong> with Google Drive integration</p>
        <p>☁️ <strong>Google Drive:</strong> Save files directly to your cloud storage</p>
        <p>⚡ <strong>Concurrent scraping</strong> with thread-safe logging</p>
        <p>⚠️ <strong>Note:</strong> Respect website terms of service. Use responsibly.</p>
        <p>🛠️ <strong>Technologies:</strong> Python, BeautifulSoup, Google Drive API, Streamlit</p>
    </div>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    # Create necessary directories
    os.makedirs("static", exist_ok=True)
    os.makedirs("scraped_data", exist_ok=True)
    main()