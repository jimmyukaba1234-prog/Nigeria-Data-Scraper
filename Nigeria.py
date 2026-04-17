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
from urllib.parse import urljoin, urlparse, parse_qs
import numpy as np
import concurrent.futures
import io
import threading
from queue import Queue
import pickle
import webbrowser
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from collections import deque
import hashlib
import zipfile
from pathlib import Path
import socket
import urllib3

# Disable SSL warnings for problematic sites
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Try to import PDF libraries with error handling
PDF_LIBRARIES_AVAILABLE = False
try:
    # Basic PDF libraries
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
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 1rem;
        font-weight: bold;
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
        transition: all 0.3s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(0,0,0,0.2);
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
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .pdf-card {
        border: 1px solid #ddd;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        background: #f9f9f9;
    }
    .stats-header {
        color: #2E4053;
        font-size: 1.2rem;
        font-weight: bold;
        margin-top: 1rem;
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
    """Manage Google Drive integration with PDF upload support"""
    
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
    
    def upload_pdf_file(self, pdf_path, folder_id=None):
        """Upload a PDF file to Google Drive"""
        try:
            if not os.path.exists(pdf_path):
                st.error(f"PDF file not found: {pdf_path}")
                return None
            
            file_name = os.path.basename(pdf_path)
            
            file_metadata = {'name': file_name}
            if folder_id:
                file_metadata['parents'] = [folder_id]
            
            media = MediaFileUpload(
                pdf_path,
                mimetype='application/pdf',
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
                'file_name': file_name,
                'mime_type': 'application/pdf'
            }
            
        except Exception as e:
            st.error(f"Error uploading PDF to Google Drive: {e}")
            return None
    
    def upload_multiple_pdfs(self, pdf_paths, folder_id=None):
        """Upload multiple PDF files to Google Drive"""
        results = []
        
        for pdf_path in pdf_paths:
            if os.path.exists(pdf_path):
                result = self.upload_pdf_file(pdf_path, folder_id)
                if result:
                    results.append(result)
        
        return results
    
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
    
    def list_files(self, folder_id=None, mime_type=None):
        """List files in Google Drive with optional mime type filter"""
        try:
            query = "trashed=false"
            if folder_id:
                query = f"'{folder_id}' in parents and trashed=false"
            if mime_type:
                query += f" and mimeType='{mime_type}'"
            
            results = self.service.files().list(
                q=query,
                pageSize=100,
                fields="files(id, name, mimeType, createdTime, modifiedTime, size, webViewLink)"
            ).execute()
            
            return results.get('files', [])
        except Exception as e:
            st.error(f"Error listing files: {e}")
            return []
    
    def list_pdf_files(self, folder_id=None):
        """List only PDF files in Google Drive"""
        return self.list_files(folder_id, mime_type='application/pdf')
    
    def get_folder_id_by_name(self, folder_name):
        """Get folder ID by name"""
        try:
            # Search for folder with the given name
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
    
    def create_pdf_folder_structure(self, base_folder_name, subfolders=None):
        """Create a hierarchical folder structure for PDFs"""
        try:
            # Create base folder
            base_folder_id = self.create_folder(base_folder_name)
            
            if not base_folder_id:
                return None
            
            folder_structure = {'base': base_folder_id}
            
            # Create subfolders if specified
            if subfolders:
                for subfolder_name in subfolders:
                    subfolder_id = self.create_folder(subfolder_name, base_folder_id)
                    if subfolder_id:
                        folder_structure[subfolder_name] = subfolder_id
            
            return folder_structure
            
        except Exception as e:
            st.error(f"Error creating folder structure: {e}")
            return None

# PDF Upload Helper Functions
def upload_pdfs_to_drive(pdf_dataframe, drive_manager, folder_id=None, category_folders=True):
    """Upload all PDFs from the scraped data to Google Drive"""
    if pdf_dataframe is None or pdf_dataframe.empty:
        return []
    
    results = []
    
    if category_folders and 'Source_Category' in pdf_dataframe.columns:
        # Group PDFs by category and create subfolders
        for category in pdf_dataframe['Source_Category'].unique():
            category_pdfs = pdf_dataframe[pdf_dataframe['Source_Category'] == category]
            
            # Create category folder
            category_folder_name = f"PDFs_{category.replace(' ', '_')}"
            category_folder_id = drive_manager.create_folder(category_folder_name, folder_id)
            
            if category_folder_id:
                for _, row in category_pdfs.iterrows():
                    local_path = row.get('Local_Path')
                    if local_path and os.path.exists(local_path):
                        result = drive_manager.upload_pdf_file(local_path, category_folder_id)
                        if result:
                            result['category'] = category
                            result['original_file'] = row.get('File_Name', '')
                            results.append(result)
    else:
        # Upload all PDFs to the same folder
        for _, row in pdf_dataframe.iterrows():
            local_path = row.get('Local_Path')
            if local_path and os.path.exists(local_path):
                result = drive_manager.upload_pdf_file(local_path, folder_id)
                if result:
                    result['original_file'] = row.get('File_Name', '')
                    results.append(result)
    
    return results

def create_drive_pdf_summary(upload_results):
    """Create a summary DataFrame of uploaded PDFs"""
    if not upload_results:
        return None
    
    summary_data = []
    for result in upload_results:
        summary_data.append({
            'File Name': result.get('file_name', ''),
            'Category': result.get('category', 'Uncategorized'),
            'Drive Link': result.get('web_link', ''),
            'File ID': result.get('file_id', ''),
            'Original File': result.get('original_file', '')
        })
    
    return pd.DataFrame(summary_data)

def auto_upload_to_drive(scraper_results, drive_manager, folder_id, upload_pdfs=True, upload_data=True):
    """Automatically upload results to Google Drive after scraping"""
    upload_summary = {
        'data': None,
        'pdfs': []
    }
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Upload data if available
    if upload_data and scraper_results.get('data') is not None:
        data_result = drive_manager.upload_dataframe(
            scraper_results['data'],
            f"nigeria_stats_{timestamp}",
            folder_id,
            'csv'
        )
        upload_summary['data'] = data_result
    
    # Upload PDFs if available
    if upload_pdfs and scraper_results.get('pdfs') is not None and not scraper_results['pdfs'].empty:
        # Create PDFs subfolder
        pdf_folder_name = f"PDFs_{timestamp}"
        pdf_folder_id = drive_manager.create_folder(pdf_folder_name, folder_id)
        
        if pdf_folder_id:
            pdf_results = upload_pdfs_to_drive(
                scraper_results['pdfs'],
                drive_manager,
                pdf_folder_id,
                category_folders=True
            )
            upload_summary['pdfs'] = pdf_results
    
    return upload_summary

class NigerianStatsScraper:
    """Enhanced web scraper for Nigerian statistical data with multi-website support"""
    
    def __init__(self, max_workers=5, use_selenium=False, logger=None, timeout=30, verify_ssl=True):
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        
        # Add retry strategy
        retry_strategy = urllib3.Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        self.max_workers = max_workers
        self.use_selenium = use_selenium and SELENIUM_AVAILABLE
        self.driver = None
        self.logger = logger
        self.failed_pdfs = []
    
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
            chrome_options.add_argument("--ignore-certificate-errors")  # Ignore SSL errors in Chrome
            chrome_options.add_argument("--allow-insecure-localhost")
            chrome_options.add_argument("--disable-web-security")
            
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
    
    def scrape_pdf(self, url, save_folder="downloaded_pdfs", max_retries=3):
        """
        Download PDF file with enhanced error handling for 403 errors
        Returns metadata about the downloaded file
        """
        pdf_data = []
        
        # Check if URL is None or empty
        if not url:
            return pdf_data

        try:
            self.log(f"Downloading PDF: {url}")

            # Try different user agents and headers for 403 errors
            headers_list = [
                {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Referer': urlparse(url).scheme + '://' + urlparse(url).netloc,
                },
                {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
                    'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                },
                {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
                    'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                }
            ]
            
            for attempt in range(max_retries):
                try:
                    # Rotate headers
                    self.session.headers.update(headers_list[attempt % len(headers_list)])
                    
                    # Try with SSL verification first
                    response = self.session.get(
                        url, 
                        timeout=self.timeout, 
                        verify=self.verify_ssl,
                        allow_redirects=True
                    )
                    
                    # Check if response was successful
                    response.raise_for_status()

                    # Check content type
                    content_type = response.headers.get("content-type", "").lower()
                    
                    if response.status_code == 200:
                        # Check if it's a PDF or if URL ends with .pdf
                        if "application/pdf" in content_type or ".pdf" in url.lower():

                            # Create folder if it doesn't exist
                            os.makedirs(save_folder, exist_ok=True)

                            # Generate safe filename
                            file_name = url.split("/")[-1].split("?")[0]
                            if not file_name.lower().endswith(".pdf"):
                                file_name = f"{file_name}.pdf"
                            
                            # Sanitize filename
                            file_name = re.sub(r'[^\w\-_\. ]', '_', file_name)

                            file_path = os.path.join(save_folder, file_name)

                            # Save file
                            with open(file_path, "wb") as f:
                                f.write(response.content)

                            file_size = len(response.content)
                            self.log(f"PDF saved: {file_path} ({file_size/1024:.1f} KB)")

                            pdf_data.append({
                                "Content_Type": "PDF_File",
                                "PDF_URL": url,
                                "Local_Path": file_path,
                                "File_Name": file_name,
                                "File_Size_KB": round(file_size / 1024, 2),
                                "Scrape_Date": datetime.now().strftime("%Y-%m-%d")
                            })
                            break  # Success, exit retry loop
                        else:
                            self.log(f"URL is not a PDF or returned wrong content type: {content_type}")
                            break
                            
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 403:
                        self.log(f"403 Forbidden for {url} (attempt {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            time.sleep(2 ** attempt)  # Exponential backoff
                        else:
                            self.failed_pdfs.append({
                                'url': url,
                                'error': '403 Forbidden',
                                'attempts': max_retries
                            })
                            self.log(f"Failed to download PDF after {max_retries} attempts: {url}")
                    else:
                        raise
                        
                except requests.exceptions.SSLError as e:
                    self.log(f"SSL Error downloading PDF {url}: {e}")
                    if attempt < max_retries - 1:
                        # Try with SSL verification disabled
                        self.verify_ssl = False
                        time.sleep(1)
                    else:
                        self.failed_pdfs.append({
                            'url': url,
                            'error': 'SSL Error',
                            'attempts': attempt + 1
                        })
                        
                except requests.exceptions.ConnectionError as e:
                    self.log(f"Connection error for {url}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                    else:
                        self.failed_pdfs.append({
                            'url': url,
                            'error': 'Connection Error',
                            'attempts': attempt + 1
                        })
                        
                except requests.exceptions.Timeout as e:
                    self.log(f"Timeout for {url}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                    else:
                        self.failed_pdfs.append({
                            'url': url,
                            'error': 'Timeout',
                            'attempts': attempt + 1
                        })

        except requests.exceptions.RequestException as e:
            self.log(f"Request error downloading PDF {url}: {e}")
            self.failed_pdfs.append({
                'url': url,
                'error': str(e)[:100],
                'attempts': max_retries
            })
        except Exception as e:
            self.log(f"Error downloading PDF {url}: {e}")
            self.failed_pdfs.append({
                'url': url,
                'error': str(e)[:100],
                'attempts': max_retries
            })

        return pdf_data
    
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
    
    def scrape_with_requests(self, url, search_query, retry_count=3):
        """Scrape website using requests library with enhanced error handling"""
        data = []
        
        for attempt in range(retry_count):
            try:
                # Try with SSL verification first
                try:
                    response = self.session.get(
                        url, 
                        timeout=self.timeout, 
                        verify=self.verify_ssl,
                        allow_redirects=True
                    )
                except requests.exceptions.SSLError as e:
                    self.log(f"SSL Error for {url}, retrying with verification disabled: {e}")
                    # Retry with SSL verification disabled
                    response = self.session.get(
                        url, 
                        timeout=self.timeout, 
                        verify=False,
                        allow_redirects=True
                    )
                except requests.exceptions.ConnectionError as e:
                    self.log(f"Connection error for {url} (attempt {attempt + 1}/{retry_count}): {e}")
                    if attempt < retry_count - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    else:
                        raise
                except requests.exceptions.Timeout as e:
                    self.log(f"Timeout for {url} (attempt {attempt + 1}/{retry_count}): {e}")
                    if attempt < retry_count - 1:
                        time.sleep(2 ** attempt)
                        continue
                    else:
                        raise
                
                if response.status_code == 200:
                    # Check content type
                    content_type = response.headers.get('content-type', '').lower()
                    
                    if 'application/pdf' in content_type:
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
                        
                        # Check for embedded PDF links in HTML
                        if PDF_LIBRARIES_AVAILABLE:
                            pdf_links = soup.find_all('a', href=True)
                            pdf_count = 0
                            for link in pdf_links:
                                href = link['href']
                                if '.pdf' in href.lower():
                                    pdf_url = urljoin(url, href)
                                    # Limit number of PDFs per page to avoid too many requests
                                    if pdf_count >= 5:
                                        break
                                    pdf_data = self.scrape_pdf(pdf_url)
                                    data.extend(pdf_data)
                                    pdf_count += 1
                    
                    elif 'text/plain' in content_type:
                        # Handle plain text
                        text_data = response.text
                        data.extend(self.parse_text_data(text_data, url))
                    
                    break  # Success, exit retry loop
                    
                elif response.status_code == 403:
                    self.log(f"403 Forbidden for {url} (attempt {attempt + 1}/{retry_count})")
                    if attempt < retry_count - 1:
                        # Rotate user agent
                        self.session.headers.update({
                            'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{120 + attempt}.0.0.0 Safari/537.36'
                        })
                        time.sleep(2 ** attempt)
                    else:
                        self.log(f"Failed to access {url} after {retry_count} attempts")
                        
                else:
                    self.log(f"HTTP {response.status_code} for {url}")
                    break

            except requests.exceptions.SSLError as e:
                self.log(f"SSL Error for {url} even after retry: {e}")
                break
            except Exception as e:
                self.log(f"Error in requests scraping for {url}: {str(e)}")
                break
        
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
        """Scrape data from APIs with SSL error handling"""
        data = []
        
        try:
            # Try with SSL verification first
            try:
                response = self.session.get(url, timeout=self.timeout, verify=self.verify_ssl)
            except requests.exceptions.SSLError as e:
                self.log(f"SSL Error for API {url}, retrying with verification disabled: {e}")
                response = self.session.get(url, timeout=self.timeout, verify=False)
            
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
                'url': 'https://nigerianstat.gov.ng/',
                'scrape_method': 'direct',
                'category': 'Official Statistics',
                'priority': 1
            },
            {
                'name': 'NBS e-Library',
                'url': 'https://nigerianstat.gov.ng/elibrary',
                'scrape_method': 'direct',
                'category': 'Official Statistics',
                'priority': 2
            },
            {
                'name': 'NBS Publications',
                'url': 'https://nigerianstat.gov.ng/elibrary/publications',
                'scrape_method': 'direct',
                'category': 'Official Statistics',
                'priority': 2
            },
            {
                'name': 'NBS Reports',
                'url': 'https://nigerianstat.gov.ng/elibrary/reports',
                'scrape_method': 'direct',
                'category': 'Official Statistics',
                'priority': 2
            },
            {
                'name': 'NBS CPI Reports',
                'url': 'https://nigerianstat.gov.ng/elibrary/cpi',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 3
            },
            {
                'name': 'NBS GDP Reports',
                'url': 'https://nigerianstat.gov.ng/elibrary/gdp',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 3
            },
            {
                'name': 'NBS Labor Statistics',
                'url': 'https://nigerianstat.gov.ng/elibrary/labour',
                'scrape_method': 'direct',
                'category': 'Labor Statistics',
                'priority': 3
            },
            {
                'name': 'Central Bank of Nigeria (CBN)',
                'url': 'https://www.cbn.gov.ng/',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 4
            },
            {
                'name': 'CBN Statistics',
                'url': 'https://www.cbn.gov.ng/rates/',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 4
            },
            {
                'name': 'Nigerian Communications Commission (NCC)',
                'url': 'https://www.ncc.gov.ng/',
                'scrape_method': 'direct',
                'category': 'Telecom Statistics',
                'priority': 5
            },
            {
                'name': 'Nigerian Electricity Regulatory Commission',
                'url': 'https://nerc.gov.ng/',
                'scrape_method': 'direct',
                'category': 'Energy Statistics',
                'priority': 5
            },
            {
                'name': 'National Population Commission',
                'url': 'https://nationalpopulation.gov.ng/',
                'scrape_method': 'direct',
                'category': 'Demographic Statistics',
                'priority': 4
            },
            {
                'name': 'World Bank Nigeria',
                'url': 'https://data.worldbank.org/country/nigeria',
                'scrape_method': 'direct',
                'category': 'International Statistics',
                'priority': 6
            },
            {
                'name': 'IMF Nigeria',
                'url': 'https://www.imf.org/en/Countries/NGA',
                'scrape_method': 'direct',
                'category': 'International Statistics',
                'priority': 6
            },
            {
                'name': 'UN Data Nigeria',
                'url': 'http://data.un.org/en/iso/ng.html',
                'scrape_method': 'direct',
                'category': 'International Statistics',
                'priority': 6
            },
            {
                'name': 'WHO Nigeria',
                'url': 'https://www.who.int/countries/nga/',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 5
            },
            {
                'name': 'UNICEF Nigeria',
                'url': 'https://data.unicef.org/country/nga/',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 5
            },
            {
                'name': 'Nigerian Bureau of Statistics (Archive)',
                'url': 'https://nigerianstat.gov.ng/archive',
                'scrape_method': 'direct',
                'category': 'Official Statistics',
                'priority': 3
            },
            {
                'name': 'Nigerian Extractive Industries Initiative',
                'url': 'https://neiti.gov.ng/',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 5
            },
            {
                'name': 'National Agency for Food and Drug Admin',
                'url': 'https://www.nafdac.gov.ng/',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 6
            },
            {
                'name': 'Nigerian Ports Authority',
                'url': 'https://nigerianports.gov.ng/',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 6
            },
            {
                'name': 'National Bureau of Statistics - Surveys',
                'url': 'https://nigerianstat.gov.ng/surveys',
                'scrape_method': 'direct',
                'category': 'Official Statistics',
                'priority': 3
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

class EnhancedNigerianStatsScraper(NigerianStatsScraper):
    """Enhanced scraper with multi-page crawling and PDF downloading capabilities"""
    
    def __init__(self, max_workers=5, use_selenium=False, logger=None, timeout=30, max_pages_per_site=10, verify_ssl=True):
        super().__init__(max_workers, use_selenium, logger, timeout, verify_ssl)
        self.max_pages_per_site = max_pages_per_site
        self.visited_urls = set()
        self.pdf_urls = set()
        self.downloaded_pdfs = []
        self.failed_urls = []
        self.crawl_stats = {
            'pages_visited': 0,
            'pdfs_found': 0,
            'pdfs_downloaded': 0,
            'pdfs_failed': 0,
            'failed_urls': [],
            'start_time': None,
            'end_time': None
        }
    
    def is_valid_url(self, url, base_domain):
        """Check if URL is valid and belongs to the same domain"""
        try:
            parsed = urlparse(url)
            # Allow same domain or subdomains
            return (parsed.netloc == base_domain or parsed.netloc.endswith('.' + base_domain) or not parsed.netloc) and \
                   parsed.scheme in ['http', 'https', ''] and \
                   not any(ext in parsed.path.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.css', '.js', '.ico'])
        except:
            return False
    
    def extract_links(self, soup, base_url, base_domain):
        """Extract relevant links from page"""
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)
            
            # Check if it's a valid URL and not already visited
            if self.is_valid_url(full_url, base_domain) and full_url not in self.visited_urls:
                # Prioritize pages with statistical content
                link_text = link.get_text().lower()
                priority_keywords = ['statistic', 'data', 'report', 'publication', 
                                   'table', 'figure', 'chart', 'indicator', 'release',
                                   'census', 'survey', 'economic', 'population', 'gdp',
                                   'inflation', 'unemployment', 'trade', 'budget',
                                   'annual', 'quarterly', 'monthly', 'report']
                
                # Check if link contains .pdf
                if '.pdf' in full_url.lower():
                    if full_url not in self.pdf_urls:
                        self.pdf_urls.add(full_url)
                        links.insert(0, full_url)  # PDFs get highest priority
                elif any(keyword in link_text for keyword in priority_keywords):
                    links.insert(0, full_url)  # High priority for statistical content
                else:
                    # Only add if it's not too long and doesn't look like a navigation link
                    if len(full_url) < 200 and not any(nav in link_text for nav in ['login', 'signup', 'register', 'contact']):
                        links.append(full_url)  # Normal priority
        
        return links[:30]  # Limit to 30 links per page
    
    def crawl_website(self, start_url, search_query, max_depth=2, download_pdfs=True):
        """Crawl multiple pages of a website with enhanced error handling"""
        all_data = []
        base_domain = urlparse(start_url).netloc
        urls_to_visit = deque([(start_url, 0)])  # (url, depth)
        page_count = 0
        consecutive_failures = 0
        
        self.log(f"Starting crawl of {base_domain} with max depth {max_depth}")
        
        while urls_to_visit and page_count < self.max_pages_per_site:
            current_url, depth = urls_to_visit.popleft()
            
            if current_url in self.visited_urls:
                continue
                
            self.visited_urls.add(current_url)
            page_count += 1
            self.crawl_stats['pages_visited'] += 1
            
            self.log(f"Crawling page {page_count}/{self.max_pages_per_site} (depth {depth}): {current_url}")
            
            try:
                # Check if it's a PDF URL
                if '.pdf' in current_url.lower() and download_pdfs:
                    pdf_data = self.scrape_pdf(current_url)
                    if pdf_data:
                        for pdf in pdf_data:
                            pdf['Page_URL'] = current_url
                            pdf['Page_Depth'] = depth
                            pdf['Source_Website'] = base_domain
                        all_data.extend(pdf_data)
                        self.downloaded_pdfs.extend(pdf_data)
                        self.crawl_stats['pdfs_downloaded'] += len(pdf_data)
                        consecutive_failures = 0
                    else:
                        self.crawl_stats['pdfs_failed'] += 1
                        consecutive_failures += 1
                    continue
                
                # Scrape current page
                page_data = self.scrape_with_requests(current_url, search_query)
                
                if page_data:
                    # Add depth and page information
                    for item in page_data:
                        item['Page_Depth'] = depth
                        item['Page_URL'] = current_url
                    
                    all_data.extend(page_data)
                    consecutive_failures = 0
                    
                    # Track PDFs found on this page
                    pdfs = [item for item in page_data if item.get('Content_Type') == 'PDF_File']
                    for pdf in pdfs:
                        pdf_url = pdf.get('PDF_URL')
                        if pdf_url and pdf_url not in self.pdf_urls:
                            self.pdf_urls.add(pdf_url)
                            self.crawl_stats['pdfs_found'] += 1
                else:
                    consecutive_failures += 1
                
                # If too many consecutive failures, slow down
                if consecutive_failures > 3:
                    self.log(f"Too many consecutive failures, waiting longer...")
                    time.sleep(5)
                    consecutive_failures = 0
                
                # If not at max depth, extract and queue more links
                if depth < max_depth and page_count < self.max_pages_per_site and consecutive_failures < 5:
                    try:
                        # Handle SSL errors for link extraction too
                        try:
                            response = self.session.get(
                                current_url, 
                                timeout=self.timeout, 
                                verify=self.verify_ssl,
                                allow_redirects=True
                            )
                        except requests.exceptions.SSLError:
                            response = self.session.get(
                                current_url, 
                                timeout=self.timeout, 
                                verify=False,
                                allow_redirects=True
                            )
                        except requests.exceptions.ConnectionError:
                            self.log(f"Connection error for {current_url}, skipping link extraction")
                            continue
                        except requests.exceptions.Timeout:
                            self.log(f"Timeout for {current_url}, skipping link extraction")
                            continue
                            
                        if response.status_code == 200 and 'text/html' in response.headers.get('content-type', '').lower():
                            soup = BeautifulSoup(response.content, 'html.parser')
                            new_links = self.extract_links(soup, current_url, base_domain)
                            
                            for link in new_links:
                                if link not in [u[0] for u in urls_to_visit] and link not in self.visited_urls:
                                    urls_to_visit.append((link, depth + 1))
                    except Exception as e:
                        self.log(f"Error extracting links from {current_url}: {str(e)}")
                        self.failed_urls.append(current_url)
                
                # Be respectful to the server
                time.sleep(1)
                
            except Exception as e:
                self.log(f"Error crawling {current_url}: {str(e)}")
                self.failed_urls.append(current_url)
                consecutive_failures += 1
        
        # Update crawl stats with failed URLs
        self.crawl_stats['failed_urls'] = self.failed_urls
        self.crawl_stats['pdfs_failed'] = len(self.failed_pdfs)
        
        self.log(f"Completed crawl of {base_domain}: {len(all_data)} records, {len(self.downloaded_pdfs)} PDFs, {len(self.failed_pdfs)} failed PDFs")
        return all_data
    
    def enhanced_multi_website_scrape(self, search_query, selected_categories=None, max_websites=8, max_depth=2, download_pdfs=True):
        """Enhanced multi-website scraping with crawling and error tracking"""
        all_data = []
        all_pdfs = []
        
        self.crawl_stats['start_time'] = datetime.now()
        self.failed_pdfs = []  # Reset failed PDFs list
        
        # Get websites
        all_websites = self.get_nigerian_statistical_websites()
        
        # Filter by categories
        if selected_categories:
            filtered_websites = [w for w in all_websites 
                               if w.get('category') in selected_categories]
        else:
            filtered_websites = all_websites
        
        # Sort by priority and limit
        filtered_websites.sort(key=lambda x: x.get('priority', 99))
        websites_to_scrape = filtered_websites[:max_websites]
        
        self.log(f"Starting enhanced multi-website scrape: {len(websites_to_scrape)} websites")
        
        # Use ThreadPoolExecutor for concurrent crawling
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(self.max_workers, len(websites_to_scrape))) as executor:
            future_to_website = {}
            
            for website in websites_to_scrape:
                future = executor.submit(
                    self.crawl_website, 
                    website['url'], 
                    search_query,
                    max_depth,
                    download_pdfs
                )
                future_to_website[future] = website
            
            completed = 0
            total = len(websites_to_scrape)
            
            for future in concurrent.futures.as_completed(future_to_website):
                website = future_to_website[future]
                completed += 1
                
                try:
                    website_data = future.result()
                    if website_data:
                        # Add source information
                        for item in website_data:
                            item['Source_Website'] = website['name']
                            item['Source_Category'] = website.get('category', 'Unknown')
                        
                        # Separate PDFs from regular data
                        pdfs = [item for item in website_data if item.get('Content_Type') == 'PDF_File']
                        regular_data = [item for item in website_data if item.get('Content_Type') != 'PDF_File']
                        
                        all_data.extend(regular_data)
                        all_pdfs.extend(pdfs)
                        
                        self.log(f"({completed}/{total}) {website['name']}: {len(regular_data)} records, {len(pdfs)} PDFs")
                    else:
                        self.log(f"({completed}/{total}) {website['name']}: No data found")
                except Exception as e:
                    self.log(f"({completed}/{total}) Error scraping {website['name']}: {str(e)}")
        
        self.crawl_stats['end_time'] = datetime.now()
        
        # Update stats with failed PDFs
        if self.failed_pdfs:
            self.crawl_stats['pdfs_failed'] = len(self.failed_pdfs)
            self.crawl_stats['failed_pdfs'] = self.failed_pdfs
        
        self.log(f"Enhanced scraping complete. Total records: {len(all_data)}, Total PDFs: {len(all_pdfs)}, Failed PDFs: {len(self.failed_pdfs)}")
        
        # Create DataFrames
        result = {
            'data': None,
            'pdfs': None,
            'failed_pdfs': self.failed_pdfs,
            'stats': self.crawl_stats
        }
        
        if all_data:
            df = pd.DataFrame(all_data)
            df = df.drop_duplicates()
            result['data'] = df
        
        if all_pdfs:
            pdf_df = pd.DataFrame(all_pdfs)
            if 'PDF_URL' in pdf_df.columns:
                pdf_df = pdf_df.drop_duplicates(subset=['PDF_URL'])
            result['pdfs'] = pdf_df
        
        return result

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
    elif file_type == "html":
        html_str = df.to_html()
        b64 = base64.b64encode(html_str.encode()).decode()
        href = f'<a href="data:text/html;base64,{b64}" download="{filename}" target="_blank">📥 Download {filename}</a>'
        return href

def create_pdf_download(df, filename):
    """Create PDF report from DataFrame"""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    elements = []

    styles = getSampleStyleSheet()
    
    # Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1E3A8A'),
        spaceAfter=30,
        alignment=1  # Center alignment
    )
    elements.append(Paragraph("Nigeria Statistics Scraped Data", title_style))
    
    # Date
    date_style = ParagraphStyle(
        'DateStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.grey,
        alignment=1,
        spaceAfter=20
    )
    elements.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", date_style))
    
    # Summary
    summary_style = ParagraphStyle(
        'SummaryStyle',
        parent=styles['Normal'],
        fontSize=12,
        spaceAfter=20
    )
    elements.append(Paragraph(f"Total Records: {len(df)}", summary_style))
    elements.append(Spacer(1, 0.2 * inch))
    
    # Data table (first 50 rows)
    data = [df.columns.tolist()] + df.head(50).values.tolist()
    
    # Adjust column widths
    col_widths = [2.5 * inch] * len(df.columns)
    
    table = Table(data, colWidths=col_widths)
    table.setStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1E3A8A')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
    ])
    
    elements.append(table)
    
    doc.build(elements)
    buffer.seek(0)
    
    return buffer

def create_pdf_bundle_download(pdf_dataframe):
    """Create a downloadable bundle of all PDFs"""
    if pdf_dataframe is None or pdf_dataframe.empty:
        return None
    
    # Create a ZIP file containing all PDFs
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        for idx, row in pdf_dataframe.iterrows():
            local_path = row.get('Local_Path')
            if local_path and os.path.exists(local_path):
                arcname = row.get('File_Name', f'pdf_{idx}.pdf')
                zip_file.write(local_path, arcname)
    
    zip_buffer.seek(0)
    return zip_buffer

def display_pdf_card(pdf_row, index, show_drive_option=True):
    """Display a PDF card in the UI with optional Drive upload"""
    col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
    
    with col1:
        file_name = pdf_row.get('File_Name', f'PDF_{index}.pdf')
        file_size = pdf_row.get('File_Size_KB', 'Unknown')
        source = pdf_row.get('Source_Website', 'Unknown')
        st.markdown(f"**📄 {file_name}**")
        st.markdown(f"*{source}* • {file_size} KB")
    
    with col2:
        local_path = pdf_row.get('Local_Path')
        if local_path and os.path.exists(local_path):
            with open(local_path, 'rb') as f:
                pdf_bytes = f.read()
            st.download_button(
                label="⬇️ Download",
                data=pdf_bytes,
                file_name=file_name,
                mime="application/pdf",
                key=f"pdf_dl_{index}",
                use_container_width=True
            )
        else:
            st.warning("❌ Not available")
    
    with col3:
        pdf_url = pdf_row.get('PDF_URL')
        if pdf_url:
            st.markdown(f"[🔗 View]({pdf_url})")
    
    with col4:
        st.markdown(f"📅 {pdf_row.get('Scrape_Date', 'N/A')}")
    
    with col5:
        if show_drive_option and GOOGLE_DRIVE_AVAILABLE and st.session_state.get('google_drive_auth'):
            if st.button("☁️ Drive", key=f"drive_btn_{index}", use_container_width=True):
                if local_path and os.path.exists(local_path):
                    with st.spinner("Uploading to Drive..."):
                        result = st.session_state.google_drive_auth.upload_pdf_file(local_path)
                        if result:
                            st.success("✅ Uploaded!")
                            st.markdown(f"[Link]({result['web_link']})")

def main():
    """Main application function"""
    
    # Initialize session state with enhanced features
    if 'scraped_data' not in st.session_state:
        st.session_state.scraped_data = None
    if 'scraped_pdfs' not in st.session_state:
        st.session_state.scraped_pdfs = None
    if 'failed_pdfs' not in st.session_state:
        st.session_state.failed_pdfs = []
    if 'scraping_in_progress' not in st.session_state:
        st.session_state.scraping_in_progress = False
    if 'scraping_log' not in st.session_state:
        st.session_state.scraping_log = []
    if 'google_drive_auth' not in st.session_state:
        st.session_state.google_drive_auth = None
    if 'google_drive_folder_id' not in st.session_state:
        st.session_state.google_drive_folder_id = None
    if 'crawl_stats' not in st.session_state:
        st.session_state.crawl_stats = {}
    if 'selected_categories' not in st.session_state:
        st.session_state.selected_categories = ["Official Statistics", "Economic Statistics"]
    if 'search_history' not in st.session_state:
        st.session_state.search_history = []
    if 'drive_upload_results' not in st.session_state:
        st.session_state.drive_upload_results = []
    
    # Header with Nigerian flag design
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #008753 0%, #008753 33%, #ffffff 33%, #ffffff 66%, #008753 66%, #008753 100%);
        padding: 70px;
        border-radius: 12px;
        text-align: center;
        margin-bottom: 25px;
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.2);
        position: relative;
    ">
        <div style="
            background: rgba(0, 0, 0, 0.7);
            padding: 25px;
            border-radius: 10px;
            display: inline-block;
            backdrop-filter: blur(5px);
        ">
            <div style="font-size:50px; font-weight:700; color: white;">
                🇳🇬 Nigeria Statistics Web Scraper Pro
            </div>
            <div style="font-size:18px; color: #FFFFFF; margin-top: 10px;">
                Multi-source statistical data extraction • Multi-page crawling • PDF Download • Google Drive Integration
            </div>
        </div>
    </div>
""", unsafe_allow_html=True)
    
    # System info
    with st.expander("ℹ️ System Information", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            pdf_status = "✅ Available" if PDF_LIBRARIES_AVAILABLE else "⚠️ Limited"
            st.info(f"**PDF Libraries:** {pdf_status}")
        with col2:
            selenium_status = "✅ Available" if SELENIUM_AVAILABLE else "⚠️ Not available"
            st.info(f"**Selenium:** {selenium_status}")
        with col3:
            drive_status = "✅ Available" if GOOGLE_DRIVE_AVAILABLE else "⚠️ Not available"
            st.info(f"**Google Drive:** {drive_status}")
        with col4:
            st.info(f"**Streamlit:** Version {st.__version__}")
    
    st.markdown("""
        <div style="text-align: center; margin-bottom: 2rem;">
            <p style="font-size: 1.2rem; color: #2E4053;"><strong>Advanced web scraper for Nigerian statistical data with multi-page crawling and PDF downloading</strong></p>
            <p style="color: #566573;">Extracts real data from Nigerian government agencies and international sources • Upload directly to Google Drive</p>
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
            "Labor Statistics",
            "Demographic Statistics",
            "Telecom Statistics",
            "Energy Statistics",
            "General Statistics"
        ]
        
        selected_categories = st.multiselect(
            "Select categories to scrape:",
            categories,
            default=st.session_state.selected_categories,
            key="category_selector"
        )
        st.session_state.selected_categories = selected_categories
        
        # Enhanced crawling options
        st.subheader("🕷️ Crawling Options")
        
        max_pages_per_site = st.slider(
            "Max pages per website", 
            min_value=1, 
            max_value=50, 
            value=15,
            help="Number of pages to crawl per website (higher = more data but slower)"
        )
        
        crawl_depth = st.slider(
            "Crawl depth", 
            min_value=1, 
            max_value=3, 
            value=2,
            help="How deep to follow links (1 = homepage only, 2 = homepage + linked pages, 3 = deeper)"
        )
        
        max_websites = st.slider("Maximum websites to scrape", 1, 30, 12)
        max_workers = st.slider("Concurrent scrapers", 1, 8, 4)
        timeout = st.slider("Timeout per request (seconds)", 10, 60, 30)
        
        if SELENIUM_AVAILABLE:
            use_selenium = st.checkbox("Use Selenium for JavaScript sites", value=False)
        else:
            use_selenium = False
            st.info("ℹ️ Selenium not available - using requests only")
        
        # PDF options
        st.subheader("📄 PDF Options")
        
        download_pdfs = st.checkbox("Download PDF files", value=True)
        
        if download_pdfs:
            pdf_storage = st.radio(
                "PDF storage:",
                ["Save locally", "Memory only (no save)"],
                index=0,
                help="Local saving allows downloading PDFs later"
            )
        
        # Google Drive Configuration
        st.subheader("☁️ Google Drive Options")
        
        auto_save_drive = False
        if GOOGLE_DRIVE_AVAILABLE:
            auto_save_drive = st.checkbox("Auto-save to Google Drive", value=False, help="Automatically upload results to Google Drive after scraping")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("🔗 Connect", use_container_width=True):
                    with st.spinner("Connecting to Google Drive..."):
                        drive_manager = GoogleDriveManager()
                        if drive_manager.authenticate():
                            st.session_state.google_drive_auth = drive_manager
                            st.success("✅ Connected!")
                            
                            # Create or get folder
                            folder_name = f"Nigeria_Stats_{datetime.now().strftime('%Y%m')}"
                            folder_id = drive_manager.create_folder(folder_name)
                            if folder_id:
                                st.session_state.google_drive_folder_id = folder_id
                                st.info(f"📁 Created folder: {folder_name}")
                        else:
                            st.error("❌ Failed to connect")
            
            with col2:
                if st.session_state.google_drive_auth and st.button("📂 List PDFs", use_container_width=True):
                    with st.spinner("Loading PDFs..."):
                        pdfs = st.session_state.google_drive_auth.list_pdf_files()
                        if pdfs:
                            st.info(f"Found {len(pdfs)} PDFs in Drive")
                            for pdf in pdfs[:5]:
                                st.markdown(f"- [{pdf['name']}]({pdf.get('webViewLink', '#')})")
                        else:
                            st.info("No PDFs found")
            
            if st.session_state.google_drive_auth:
                st.success("✅ Google Drive: Connected")
                if st.session_state.google_drive_folder_id:
                    st.info(f"📁 Folder ID: {st.session_state.google_drive_folder_id[:20]}...")
                    
                    # Show PDF upload status
                    if st.session_state.scraped_pdfs is not None:
                        st.metric("PDFs ready for upload", len(st.session_state.scraped_pdfs))
        else:
            st.warning("Google Drive API not available. Install required libraries: pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client")
        
        # Export options
        st.subheader("💾 Export Options")
        export_format = st.selectbox(
            "Export format:",
            ["CSV", "JSON", "Excel", "PDF", "HTML"]
        )
        
        auto_download = st.checkbox("Auto-download after scraping", value=True)
        
        # Search history
        if st.session_state.search_history:
            st.subheader("🕒 Recent Searches")
            for query in st.session_state.search_history[-5:]:
                if st.button(f"🔄 {query}", key=f"history_{query}"):
                    st.session_state.search_input = query
                    st.rerun()
    
    # Main content
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.subheader("🔍 What Nigerian data are you looking for?")
        
        # Search input with placeholder
        search_query = st.text_input(
            "Enter search query:",
            placeholder="e.g., 'GDP growth 2024', 'population census 2023', 'unemployment rate'",
            key="search_input"
        )
        
        # Quick search buttons
        st.subheader("🚀 Quick Statistical Searches")
        
        quick_searches = [
            "GDP Nigeria 2024",
            "Population Census",
            "Unemployment Rate",
            "Inflation Rate",
            "Health Indicators",
            "Education Statistics",
            "Oil Production",
            "Agricultural Output",
            "Trade Statistics",
            "Poverty Rate",
            "Exchange Rate",
            "Foreign Investment"
        ]
        
        cols = st.columns(4)
        for i, query in enumerate(quick_searches):
            with cols[i % 4]:
                if st.button(f"🔎 {query}", use_container_width=True, key=f"quick_{i}"):
                    st.session_state.search_input = query
                    st.rerun()
    
    with col2:
        st.subheader("🎯 Available Sources")
        st.info(f"**{len(EnhancedNigerianStatsScraper().get_nigerian_statistical_websites())}** websites configured")
        st.write("**Top Sources:**")
        st.write("• National Bureau of Statistics")
        st.write("• Central Bank of Nigeria")
        st.write("• World Bank Nigeria")
        st.write("• IMF Nigeria")
        st.write("• UN Data Nigeria")
        
        if st.button("📋 View All Sources", key="view_sources", use_container_width=True):
            st.session_state.show_sources = True
    
    # Show all sources if requested
    if st.session_state.get('show_sources', False):
        st.subheader("📚 Complete List of Data Sources")
        scraper = EnhancedNigerianStatsScraper(logger=logger)
        websites = scraper.get_nigerian_statistical_websites()
        
        # Group by category
        websites_by_category = {}
        for website in websites:
            category = website.get('category', 'Other')
            if category not in websites_by_category:
                websites_by_category[category] = []
            websites_by_category[category].append(website)
        
        for category, sites in websites_by_category.items():
            with st.expander(f"{category} ({len(sites)} sources)"):
                for website in sites:
                    st.markdown(f"""
                    **{website['name']}**  
                    URL: [{website['url']}]({website['url']})  
                    Method: {website['scrape_method']} | Priority: {website['priority']}
                    ---
                    """)
        
        if st.button("Close Sources", key="close_sources"):
            st.session_state.show_sources = False
            st.rerun()
    
    # Scrape button
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        if st.button("🚀 Start Enhanced Multi-Page Scraping", type="primary", use_container_width=True, key="scrape_button"):
            if not search_query and 'search_input' not in st.session_state:
                st.warning("⚠️ Please enter a search query or select a quick search")
            else:
                query = search_query or st.session_state.get('search_input', '')
                
                # Add to search history
                if query not in st.session_state.search_history:
                    st.session_state.search_history.append(query)
                    if len(st.session_state.search_history) > 10:
                        st.session_state.search_history.pop(0)
                
                st.session_state.scraping_in_progress = True
                st.session_state.scraping_log = []
                
                # Create progress indicators
                progress_bar = st.progress(0)
                status_text = st.empty()
                stats_text = st.empty()
                
                # Initialize enhanced scraper
                scraper = EnhancedNigerianStatsScraper(
                    max_workers=max_workers,
                    use_selenium=use_selenium,
                    logger=logger,
                    timeout=timeout,
                    max_pages_per_site=max_pages_per_site,
                    verify_ssl=False
                )
                
                # Start scraping
                status_text.text("🚀 Initializing enhanced scraper...")
                progress_bar.progress(10)
                
                # Perform enhanced scraping
                results = scraper.enhanced_multi_website_scrape(
                    query,
                    selected_categories,
                    max_websites,
                    crawl_depth,
                    download_pdfs
                )
                
                progress_bar.progress(70)
                status_text.text("✅ Scraping complete! Processing results...")
                
                # Store results
                if results['data'] is not None:
                    st.session_state.scraped_data = results['data']
                
                if results['pdfs'] is not None and download_pdfs:
                    st.session_state.scraped_pdfs = results['pdfs']
                
                if results['failed_pdfs']:
                    st.session_state.failed_pdfs = results['failed_pdfs']
                
                # Store crawl stats
                st.session_state.crawl_stats = results['stats']
                
                # Auto-upload to Google Drive if enabled
                if GOOGLE_DRIVE_AVAILABLE and st.session_state.get('google_drive_auth') and st.session_state.get('google_drive_folder_id') and auto_save_drive:
                    status_text.text("📤 Uploading to Google Drive...")
                    progress_bar.progress(85)
                    
                    upload_summary = auto_upload_to_drive(
                        results,
                        st.session_state.google_drive_auth,
                        st.session_state.google_drive_folder_id,
                        upload_pdfs=download_pdfs,
                        upload_data=True
                    )
                    
                    st.session_state.drive_upload_results = upload_summary['pdfs']
                    
                    if upload_summary['data']:
                        st.success(f"✅ Data uploaded to Drive: {upload_summary['data']['file_name']}")
                    
                    if upload_summary['pdfs']:
                        st.success(f"✅ {len(upload_summary['pdfs'])} PDFs uploaded to Drive")
                
                progress_bar.progress(100)
                
                # Calculate duration
                if results['stats']['start_time'] and results['stats']['end_time']:
                    duration = (results['stats']['end_time'] - results['stats']['start_time']).total_seconds()
                    duration_str = f"{duration:.1f} seconds"
                else:
                    duration_str = "N/A"
                
                # Show summary
                if results['data'] is not None or results['pdfs'] is not None:
                    st.balloons()
                    
                    # Create summary columns
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Data Records", len(results['data']) if results['data'] is not None else 0)
                    with col2:
                        st.metric("PDFs Downloaded", len(results['pdfs']) if results['pdfs'] is not None else 0)
                    with col3:
                        st.metric("PDFs Failed", len(results['failed_pdfs']))
                    with col4:
                        st.metric("Pages Visited", results['stats']['pages_visited'])
                    
                    st.success(f"""
                    🎉 **Scraping Complete!**
                    - 📊 **{len(results['data']) if results['data'] is not None else 0}** data records
                    - 📄 **{len(results['pdfs']) if results['pdfs'] is not None else 0}** PDF files downloaded
                    - ❌ **{len(results['failed_pdfs'])}** PDFs failed (403 errors, timeouts, etc.)
                    - 🌐 **{results['stats']['pages_visited']}** pages visited
                    - ⏱️ **{duration_str}** total time
                    """)
                    
                    if results['failed_pdfs']:
                        with st.expander("View failed PDFs"):
                            failed_df = pd.DataFrame(results['failed_pdfs'])
                            st.dataframe(failed_df)
                
                st.session_state.scraping_in_progress = False
    
    with col2:
        if st.button("🧹 Clear Results & Cache", use_container_width=True, key="clear_button"):
            st.session_state.scraped_data = None
            st.session_state.scraped_pdfs = None
            st.session_state.failed_pdfs = []
            st.session_state.scraping_log = []
            st.session_state.crawl_stats = {}
            st.session_state.drive_upload_results = []
            # Clear downloaded PDFs
            import shutil
            if os.path.exists("downloaded_pdfs"):
                shutil.rmtree("downloaded_pdfs")
                os.makedirs("downloaded_pdfs", exist_ok=True)
            st.success("✅ Results cleared!")
            st.rerun()
    
    with col3:
        if st.button("📊 Dashboard", use_container_width=True, key="dashboard"):
            st.session_state.show_dashboard = True
    
    # Update session state with logs from thread-safe logger
    logs_from_threads = logger.get_logs()
    if logs_from_threads:
        st.session_state.scraping_log.extend(logs_from_threads)
    
    # Display scraping log
    if st.session_state.scraping_log:
        with st.expander("📋 Scraping Log", expanded=True):
            log_container = st.container()
            with log_container:
                for log_entry_text in st.session_state.scraping_log[-50:]:  # Show last 50 logs
                    if "Success" in log_entry_text or "found" in log_entry_text.lower() or "complete" in log_entry_text.lower():
                        st.success(log_entry_text)
                    elif "Error" in log_entry_text or "failed" in log_entry_text.lower() or "403" in log_entry_text:
                        st.error(log_entry_text)
                    elif "Warning" in log_entry_text:
                        st.warning(log_entry_text)
                    else:
                        st.info(log_entry_text)
    
    # Display results with enhanced PDF handling
    if st.session_state.scraped_data is not None or st.session_state.scraped_pdfs is not None:
        st.header("📊 Scraped Results")
        
        # Create tabs for different result types
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📋 Data Records", "📄 PDF Files", "❌ Failed PDFs", "📈 Statistics", "☁️ Cloud Export"])
        
        with tab1:
            if st.session_state.scraped_data is not None:
                df = st.session_state.scraped_data
                
                # Show summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Records", len(df), delta=None)
                with col2:
                    websites = df['Source_Website'].nunique() if 'Source_Website' in df.columns else 0
                    st.metric("Websites", websites)
                with col3:
                    pages = df['Page_URL'].nunique() if 'Page_URL' in df.columns else 0
                    st.metric("Pages", pages)
                with col4:
                    categories = df['Category'].nunique() if 'Category' in df.columns else 0
                    st.metric("Categories", categories)
                
                # Data preview with filters
                st.subheader("Data Preview")
                
                # Add filters
                with st.expander("🔍 Filter Data", expanded=False):
                    filter_col1, filter_col2 = st.columns(2)
                    
                    with filter_col1:
                        if 'Source_Website' in df.columns:
                            all_sources = ['All'] + list(df['Source_Website'].unique())
                            selected_source = st.selectbox("Filter by website:", all_sources)
                    
                    with filter_col2:
                        if 'Category' in df.columns:
                            all_cats = ['All'] + list(df['Category'].unique())
                            selected_cat = st.selectbox("Filter by category:", all_cats)
                    
                    # Apply filters
                    filtered_df = df.copy()
                    if 'selected_source' in locals() and selected_source != 'All':
                        filtered_df = filtered_df[filtered_df['Source_Website'] == selected_source]
                    if 'selected_cat' in locals() and selected_cat != 'All':
                        filtered_df = filtered_df[filtered_df['Category'] == selected_cat]
                
                st.dataframe(filtered_df if 'filtered_df' in locals() else df, use_container_width=True, height=400)
                
                # Export options for data
                st.subheader("📥 Export Data")
                col1, col2, col3, col4, col5 = st.columns(5)
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                base_filename = f"nigeria_stats_{timestamp}"
                
                with col1:
                    csv_link = create_download_link(df, f"{base_filename}.csv", "csv")
                    st.markdown(csv_link, unsafe_allow_html=True)
                
                with col2:
                    json_link = create_download_link(df, f"{base_filename}.json", "json")
                    st.markdown(json_link, unsafe_allow_html=True)
                
                with col3:
                    html_link = create_download_link(df, f"{base_filename}.html", "html")
                    st.markdown(html_link, unsafe_allow_html=True)
                
                with col4:
                    # Excel export
                    excel_buffer = io.BytesIO()
                    df.to_excel(excel_buffer, index=False)
                    st.download_button(
                        label="📥 Excel",
                        data=excel_buffer.getvalue(),
                        file_name=f"{base_filename}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                
                with col5:
                    # PDF export
                    pdf_buffer = create_pdf_download(df, f"{base_filename}.pdf")
                    st.download_button(
                        label="📥 PDF Report",
                        data=pdf_buffer.getvalue(),
                        file_name=f"{base_filename}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
        
        with tab2:
            if st.session_state.scraped_pdfs is not None and not st.session_state.scraped_pdfs.empty:
                pdf_df = st.session_state.scraped_pdfs
                
                st.subheader(f"📄 Found {len(pdf_df)} PDF Files")
                
                # PDF summary
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    total_size = pdf_df['File_Size_KB'].sum() if 'File_Size_KB' in pdf_df.columns else 0
                    st.metric("Total Size", f"{total_size:.0f} KB")
                with col2:
                    sources = pdf_df['Source_Website'].nunique() if 'Source_Website' in pdf_df.columns else 0
                    st.metric("Sources", sources)
                with col3:
                    st.metric("Files", len(pdf_df))
                with col4:
                    downloaded = pdf_df['Local_Path'].notna().sum() if 'Local_Path' in pdf_df.columns else 0
                    st.metric("Downloaded", downloaded)
                
                # Google Drive Upload Section for PDFs
                if GOOGLE_DRIVE_AVAILABLE and st.session_state.get('google_drive_auth'):
                    st.subheader("☁️ Upload PDFs to Google Drive")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        drive_folder_name = st.text_input(
                            "Google Drive folder name for PDFs:",
                            value=f"Nigeria_PDFs_{datetime.now().strftime('%Y%m')}",
                            key="drive_pdf_folder"
                        )
                        
                        use_category_folders = st.checkbox(
                            "Create subfolders by category",
                            value=True,
                            help="Organize PDFs into separate folders based on their category"
                        )
                    
                    with col2:
                        st.markdown("### Upload Options")
                        upload_all = st.button("📤 Upload All PDFs to Drive", use_container_width=True)
                    
                    if upload_all:
                        with st.spinner(f"Uploading {len(pdf_df)} PDFs to Google Drive..."):
                            # Get or create folder
                            folder_id = st.session_state.google_drive_auth.get_folder_id_by_name(drive_folder_name)
                            if not folder_id:
                                folder_id = st.session_state.google_drive_auth.create_folder(drive_folder_name)
                            
                            if folder_id:
                                # Upload PDFs
                                upload_results = upload_pdfs_to_drive(
                                    pdf_df, 
                                    st.session_state.google_drive_auth, 
                                    folder_id,
                                    use_category_folders
                                )
                                
                                st.session_state.drive_upload_results = upload_results
                                
                                if upload_results:
                                    st.success(f"✅ Successfully uploaded {len(upload_results)} PDFs to Google Drive!")
                                    
                                    # Show summary
                                    summary_df = create_drive_pdf_summary(upload_results)
                                    if summary_df is not None:
                                        st.dataframe(summary_df, use_container_width=True)
                                    
                                    # Create shareable link to folder
                                    folder_link = f"https://drive.google.com/drive/folders/{folder_id}"
                                    st.markdown(f"🔗 **Folder Link:** [Open in Google Drive]({folder_link})")
                                else:
                                    st.warning("No PDFs were uploaded to Google Drive")
                            else:
                                st.error("Failed to create folder in Google Drive")
                
                # PDF list with download options
                st.subheader("📋 PDF Files Found")
                
                # Display PDFs with Drive upload option
                for idx, row in pdf_df.iterrows():
                    with st.container():
                        st.markdown("---" if idx > 0 else "")
                        display_pdf_card(row, idx, show_drive_option=GOOGLE_DRIVE_AVAILABLE)
                
                # Bundle download
                st.subheader("📦 Download All PDFs")
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("📥 Download All as ZIP", use_container_width=True):
                        zip_buffer = create_pdf_bundle_download(pdf_df)
                        if zip_buffer:
                            st.download_button(
                                label="📥 Click to Download ZIP",
                                data=zip_buffer.getvalue(),
                                file_name=f"nigeria_pdfs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                                mime="application/zip",
                                key="zip_download"
                            )
                
                with col2:
                    # Create a summary CSV of PDF metadata
                    pdf_meta = pdf_df[['File_Name', 'Source_Website', 'Source_Category' if 'Source_Category' in pdf_df.columns else 'Category', 
                                      'File_Size_KB', 'PDF_URL', 'Scrape_Date']].copy() if all(col in pdf_df.columns for col in ['File_Name', 'Source_Website', 'File_Size_KB', 'PDF_URL', 'Scrape_Date']) else pdf_df
                    csv_meta = pdf_meta.to_csv(index=False)
                    st.download_button(
                        label="📥 Download PDF List (CSV)",
                        data=csv_meta,
                        file_name=f"pdf_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
            else:
                st.info("ℹ️ No PDF files were found during scraping.")
        
        with tab3:
            if st.session_state.failed_pdfs:
                st.subheader(f"❌ Failed PDF Downloads: {len(st.session_state.failed_pdfs)}")
                
                # Create DataFrame of failed PDFs
                failed_df = pd.DataFrame(st.session_state.failed_pdfs)
                st.dataframe(failed_df, use_container_width=True)
                
                # Export failed PDFs list
                csv_failed = failed_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Failed PDFs List (CSV)",
                    data=csv_failed,
                    file_name=f"failed_pdfs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No failed PDF downloads.")
        
        with tab4:
            if st.session_state.crawl_stats:
                st.subheader("📊 Crawl Statistics")
                
                stats = st.session_state.crawl_stats
                
                # Duration calculation
                if stats.get('start_time') and stats.get('end_time'):
                    duration = (stats['end_time'] - stats['start_time']).total_seconds()
                    minutes = int(duration // 60)
                    seconds = int(duration % 60)
                    duration_str = f"{minutes}m {seconds}s"
                else:
                    duration_str = "N/A"
                
                # Metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Pages Visited", stats.get('pages_visited', 0))
                with col2:
                    st.metric("PDFs Found", stats.get('pdfs_found', 0))
                with col3:
                    st.metric("PDFs Downloaded", stats.get('pdfs_downloaded', 0))
                with col4:
                    st.metric("PDFs Failed", stats.get('pdfs_failed', 0))
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Duration", duration_str)
                
                # Sources breakdown
                if st.session_state.scraped_data is not None:
                    st.subheader("📊 Data by Source")
                    source_counts = st.session_state.scraped_data['Source_Website'].value_counts()
                    st.bar_chart(source_counts)
                    
                    # Source details
                    with st.expander("View source details"):
                        source_df = pd.DataFrame({
                            'Source': source_counts.index,
                            'Records': source_counts.values
                        })
                        st.dataframe(source_df, use_container_width=True)
                
                # Categories breakdown
                if st.session_state.scraped_data is not None and 'Category' in st.session_state.scraped_data.columns:
                    st.subheader("📊 Data by Category")
                    category_counts = st.session_state.scraped_data['Category'].value_counts()
                    st.bar_chart(category_counts)
        
        with tab5:
            st.subheader("☁️ Cloud Export Options")
            
            if GOOGLE_DRIVE_AVAILABLE:
                tab_pdf, tab_data = st.tabs(["📄 Upload PDFs", "📊 Upload Data"])
                
                with tab_pdf:
                    if st.session_state.scraped_pdfs is not None and not st.session_state.scraped_pdfs.empty:
                        st.markdown("### Upload PDFs to Google Drive")
                        
                        if not st.session_state.get('google_drive_auth'):
                            if st.button("🔗 Connect to Google Drive", key="connect_drive_pdf_tab"):
                                with st.spinner("Connecting..."):
                                    drive_manager = GoogleDriveManager()
                                    if drive_manager.authenticate():
                                        st.session_state.google_drive_auth = drive_manager
                                        st.rerun()
                        else:
                            st.success("✅ Connected to Google Drive")
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                pdf_folder_name = st.text_input(
                                    "Folder name for PDFs:",
                                    value=f"Nigeria_PDFs_{datetime.now().strftime('%Y%m')}",
                                    key="pdf_folder_cloud"
                                )
                                
                                organize_by_category = st.checkbox(
                                    "Organize by category",
                                    value=True,
                                    help="Create subfolders for each data category"
                                )
                                
                                include_metadata = st.checkbox(
                                    "Include metadata file",
                                    value=True,
                                    help="Upload a CSV file with PDF metadata"
                                )
                            
                            with col2:
                                st.markdown("### Statistics")
                                st.metric("PDFs to upload", len(st.session_state.scraped_pdfs))
                                total_size = st.session_state.scraped_pdfs['File_Size_KB'].sum() if 'File_Size_KB' in st.session_state.scraped_pdfs.columns else 0
                                st.metric("Total size", f"{total_size:.0f} KB")
                                
                                if st.button("📤 Upload All PDFs", use_container_width=True, type="primary"):
                                    with st.spinner("Uploading PDFs to Google Drive..."):
                                        # Get or create folder
                                        folder_id = st.session_state.google_drive_auth.get_folder_id_by_name(pdf_folder_name)
                                        if not folder_id:
                                            folder_id = st.session_state.google_drive_auth.create_folder(pdf_folder_name)
                                        
                                        if folder_id:
                                            # Upload PDFs
                                            upload_results = upload_pdfs_to_drive(
                                                st.session_state.scraped_pdfs,
                                                st.session_state.google_drive_auth,
                                                folder_id,
                                                organize_by_category
                                            )
                                            
                                            st.session_state.drive_upload_results = upload_results
                                            
                                            if upload_results:
                                                st.success(f"✅ Successfully uploaded {len(upload_results)} PDFs!")
                                                
                                                # Create and upload metadata file if requested
                                                if include_metadata:
                                                    metadata_df = create_drive_pdf_summary(upload_results)
                                                    if metadata_df is not None:
                                                        metadata_result = st.session_state.google_drive_auth.upload_dataframe(
                                                            metadata_df,
                                                            f"pdf_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                                                            folder_id,
                                                            'csv'
                                                        )
                                                        if metadata_result:
                                                            st.info(f"📊 Metadata file uploaded: {metadata_result['file_name']}")
                                                
                                                # Show uploaded files
                                                st.subheader("📋 Uploaded Files")
                                                for result in upload_results[:5]:  # Show first 5
                                                    st.markdown(f"- [{result['file_name']}]({result['web_link']})")
                                                
                                                if len(upload_results) > 5:
                                                    st.markdown(f"*... and {len(upload_results) - 5} more*")
                                                
                                                # Folder link
                                                folder_link = f"https://drive.google.com/drive/folders/{folder_id}"
                                                st.markdown(f"🔗 **Folder Link:** [Open in Google Drive]({folder_link})")
                                        else:
                                            st.error("Failed to create folder in Google Drive")
                    else:
                        st.info("No PDF files available to upload. Run a scrape first!")
                
                with tab_data:
                    st.markdown("### Upload Data to Google Drive")
                    
                    if not st.session_state.get('google_drive_auth'):
                        if st.button("🔗 Connect to Google Drive", key="connect_drive_data_tab"):
                            with st.spinner("Connecting..."):
                                drive_manager = GoogleDriveManager()
                                if drive_manager.authenticate():
                                    st.session_state.google_drive_auth = drive_manager
                                    st.rerun()
                    else:
                        if st.session_state.scraped_data is not None:
                            st.success("✅ Connected to Google Drive")
                            
                            # File upload options
                            drive_filename = st.text_input(
                                "File name:",
                                value=f"nigeria_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                                key="drive_filename_data"
                            )
                            
                            drive_format = st.selectbox(
                                "File format:",
                                ["CSV", "JSON", "Excel"],
                                key="drive_format_data"
                            )
                            
                            data_folder_name = st.text_input(
                                "Folder name:",
                                value=f"Nigeria_Stats_{datetime.now().strftime('%Y%m')}",
                                key="drive_folder_data"
                            )
                            
                            if st.button("📤 Upload Data to Google Drive", use_container_width=True, type="primary"):
                                with st.spinner("Uploading to Google Drive..."):
                                    # Get or create folder
                                    folder_id = st.session_state.google_drive_auth.get_folder_id_by_name(data_folder_name)
                                    if not folder_id:
                                        folder_id = st.session_state.google_drive_auth.create_folder(data_folder_name)
                                    
                                    if folder_id:
                                        result = st.session_state.google_drive_auth.upload_dataframe(
                                            st.session_state.scraped_data,
                                            drive_filename,
                                            folder_id,
                                            drive_format.lower()
                                        )
                                        
                                        if result:
                                            st.success(f"✅ File uploaded to Google Drive!")
                                            st.info(f"📁 **Folder:** {data_folder_name}")
                                            st.info(f"📄 **File:** {result['file_name']}")
                                            st.markdown(f"🔗 **Link:** [Open in Google Drive]({result['web_link']})", unsafe_allow_html=True)
                                        else:
                                            st.error("❌ Failed to upload file")
                                    else:
                                        st.error("❌ Failed to create folder")
                        else:
                            st.warning("No data to upload. Run a scrape first!")
            else:
                st.warning("Google Drive API not available. Install required libraries.")
    
    # Footer
    st.markdown("---")
    st.markdown("""
        <div style="text-align: center; color: #666; padding: 2rem 0;">
            <p><strong>Nigeria Statistics Web Scraper Pro</strong> • Version 5.1</p>
            <p>🌐 <strong>Multi-website crawling</strong> with multi-page support • 📄 <strong>PDF downloading</strong> • ☁️ <strong>Google Drive integration</strong></p>
            <p>⚡ <strong>Concurrent scraping</strong> with thread-safe logging • 📊 <strong>Advanced data extraction</strong></p>
            <p>⚠️ <strong>Note:</strong> Respect website terms of service and robots.txt. Use responsibly.</p>
            <p>🛠️ <strong>Technologies:</strong> Python, BeautifulSoup, Selenium, Google Drive API, Streamlit, Pandas</p>
            <p>© 2026 Nigeria Stats Web Scraper Pro</p>
            <p> Built by Jimmy Ukaba </p>
        </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    # Create necessary directories
    os.makedirs("static", exist_ok=True)
    os.makedirs("scraped_data", exist_ok=True)
    os.makedirs("downloaded_pdfs", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    # Run the main application
    main()