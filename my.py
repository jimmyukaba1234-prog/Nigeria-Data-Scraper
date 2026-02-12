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
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import html5lib
import zipfile
import tempfile

# Google Drive API libraries
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
    import mimetypes
    GOOGLE_DRIVE_AVAILABLE = True
except ImportError as e:
    GOOGLE_DRIVE_AVAILABLE = False
    print(f"Google Drive libraries not available: {e}")

# Page configuration
st.set_page_config(
    page_title="Nigeria Data Table Scraper",
    page_icon="🇳🇬",
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
    .topic-card {
        padding: 1rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
        margin: 0.5rem 0;
        cursor: pointer;
        transition: transform 0.3s;
    }
    .topic-card:hover {
        transform: translateY(-5px);
    }
    .topic-icon {
        font-size: 2rem;
        margin-bottom: 0.5rem;
    }
    .topic-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
        gap: 10px;
        margin: 1rem 0;
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
    .google-drive-btn {
        background: linear-gradient(45deg, #4285F4, #34A853, #FBBC05, #EA4335);
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 5px;
        cursor: pointer;
        font-weight: bold;
        width: 100%;
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
        print(log_message)
    
    def get_logs(self):
        logs = []
        while not self.log_queue.empty():
            logs.append(self.log_queue.get())
        return logs
    
    def get_all_logs(self):
        return self.logs

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
                fields='id, webViewLink'
            ).execute()
            
            return {
                'id': folder.get('id'),
                'link': folder.get('webViewLink')
            }
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
                fields='id, webViewLink, size'
            ).execute()
            
            return {
                'id': file.get('id'),
                'link': file.get('webViewLink'),
                'name': file_name,
                'size': file.get('size', '0')
            }
            
        except Exception as e:
            st.error(f"Error uploading file {file_name}: {e}")
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
                fields='id, webViewLink, size'
            ).execute()
            
            return {
                'id': file.get('id'),
                'link': file.get('webViewLink'),
                'name': file_name,
                'size': file.get('size', '0')
            }
            
        except Exception as e:
            st.error(f"Error uploading DataFrame: {e}")
            return None
    
    def upload_zip_file(self, zip_path, zip_name, folder_id=None):
        """Upload a zip file to Google Drive"""
        return self.upload_file(zip_path, zip_name, folder_id, 'application/zip')
    
    def list_files(self, folder_id=None):
        """List files in Google Drive"""
        try:
            query = "trashed=false"
            if folder_id:
                query = f"'{folder_id}' in parents and trashed=false"
            
            results = self.service.files().list(
                q=query,
                pageSize=50,
                fields="files(id, name, mimeType, createdTime, size, webViewLink)"
            ).execute()
            
            return results.get('files', [])
        except Exception as e:
            st.error(f"Error listing files: {e}")
            return []
    
    def get_folder_id_by_name(self, folder_name, parent_id=None):
        """Get folder ID by name"""
        try:
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_id:
                query += f" and '{parent_id}' in parents"
            
            results = self.service.files().list(
                q=query,
                fields="files(id, name)"
            ).execute()
            
            files = results.get('files', [])
            if files:
                return files[0]['id']
            return None
        except Exception as e:
            st.error(f"Error getting folder ID: {e}")
            return None

# Nigerian Topics with Icons and Search Terms
NIGERIAN_TOPICS = {
    "Agriculture": {
        "icon": "🌾",
        "search_terms": ["agriculture", "farming", "crops", "livestock", "food production", "agricultural GDP"],
        "keywords": ["crop production", "agricultural output", "farm inputs", "fertilizer", "irrigation"]
    },
    "Banking": {
        "icon": "🏦",
        "search_terms": ["banking", "financial sector", "banks", "deposits", "loans", "credit"],
        "keywords": ["bank assets", "non-performing loans", "bank profitability", "financial inclusion"]
    },
    "Budget": {
        "icon": "💰",
        "search_terms": ["budget", "federal budget", "appropriation", "government spending", "fiscal"],
        "keywords": ["budget allocation", "capital expenditure", "recurrent expenditure", "budget deficit"]
    },
    "Business": {
        "icon": "💼",
        "search_terms": ["business", "companies", "enterprises", "MSMEs", "private sector"],
        "keywords": ["business registration", "corporate tax", "investment", "entrepreneurship"]
    },
    "Capital Importation": {
        "icon": "🌍",
        "search_terms": ["capital importation", "foreign investment", "FDI", "portfolio investment"],
        "keywords": ["foreign direct investment", "capital flows", "investment inflow", "external investment"]
    },
    "GDP": {
        "icon": "📈",
        "search_terms": ["GDP", "gross domestic product", "economic growth", "national income"],
        "keywords": ["GDP growth rate", "GDP per capita", "sectoral contribution", "real GDP"]
    },
    "Health": {
        "icon": "🏥",
        "search_terms": ["health", "healthcare", "hospitals", "disease", "mortality", "life expectancy"],
        "keywords": ["health expenditure", "doctor patient ratio", "hospital beds", "immunization"]
    },
    "Education": {
        "icon": "🎓",
        "search_terms": ["education", "schools", "universities", "literacy", "enrollment"],
        "keywords": ["education budget", "student enrollment", "teacher ratio", "literacy rate"]
    },
    "Unemployment": {
        "icon": "👨‍💼",
        "search_terms": ["unemployment", "employment", "jobs", "labor force", "underemployment"],
        "keywords": ["unemployment rate", "youth unemployment", "employment by sector", "labor statistics"]
    },
    "Population": {
        "icon": "👥",
        "search_terms": ["population", "census", "demographics", "birth rate", "migration"],
        "keywords": ["population growth", "population density", "age distribution", "urban population"]
    },
    "Oil and Gas": {
        "icon": "🛢️",
        "search_terms": ["oil", "gas", "petroleum", "crude oil", "NNPC", "refinery"],
        "keywords": ["oil production", "oil exports", "gas flaring", "petroleum subsidy"]
    },
    "Trade": {
        "icon": "📦",
        "search_terms": ["trade", "exports", "imports", "balance of trade", "customs"],
        "keywords": ["export commodities", "import composition", "trade balance", "trade partners"]
    },
    "Inflation": {
        "icon": "📊",
        "search_terms": ["inflation", "CPI", "consumer prices", "price index"],
        "keywords": ["inflation rate", "food inflation", "core inflation", "price indices"]
    },
    "Security": {
        "icon": "🛡️",
        "search_terms": ["security", "crime", "terrorism", "Boko Haram", "police", "military"],
        "keywords": ["crime statistics", "security spending", "terror incidents", "police strength"]
    },
    "Transportation": {
        "icon": "🚗",
        "search_terms": ["transport", "roads", "railways", "airports", "ports", "logistics"],
        "keywords": ["road network", "vehicle registration", "transport costs", "infrastructure"]
    },
    "Telecommunications": {
        "icon": "📱",
        "search_terms": ["telecom", "mobile", "internet", "broadband", "GSM", "NCC"],
        "keywords": ["mobile penetration", "internet users", "telco revenue", "broadband access"]
    },
    "Energy": {
        "icon": "⚡",
        "search_terms": ["energy", "electricity", "power", "generation", "transmission", "distribution"],
        "keywords": ["electricity generation", "power distribution", "energy consumption", "grid capacity"]
    },
    "Poverty": {
        "icon": "🏚️",
        "search_terms": ["poverty", "inequality", "vulnerability", "social safety nets"],
        "keywords": ["poverty rate", "income inequality", "poverty line", "social programs"]
    },
    "Tax": {
        "icon": "🧾",
        "search_terms": ["tax", "taxation", "FIRS", "revenue", "VAT", "tax collection"],
        "keywords": ["tax revenue", "VAT collection", "company income tax", "tax-to-GDP ratio"]
    },
    "Debt": {
        "icon": "💳",
        "search_terms": ["debt", "borrowing", "DMO", "external debt", "domestic debt"],
        "keywords": ["debt-to-GDP", "debt service", "external borrowing", "debt stock"]
    },
    "Politics": {
        "icon": "🏛️",
        "search_terms": ["politics", "elections", "governance", "democracy", "political parties"],
        "keywords": ["election results", "voter turnout", "political appointments", "governance indicators"]
    }
}

class TableScraper:
    """Specialized class for extracting and processing tables from websites"""
    
    def __init__(self, logger=None):
        self.logger = logger
    
    def log(self, message):
        if self.logger:
            self.logger.add_log(message)
    
    def extract_all_tables(self, soup, url, search_terms):
        """Extract all tables from HTML content"""
        tables_data = []
        
        try:
            # Method 1: Use pandas read_html
            try:
                df_list = pd.read_html(str(soup), flavor='html5lib')
                for i, df in enumerate(df_list):
                    if not df.empty and len(df) > 1:
                        if self._table_matches_search(df, search_terms):
                            table_info = self._process_table(df, i, url, "pandas")
                            tables_data.append(table_info)
            except:
                pass
            
            # Method 2: Manual extraction
            html_tables = soup.find_all('table')
            for table_idx, table in enumerate(html_tables):
                try:
                    table_data = self._extract_table_manually(table, table_idx, url)
                    if table_data and self._table_matches_search(table_data['dataframe'], search_terms):
                        tables_data.append(table_data)
                except:
                    continue
            
        except Exception as e:
            self.log(f"Error extracting tables: {str(e)[:100]}")
        
        return tables_data
    
    def _process_table(self, df, table_index, url, method):
        """Process a pandas DataFrame table"""
        df = df.copy().dropna(how='all').reset_index(drop=True)
        
        metadata = {
            'source_url': url,
            'table_name': f"Table_{table_index+1}",
            'extraction_method': method,
            'rows': len(df),
            'columns': len(df.columns),
            'scrape_date': datetime.now().strftime('%Y-%m-%d'),
            'column_names': list(df.columns)
        }
        
        return {
            'metadata': metadata,
            'dataframe': df,
            'preview': df.head(5).to_dict('records')
        }
    
    def _extract_table_manually(self, table, table_index, url):
        """Manually extract table data"""
        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return None
            
            data = []
            headers = []
            
            # Extract headers
            header_row = rows[0]
            header_cells = header_row.find_all(['th', 'td'])
            headers = [cell.get_text(strip=True) or f"Column_{i+1}" 
                      for i, cell in enumerate(header_cells)]
            
            # Extract data
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if cells:
                    row_data = {}
                    for i, cell in enumerate(cells[:len(headers)]):
                        col_name = headers[i] if i < len(headers) else f"Column_{i+1}"
                        row_data[col_name] = cell.get_text(strip=True)
                    data.append(row_data)
            
            if not data:
                return None
            
            df = pd.DataFrame(data)
            metadata = {
                'source_url': url,
                'table_name': f"Table_{table_index+1}_manual",
                'extraction_method': 'manual',
                'rows': len(df),
                'columns': len(df.columns),
                'scrape_date': datetime.now().strftime('%Y-%m-%d'),
                'column_names': list(df.columns)
            }
            
            return {
                'metadata': metadata,
                'dataframe': df,
                'preview': df.head(5).to_dict('records')
            }
            
        except:
            return None
    
    def _table_matches_search(self, df, search_terms):
        """Check if table content matches search terms"""
        if not search_terms:
            return True
        
        # Convert DataFrame to string for searching
        df_str = df.to_string().lower()
        
        for term in search_terms:
            if term.lower() in df_str:
                return True
        
        # Check column names
        columns_str = ' '.join(df.columns.astype(str)).lower()
        for term in search_terms:
            if term.lower() in columns_str:
                return True
        
        return False
    
    def save_table(self, table_data, filename_prefix, folder="tables"):
        """Save table to CSV and Excel files"""
        try:
            os.makedirs(folder, exist_ok=True)
            
            df = table_data['dataframe']
            table_name = table_data['metadata']['table_name'].replace(' ', '_')
            
            # Save as CSV
            csv_path = os.path.join(folder, f"{filename_prefix}_{table_name}.csv")
            df.to_csv(csv_path, index=False)
            
            # Save as Excel
            excel_path = os.path.join(folder, f"{filename_prefix}_{table_name}.xlsx")
            df.to_excel(excel_path, index=False)
            
            # Save metadata
            meta_path = os.path.join(folder, f"{filename_prefix}_{table_name}_metadata.json")
            with open(meta_path, 'w') as f:
                json.dump(table_data['metadata'], f, indent=2)
            
            return {
                'csv_path': csv_path,
                'excel_path': excel_path,
                'metadata_path': meta_path,
                'table_name': table_name,
                'rows': len(df),
                'columns': len(df.columns)
            }
            
        except Exception as e:
            self.log(f"Error saving table: {str(e)}")
            return None

class NigerianDataScraper:
    """Main scraper for Nigerian data with topic-based searching"""
    
    def __init__(self, max_workers=3, logger=None):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        self.max_workers = max_workers
        self.logger = logger
        self.table_scraper = TableScraper(logger=logger)
    
    def log(self, message):
        if self.logger:
            self.logger.add_log(message)
    
    def get_topic_sources(self, topic):
        """Get specialized sources for specific topics"""
        topic_sources = {
            "GDP": [
                {"name": "World Bank GDP Data", "url": "https://data.worldbank.org/indicator/NY.GDP.MKTP.CD?locations=NG"},
                {"name": "NBS GDP Reports", "url": "https://nigerianstat.gov.ng/elibrary?page=2&query=GDP"},
                {"name": "IMF Nigeria GDP", "url": "https://www.imf.org/en/Countries/NGA"},
            ],
            "Agriculture": [
                {"name": "FAO Nigeria Data", "url": "https://www.fao.org/countryprofiles/index/en/?iso3=NGA"},
                {"name": "NBS Agriculture", "url": "https://nigerianstat.gov.ng/elibrary?query=agriculture"},
                {"name": "World Bank Agri Data", "url": "https://data.worldbank.org/topic/agriculture-and-rural-development?locations=NG"},
            ],
            "Oil and Gas": [
                {"name": "NNPC Statistics", "url": "https://nnpcgroup.com/Public-Relations/Oil-and-Gas-Statistics.aspx"},
                {"name": "OPEC Nigeria", "url": "https://www.opec.org/opec_web/en/about_us/167.htm"},
                {"name": "EIA Nigeria", "url": "https://www.eia.gov/international/analysis/country/NGA"},
            ],
            "Banking": [
                {"name": "CBN Statistics", "url": "https://www.cbn.gov.ng/rates/"},
                {"name": "NDIC Reports", "url": "https://ndic.gov.ng/"},
                {"name": "World Bank Financial", "url": "https://data.worldbank.org/topic/financial-sector?locations=NG"},
            ],
            "Health": [
                {"name": "WHO Nigeria", "url": "https://www.who.int/countries/nga"},
                {"name": "NBS Health", "url": "https://nigerianstat.gov.ng/elibrary?query=health"},
                {"name": "World Bank Health", "url": "https://data.worldbank.org/topic/health?locations=NG"},
            ],
            "Education": [
                {"name": "UNESCO Nigeria", "url": "https://uis.unesco.org/en/country/ng"},
                {"name": "World Bank Education", "url": "https://data.worldbank.org/topic/education?locations=NG"},
                {"name": "NBS Education", "url": "https://nigerianstat.gov.ng/elibrary?query=education"},
            ],
            "Population": [
                {"name": "UN Population Division", "url": "https://population.un.org/wpp/"},
                {"name": "World Bank Population", "url": "https://data.worldbank.org/indicator/SP.POP.TOTL?locations=NG"},
                {"name": "Worldometer Nigeria", "url": "https://www.worldometers.info/world-population/nigeria-population/"},
            ],
            "Trade": [
                {"name": "UN Comtrade Nigeria", "url": "https://comtradeplus.un.org/"},
                {"name": "WTO Nigeria", "url": "https://www.wto.org/english/thewto_e/countries_e/nigeria_e.htm"},
                {"name": "NBS Trade", "url": "https://nigerianstat.gov.ng/elibrary?query=trade"},
            ]
        }
        
        # Default sources if topic not specifically mapped
        default_sources = [
            {"name": "World Bank Nigeria", "url": "https://data.worldbank.org/country/nigeria"},
            {"name": "UN Data Nigeria", "url": "https://data.un.org/en/iso/ng.html"},
            {"name": "NBS Library", "url": "https://nigerianstat.gov.ng/elibrary"},
            {"name": "Knoema Nigeria", "url": "https://knoema.com/atlas/Nigeria"},
            {"name": "Trading Economics", "url": "https://tradingeconomics.com/nigeria/indicators"},
        ]
        
        return topic_sources.get(topic, default_sources)
    
    def scrape_topic(self, topic, search_terms, max_sources=5):
        """Scrape data for a specific topic"""
        all_tables = []
        all_data = []
        
        # Get sources for this topic
        sources = self.get_topic_sources(topic)[:max_sources]
        
        self.log(f"📊 Scraping {topic} data from {len(sources)} sources...")
        
        # Scrape each source
        for source in sources:
            try:
                self.log(f"  🔍 Checking {source['name']}...")
                
                # Try to get the page
                try:
                    response = self.session.get(source['url'], timeout=10)
                    if response.status_code != 200:
                        self.log(f"    ⚠️ Could not access {source['name']}")
                        continue
                except:
                    self.log(f"    ⚠️ Connection failed for {source['name']}")
                    continue
                
                # Parse the page
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract tables
                tables = self.table_scraper.extract_all_tables(soup, source['url'], search_terms)
                
                if tables:
                    self.log(f"    ✅ Found {len(tables)} tables")
                    all_tables.extend(tables)
                    
                    # Also extract text data
                    text_data = self.extract_text_data(soup, source['url'], search_terms)
                    all_data.extend(text_data)
                else:
                    self.log(f"    ℹ️ No tables found")
            
            except Exception as e:
                self.log(f"    ❌ Error scraping {source['name']}: {str(e)[:100]}")
        
        return all_data, all_tables
    
    def extract_text_data(self, soup, url, search_terms):
        """Extract relevant text data"""
        data = []
        text = soup.get_text()
        
        # Look for patterns with numbers (likely statistics)
        patterns = [
            r'\b\d+\.?\d*\s*%\b',  # Percentages
            r'\b\d{1,3}(?:,\d{3})+\b',  # Large numbers
            r'\b\d+\s*(?:million|billion|thousand)\b',  # Quantities
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches[:10]:  # Limit matches
                # Check if match is relevant to search terms
                for term in search_terms:
                    if term.lower() in text.lower():
                        data.append({
                            'value': match,
                            'context': self.get_context(text, match),
                            'source_url': url,
                            'scrape_date': datetime.now().strftime('%Y-%m-%d')
                        })
                        break
        
        return data
    
    def get_context(self, text, match, chars=100):
        """Get context around a match"""
        idx = text.find(match)
        if idx == -1:
            return match
        
        start = max(0, idx - chars)
        end = min(len(text), idx + len(match) + chars)
        return text[start:end]
    
    def save_topic_data(self, topic, tables, text_data, search_terms):
        """Save all data for a topic"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        topic_folder = f"data/{topic.replace(' ', '_')}_{timestamp}"
        os.makedirs(topic_folder, exist_ok=True)
        
        saved_files = []
        
        # Save tables
        for i, table in enumerate(tables):
            filename = f"{topic}_{i+1:03d}"
            saved = self.table_scraper.save_table(table, filename, topic_folder)
            if saved:
                saved_files.append(saved)
        
        # Save text data
        if text_data:
            text_df = pd.DataFrame(text_data)
            text_csv = os.path.join(topic_folder, f"{topic}_text_data.csv")
            text_excel = os.path.join(topic_folder, f"{topic}_text_data.xlsx")
            text_df.to_csv(text_csv, index=False)
            text_df.to_excel(text_excel, index=False)
            saved_files.append({
                'csv_path': text_csv,
                'excel_path': text_excel,
                'type': 'text_data'
            })
        
        # Create summary
        summary = {
            'topic': topic,
            'search_terms': search_terms,
            'tables_found': len(tables),
            'text_records': len(text_data),
            'scrape_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'files': [os.path.basename(f['csv_path']) for f in saved_files if 'csv_path' in f]
        }
        
        summary_path = os.path.join(topic_folder, "scrape_summary.json")
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        return topic_folder, saved_files, summary

def create_zip_file(folder_path, zip_name):
    """Create a zip file from folder"""
    zip_path = f"{zip_name}.zip"
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, os.path.dirname(folder_path))
                zipf.write(file_path, arcname)
    return zip_path

def main():
    """Main Streamlit application"""
    
    # Initialize session state
    if 'current_topic' not in st.session_state:
        st.session_state.current_topic = None
    if 'scraped_data' not in st.session_state:
        st.session_state.scraped_data = None
    if 'extracted_tables' not in st.session_state:
        st.session_state.extracted_tables = []
    if 'saved_files' not in st.session_state:
        st.session_state.saved_files = []
    if 'scraping_log' not in st.session_state:
        st.session_state.scraping_log = []
    if 'google_drive_auth' not in st.session_state:
        st.session_state.google_drive_auth = None
    if 'drive_folder_id' not in st.session_state:
        st.session_state.drive_folder_id = None
    
    # Header
    st.markdown('<h1 class="main-header">🇳🇬 Nigeria Data Table Scraper</h1>', unsafe_allow_html=True)
    st.markdown("""
        <div style="text-align: center; margin-bottom: 2rem;">
            <p><strong>Extract tables and data for specific Nigerian topics</strong></p>
            <p>Click on any topic below to start scraping relevant data tables</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Sidebar with Google Drive Configuration
    with st.sidebar:
        st.header("⚙️ Settings")
        
        max_sources = st.slider("Maximum sources per topic", 1, 10, 5)
        
        st.header("☁️ Google Drive")
        
        if GOOGLE_DRIVE_AVAILABLE:
            # Google Drive Authentication
            if st.session_state.google_drive_auth is None:
                if st.button("🔗 Connect to Google Drive", use_container_width=True):
                    with st.spinner("Connecting to Google Drive..."):
                        drive_manager = GoogleDriveManager()
                        if drive_manager.authenticate():
                            st.session_state.google_drive_auth = drive_manager
                            st.success("✅ Connected to Google Drive!")
                            st.rerun()
                        else:
                            st.error("❌ Failed to connect to Google Drive")
        else:
            st.success("✅ Google Drive: Connected")
                
            # Auto-save option
            auto_save = st.checkbox("Auto-save to Google Drive", value=True)
            st.session_state.auto_save_drive = auto_save
                
            # Create folder option
            if st.button("📁 Create Google Drive Folder", use_container_width=True):
                folder_name = f"Nigeria_Data_{datetime.now().strftime('%Y%m%d')}"
                result = st.session_state.google_drive_auth.create_folder(folder_name)
                if result:
                    st.session_state.drive_folder_id = result['id']
                    st.success(f"✅ Created folder: {folder_name}")
                    st.info(f"📁 Folder Link: [Open in Drive]({result['link']})")
            
            # Show current folder
            if st.session_state.drive_folder_id:
                st.info("📁 Folder created in Google Drive")
                
        # List files button
        if st.button("📋 List Drive Files", use_container_width=True):
            files = st.session_state.google_drive_auth.list_files(
                st.session_state.drive_folder_id)
            if files:
                st.subheader("Files in Drive:")
                for file in files:
                    st.write(f"📄 {file['name']} ({file.get('size', '0')} bytes)")
            else:
                st.info("No files found")
        
        if st.button("🚪 Disconnect Google Drive", use_container_width=True, type="secondary"):
            st.session_state.google_drive_auth = None
            st.session_state.drive_folder_id = None
            st.success("Disconnected from Google Drive")
            st.rerun()
        else:
            st.warning("⚠️ Google Drive libraries not installed")
            st.info("""
            Install required packages:
            ```bash
            pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
            ```
            """)
    
    # Main content area
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.header("📊 Statistics")
        st.metric("Available Topics", len(NIGERIAN_TOPICS))
        
        if st.session_state.extracted_tables:
            st.metric("Tables Found", len(st.session_state.extracted_tables))
        
        if st.session_state.saved_files:
            st.metric("Files Saved", len(st.session_state.saved_files))
    
    with col1:
        st.header("🎯 Select a Topic")
        
        # Display topic cards in a grid using columns
        cols = st.columns(4)
        for idx, (topic, info) in enumerate(NIGERIAN_TOPICS.items()):
            with cols[idx % 4]:
                # Create a clickable card using st.markdown
                card_html = f"""
                <div style="
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 1rem;
                    border-radius: 10px;
                    text-align: center;
                    margin: 0.5rem 0;
                    cursor: pointer;
                    transition: transform 0.3s;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                "
                onclick="
                    const event = new CustomEvent('topic_selected', {{detail: '{topic}'}});
                    window.parent.document.dispatchEvent(event);
                ">
                    <div style="font-size: 2rem; margin-bottom: 0.5rem;">{info['icon']}</div>
                    <div style="font-weight: bold; font-size: 1.1rem;">{topic}</div>
                    <div style="font-size: 0.8rem; opacity: 0.9; margin-top: 0.5rem;">
                        {len(info['search_terms'])} search terms
                    </div>
                </div>
                """
                
                st.markdown(card_html, unsafe_allow_html=True)

    # JavaScript to handle clicks
    st.markdown("""
    <script>
    // Listen for topic selection events
    window.parent.document.addEventListener('topic_selected', function(e) {
        // Send the topic to Streamlit
        window.parent.document.dispatchEvent(new CustomEvent('STREAMLIT_TOPIC_SELECTED', {
            detail: e.detail
        }));
    });
    </script>
    """, unsafe_allow_html=True)

    # Listen for the JavaScript event
    if st.session_state.get('_topic_selected'):
        topic = st.session_state['_topic_selected']
        st.session_state.current_topic = topic
        st.session_state['_topic_selected'] = None
        st.rerun()

    # Add JavaScript event listener
    st.markdown("""
    <script>
    window.parent.document.addEventListener('STREAMLIT_TOPIC_SELECTED', function(e) {
        // Send data to Streamlit via custom component
        if (window.parent.Streamlit) {
            window.parent.Streamlit.setComponentValue(e.detail);
        }
    });
    </script>
    """, unsafe_allow_html=True)
        
    # Custom search option
    with st.expander("🔍 Custom Search", expanded=False):
        custom_topic = st.text_input("Enter your topic:")
        custom_terms = st.text_area("Search terms (comma-separated):")
        
        if st.button("Start Custom Search", use_container_width=True):
            if custom_topic and custom_terms:
                st.session_state.current_topic = custom_topic
                search_terms = [term.strip() for term in custom_terms.split(',')]
                st.session_state.custom_search_terms = search_terms
                st.rerun()
        else:
            st.warning("Please enter both topic and search terms")
    
    # Topic-specific scraping section
    if st.session_state.current_topic:
        st.markdown("---")
        
        topic = st.session_state.current_topic
        if topic in NIGERIAN_TOPICS:
            topic_info = NIGERIAN_TOPICS[topic]
            search_terms = topic_info['search_terms']
        else:
            topic_info = {"icon": "🔍"}
            search_terms = st.session_state.get('custom_search_terms', [topic])
        
        # Topic header
        col1, col2, col3 = st.columns([1, 3, 1])
        with col2:
            st.markdown(f"""
                <div style="text-align: center; padding: 1rem; background: #f0f2f6; border-radius: 10px;">
                    <h2>{topic_info.get('icon', '📊')} {topic}</h2>
                    <p><strong>Search Terms:</strong> {', '.join(search_terms[:5])}</p>
                </div>
            """, unsafe_allow_html=True)
        
        # Scraping controls
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button(f"🚀 Start Scraping {topic}", use_container_width=True, type="primary"):
                with st.spinner(f"Scraping {topic} data..."):
                    # Initialize scraper
                    scraper = NigerianDataScraper(max_workers=max_sources, logger=logger)
                    
                    # Create progress bar
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # Scrape the topic
                    status_text.text(f"🔄 Scraping {topic} from multiple sources...")
                    
                    text_data, tables = scraper.scrape_topic(
                        topic, 
                        search_terms, 
                        max_sources=max_sources
                    )
                    
                    progress_bar.progress(0.5)
                    
                    if tables:
                        status_text.text(f"✅ Found {len(tables)} tables. Saving data...")
                        
                        # Save data locally
                        folder_path, saved_files, summary = scraper.save_topic_data(
                            topic, tables, text_data, search_terms
                        )
                        
                        progress_bar.progress(0.8)
                        
                        # Update session state
                        st.session_state.scraped_data = {
                            'topic': topic,
                            'tables': tables,
                            'text_data': text_data,
                            'summary': summary,
                            'folder_path': folder_path,
                            'saved_files': saved_files
                        }
                        st.session_state.extracted_tables = tables
                        st.session_state.saved_files = saved_files
                        
                        # Auto-save to Google Drive if enabled
                        if (GOOGLE_DRIVE_AVAILABLE and 
                            st.session_state.google_drive_auth and 
                            st.session_state.get('auto_save_drive', False)):
                            
                            status_text.text("📤 Uploading to Google Drive...")
                            
                            # Create zip file
                            zip_name = f"{topic.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M')}"
                            zip_path = create_zip_file(folder_path, zip_name)
                            
                            # Upload to Google Drive
                            folder_id = st.session_state.drive_folder_id
                            if not folder_id:
                                # Create folder if not exists
                                folder_name = f"Nigeria_Data_{datetime.now().strftime('%Y%m%d')}"
                                folder_result = st.session_state.google_drive_auth.create_folder(folder_name)
                                if folder_result:
                                    folder_id = folder_result['id']
                                    st.session_state.drive_folder_id = folder_id
                            
                            if folder_id:
                                # Upload zip file
                                upload_result = st.session_state.google_drive_auth.upload_zip_file(
                                    zip_path, f"{zip_name}.zip", folder_id
                                )
                                if upload_result:
                                    st.success(f"✅ Uploaded to Google Drive: [{upload_result['name']}]({upload_result['link']})")
                            
                            # Clean up zip file
                            if os.path.exists(zip_path):
                                os.remove(zip_path)
                        
                        progress_bar.progress(1.0)
                        status_text.text(f"✅ Scraping complete! Found {len(tables)} tables and {len(text_data)} text records.")
                        
                        st.balloons()
                        
                    else:
                        status_text.text("⚠️ No tables found. Try different search terms or sources.")
                        progress_bar.progress(1.0)
        
        with col2:
            if st.button("✖️ Clear Results", use_container_width=True, type="secondary"):
                st.session_state.current_topic = None
                st.session_state.scraped_data = None
                st.session_state.extracted_tables = []
                st.session_state.saved_files = []
                st.rerun()
        
        # Display results if available
        if st.session_state.scraped_data and st.session_state.extracted_tables:
            st.markdown("---")
            st.header("📋 Extracted Tables")
            
            # Display tables in tabs
            tabs = st.tabs([f"Table {i+1}" for i in range(len(st.session_state.extracted_tables))])
            
            for idx, (tab, table_data) in enumerate(zip(tabs, st.session_state.extracted_tables)):
                with tab:
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.dataframe(
                            table_data['dataframe'],
                            use_container_width=True,
                            height=400
                        )
                    
                    with col2:
                        metadata = table_data['metadata']
                        st.metric("Rows", metadata['rows'])
                        st.metric("Columns", metadata['columns'])
                        
                        # Download buttons
                        csv_data = table_data['dataframe'].to_csv(index=False).encode('utf-8')
                        excel_buffer = io.BytesIO()
                        table_data['dataframe'].to_excel(excel_buffer, index=False)
                        excel_data = excel_buffer.getvalue()
                        
                        col_download1, col_download2 = st.columns(2)
                        
                        with col_download1:
                            st.download_button(
                                label="📥 CSV",
                                data=csv_data,
                                file_name=f"{topic}_table_{idx+1}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                        
                        with col_download2:
                            st.download_button(
                                label="📥 Excel",
                                data=excel_data,
                                file_name=f"{topic}_table_{idx+1}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True
                            )
            
            # Download all data
            st.markdown("---")
            st.header("💾 Download All Data")
            
            if st.session_state.saved_files:
                # Create zip file of all data
                zip_name = f"{topic.replace(' ', '_')}_complete_{datetime.now().strftime('%Y%m%d_%H%M')}"
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("📦 Create ZIP Archive", use_container_width=True):
                        zip_path = create_zip_file(
                            st.session_state.scraped_data['folder_path'], 
                            zip_name
                        )
                        
                        with open(zip_path, "rb") as f:
                            zip_data = f.read()
                        
                        st.download_button(
                            label="📥 Download ZIP",
                            data=zip_data,
                            file_name=f"{zip_name}.zip",
                            mime="application/zip",
                            use_container_width=True
                        )
                
                with col2:
                    # Individual file downloads
                    with st.expander("📄 Download Individual Files"):
                        for file_info in st.session_state.saved_files:
                            if 'csv_path' in file_info and os.path.exists(file_info['csv_path']):
                                with open(file_info['csv_path'], 'rb') as f:
                                    csv_data = f.read()
                                
                                st.download_button(
                                    label=f"📥 {os.path.basename(file_info['csv_path'])}",
                                    data=csv_data,
                                    file_name=os.path.basename(file_info['csv_path']),
                                    mime="text/csv",
                                    use_container_width=True,
                                    key=f"csv_{file_info['csv_path']}"
                                )
    
    # Log display section
    with st.expander("📝 Scraping Log", expanded=False):
        # Get logs from thread-safe logger
        logs = logger.get_all_logs()
        
        if logs:
            # Display logs in reverse chronological order
            for log in reversed(logs[-50:]):  # Show last 50 logs
                st.text(log)
        else:
            st.info("No logs yet. Start scraping to see activity.")
        
        if st.button("Clear Logs", use_container_width=True):
            logger.logs = []
            st.rerun()
    
    # Footer
    st.markdown("---")
    st.markdown("""
        <div style="text-align: center; color: #666; padding: 1rem;">
            <p><strong>Nigeria Data Table Scraper</strong> | For research and analysis purposes only</p>
            <p>Data sources: NBS, World Bank, UN, and other official sources</p>
            <p style="font-size: 0.8rem;">⚠️ Respect website terms of service and robots.txt files</p>
        </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    # Create necessary directories
    os.makedirs("data", exist_ok=True)
    os.makedirs("tables", exist_ok=True)
    
    main()