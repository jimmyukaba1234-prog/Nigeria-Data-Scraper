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
    },
    "Sports": {
        "icon": "⚽",
        "search_terms": ["sports", "football", "athletics", "NFF", "stadia", "tournaments"],
        "keywords": ["sports funding", "athlete performance", "sports facilities", "international competitions"]
    },
    "Entertainment": {
        "icon": "🎬",
        "search_terms": ["entertainment", "Nollywood", "music", "film", "arts", "culture"],
        "keywords": ["film production", "music industry", "creative economy", "cultural events"]
    },
    "Technology": {
        "icon": "💻",
        "search_terms": ["technology", "tech", "innovation", "startups", "digital", "ICT"],
        "keywords": ["tech startups", "ICT contribution", "digital economy", "innovation index"]
    },
    "Real Estate": {
        "icon": "🏠",
        "search_terms": ["real estate", "property", "housing", "mortgage", "construction"],
        "keywords": ["property prices", "housing deficit", "construction permits", "mortgage loans"]
    },
    "Manufacturing": {
        "icon": "🏭",
        "search_terms": ["manufacturing", "industry", "factories", "production", "PMI"],
        "keywords": ["manufacturing output", "industrial production", "factory capacity", "sector growth"]
    },
    "Tourism": {
        "icon": "🏨",
        "search_terms": ["tourism", "hotels", "travel", "visitors", "heritage"],
        "keywords": ["tourist arrivals", "hotel occupancy", "tourism revenue", "cultural sites"]
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
    
    # Header
    st.markdown('<h1 class="main-header">🇳🇬 Nigeria Data Table Scraper</h1>', unsafe_allow_html=True)
    st.markdown("""
        <div style="text-align: center; margin-bottom: 2rem;">
            <p><strong>Extract tables and data for specific Nigerian topics</strong></p>
            <p>Click on any topic below to start scraping relevant data tables</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Settings")
        
        max_sources = st.slider("Maximum sources per topic", 1, 10, 5)
        
        st.header("📊 Export Options")
        export_formats = st.multiselect(
            "File formats:",
            ["CSV", "Excel", "JSON"],
            default=["CSV", "Excel"]
        )
        
        if st.button("🔄 Clear All Data", type="secondary"):
            st.session_state.current_topic = None
            st.session_state.scraped_data = None
            st.session_state.extracted_tables = []
            st.session_state.saved_files = []
            st.session_state.scraping_log = []
            st.success("Data cleared!")
            st.rerun()
    
    # Main content - Topic Selection
    st.header("🎯 Select a Nigerian Topic")
    
    # Create topic grid
    cols = st.columns(4)
    for idx, (topic, info) in enumerate(NIGERIAN_TOPICS.items()):
        with cols[idx % 4]:
            if st.button(
                f"{info['icon']}\n\n**{topic}**",
                key=f"topic_{topic}",
                use_container_width=True
            ):
                st.session_state.current_topic = topic
                st.session_state.scraping_log = []
                st.rerun()
    
    # If topic is selected, show scraping interface
    if st.session_state.current_topic:
        topic = st.session_state.current_topic
        topic_info = NIGERIAN_TOPICS[topic]
        
        st.markdown(f"## {topic_info['icon']} {topic}")
        st.info(f"**Search Terms:** {', '.join(topic_info['search_terms'])}")
        
        # Custom search input
        custom_search = st.text_input(
            "Add custom search terms (optional):",
            placeholder="e.g., 'rice production 2023', 'bank profits Q4'",
            key="custom_search"
        )
        
        # Combine search terms
        search_terms = topic_info['search_terms'] + topic_info['keywords']
        if custom_search:
            search_terms.extend([term.strip() for term in custom_search.split(',')])
        
        # Scrape button
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            if st.button(f"🚀 Scrape {topic} Data", type="primary", use_container_width=True):
                with st.spinner(f"Scraping {topic} data from multiple sources..."):
                    # Initialize scraper
                    scraper = NigerianDataScraper(logger=logger)
                    
                    # Scrape the topic
                    text_data, tables = scraper.scrape_topic(topic, search_terms, max_sources)
                    
                    # Save the data
                    if tables or text_data:
                        folder, saved_files, summary = scraper.save_topic_data(
                            topic, tables, text_data, search_terms
                        )
                        
                        st.session_state.scraped_data = text_data
                        st.session_state.extracted_tables = tables
                        st.session_state.saved_files = saved_files
                        st.session_state.save_folder = folder
                        st.session_state.scrape_summary = summary
                        
                        st.success(f"✅ Successfully extracted {len(tables)} tables and {len(text_data)} text records!")
                    else:
                        st.warning("⚠️ No data found for this topic. Try different search terms.")
        
        with col2:
            if st.button("📋 View Log", use_container_width=True):
                st.session_state.show_log = not st.session_state.get('show_log', False)
        
        with col3:
            if st.button("🗑️ Clear Topic", type="secondary", use_container_width=True):
                st.session_state.current_topic = None
                st.rerun()
        
        # Show scraping log
        logs = logger.get_logs()
        if logs:
            st.session_state.scraping_log.extend(logs)
        
        if st.session_state.get('show_log', False) and st.session_state.scraping_log:
            with st.expander("📋 Scraping Log", expanded=True):
                for log in st.session_state.scraping_log[-20:]:
                    if "✅" in log or "Found" in log:
                        st.success(log)
                    elif "⚠️" in log or "Could not" in log:
                        st.warning(log)
                    elif "❌" in log or "Error" in log:
                        st.error(log)
                    else:
                        st.info(log)
        
        # Display results if we have data
        if st.session_state.extracted_tables or st.session_state.scraped_data:
            st.markdown("---")
            st.header("📊 Extracted Data")
            
            # Show summary
            if hasattr(st.session_state, 'scrape_summary'):
                summary = st.session_state.scrape_summary
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Tables Found", summary['tables_found'])
                with col2:
                    st.metric("Text Records", summary['text_records'])
                with col3:
                    st.metric("Files Created", len(summary['files']))
                with col4:
                    st.metric("Topic", summary['topic'])
            
            # Create tabs for different views
            tab1, tab2, tab3, tab4 = st.tabs(["📋 Tables", "📝 Text Data", "🔍 Browse", "💾 Download"])
            
            with tab1:
                if st.session_state.extracted_tables:
                    st.subheader(f"Extracted Tables ({len(st.session_state.extracted_tables)})")
                    
                    # Table selector
                    table_options = [
                        f"Table {i+1}: {t['metadata']['table_name']} "
                        f"({t['metadata']['rows']} rows, {t['metadata']['columns']} columns)"
                        for i, t in enumerate(st.session_state.extracted_tables)
                    ]
                    
                    selected_idx = st.selectbox(
                        "Select a table to preview:",
                        range(len(table_options)),
                        format_func=lambda x: table_options[x]
                    )
                    
                    if selected_idx is not None:
                        table = st.session_state.extracted_tables[selected_idx]
                        
                        # Show table info
                        st.write(f"**Source:** {table['metadata']['source_url']}")
                        st.write(f"**Extracted:** {table['metadata']['scrape_date']}")
                        
                        # Show the table
                        st.dataframe(table['dataframe'], use_container_width=True, height=400)
                else:
                    st.info("No tables extracted")
            
            with tab2:
                if st.session_state.scraped_data:
                    st.subheader("Extracted Text Data")
                    df = pd.DataFrame(st.session_state.scraped_data)
                    st.dataframe(df, use_container_width=True, height=400)
                else:
                    st.info("No text data extracted")
            
            with tab3:
                if st.session_state.extracted_tables:
                    st.subheader("Browse All Tables")
                    
                    for i, table in enumerate(st.session_state.extracted_tables):
                        with st.expander(f"Table {i+1}: {table['metadata']['table_name']}"):
                            st.write(f"**Source:** {table['metadata']['source_url']}")
                            st.write(f"**Size:** {table['metadata']['rows']} × {table['metadata']['columns']}")
                            
                            # Show first few rows
                            st.dataframe(table['dataframe'].head(), use_container_width=True)
            
            with tab4:
                st.subheader("Download Options")
                
                if hasattr(st.session_state, 'save_folder'):
                    # Create zip of all files
                    import zipfile
                    import tempfile
                    
                    zip_filename = f"{topic}_data_{datetime.now().strftime('%Y%m%d')}.zip"
                    
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp_zip:
                        with zipfile.ZipFile(tmp_zip.name, 'w') as zipf:
                            for root, dirs, files in os.walk(st.session_state.save_folder):
                                for file in files:
                                    file_path = os.path.join(root, file)
                                    arcname = os.path.relpath(file_path, st.session_state.save_folder)
                                    zipf.write(file_path, arcname)
                    
                    # Download button for zip
                    with open(tmp_zip.name, 'rb') as f:
                        zip_bytes = f.read()
                    
                    st.download_button(
                        label="📦 Download All Files (ZIP)",
                        data=zip_bytes,
                        file_name=zip_filename,
                        mime="application/zip",
                        use_container_width=True
                    )
                    
                    # Individual file downloads
                    st.subheader("Individual Files")
                    
                    for saved_file in st.session_state.saved_files:
                        if 'csv_path' in saved_file:
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                st.write(f"**{os.path.basename(saved_file['csv_path'])}**")
                                if 'rows' in saved_file:
                                    st.caption(f"{saved_file['rows']} rows × {saved_file['columns']} columns")
                            with col2:
                                # CSV download
                                with open(saved_file['csv_path'], 'rb') as f:
                                    csv_bytes = f.read()
                                st.download_button(
                                    label="📥 CSV",
                                    data=csv_bytes,
                                    file_name=os.path.basename(saved_file['csv_path']),
                                    mime="text/csv",
                                    key=f"csv_{saved_file['csv_path']}"
                                )
                                
                                # Excel download if available
                                if 'excel_path' in saved_file and os.path.exists(saved_file['excel_path']):
                                    with open(saved_file['excel_path'], 'rb') as f:
                                        excel_bytes = f.read()
                                    st.download_button(
                                        label="📥 Excel",
                                        data=excel_bytes,
                                        file_name=os.path.basename(saved_file['excel_path']),
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key=f"excel_{saved_file['excel_path']}"
                                    )
    
    else:
        # Show instructions when no topic is selected
        st.markdown("""
            <div class="info-box">
                <h3>📚 How to Use:</h3>
                <ol>
                    <li><strong>Select a topic</strong> from the grid above</li>
                    <li><strong>Add custom search terms</strong> if needed (optional)</li>
                    <li><strong>Click "Scrape [Topic] Data"</strong> to start extraction</li>
                    <li><strong>View and download</strong> the extracted tables and data</li>
                </ol>
                <p><strong>💡 Tip:</strong> Each topic has pre-defined search terms, but you can add your own for more specific results.</p>
            </div>
        """, unsafe_allow_html=True)
        
        # Show example outputs
        st.markdown("### 📊 Example Data You Can Extract:")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
                **🌾 Agriculture:**
                - Crop production statistics
                - Livestock population
                - Fertilizer usage data
                - Agricultural export tables
                
                **💰 Budget:**
                - Federal budget allocations
                - State government budgets
                - Capital expenditure tables
                - Revenue projections
                
                **📈 GDP:**
                - Quarterly GDP growth
                - Sectoral contributions
                - GDP per capita data
                - Historical GDP tables
                
                **🏥 Health:**
                - Disease prevalence rates
                - Hospital statistics
                - Health expenditure data
                - Mortality rate tables
            """)
        
        with col2:
            st.markdown("""
                **🎓 Education:**
                - School enrollment data
                - Literacy rate statistics
                - Education budget tables
                - University enrollment
                
                **🛢️ Oil and Gas:**
                - Crude oil production
                - Export volume data
                - Refinery output tables
                - Gas flaring statistics
                
                **📦 Trade:**
                - Import/export statistics
                - Trade balance data
                - Commodity trade tables
                - Trading partner data
                
                **👨‍💼 Unemployment:**
                - Unemployment rates by state
                - Youth unemployment data
                - Employment by sector
                - Labor force statistics
            """)

if __name__ == "__main__":
    # Create data directory
    os.makedirs("data", exist_ok=True)
    main()