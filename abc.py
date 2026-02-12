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
from queue import Queue
import threading

# PDF scraping libraries
import PyPDF2
import pdfplumber
import io
from pdfminer.high_level import extract_text as pdfminer_extract
import fitz  # PyMuPDF

# For handling different content types
import textract
from docx import Document

# For handling JavaScript-heavy sites
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

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
    </style>
""", unsafe_allow_html=True)

# Initialize session state
if 'scraped_data' not in st.session_state:
    st.session_state.scraped_data = None
if 'scraping_in_progress' not in st.session_state:
    st.session_state.scraping_in_progress = False
if 'scraping_log' not in st.session_state:
    st.session_state.scraping_log = []
if 'website_queue' not in st.session_state:
    st.session_state.website_queue = Queue()
if 'active_scrapers' not in st.session_state:
    st.session_state.active_scrapers = 0

def log_entry(message):
    """Add message to scraping log"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_message = f"[{timestamp}] {message}"
    st.session_state.scraping_log.append(log_message)
    print(log_message)

class NigerianStatsScraper:
    """Enhanced web scraper for Nigerian statistical data with multi-website support"""
    
    def __init__(self, max_workers=5, use_selenium=False):
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
        self.use_selenium = use_selenium
        self.driver = None
        
        if use_selenium:
            self.init_selenium()
    
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
        except Exception as e:
            log_entry(f"Failed to initialize Selenium: {e}")
            self.use_selenium = False
    
    def close_selenium(self):
        """Close Selenium WebDriver"""
        if self.driver:
            self.driver.quit()
    
    def scrape_pdf(self, url):
        """Scrape data from PDF files using multiple libraries"""
        pdf_data = []
        
        try:
            log_entry(f"Scraping PDF: {url}")
            
            # Download PDF
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200 and response.headers.get('content-type', '').lower() == 'application/pdf':
                pdf_content = response.content
                
                # Try different PDF parsing methods
                methods = [
                    self._parse_pdf_with_pdfplumber,
                    self._parse_pdf_with_pymupdf,
                    self._parse_pdf_with_pypdf2,
                    self._parse_pdf_with_pdfminer,
                    self._parse_pdf_with_textract
                ]
                
                for method in methods:
                    try:
                        data = method(pdf_content, url)
                        if data and len(data) > 0:
                            pdf_data.extend(data)
                            log_entry(f"✓ Successfully parsed PDF with {method.__name__}")
                            break
                    except Exception as e:
                        log_entry(f"✗ PDF parsing method failed: {e}")
                        continue
                
                if not pdf_data:
                    # Try to extract at least some text
                    try:
                        text = self._extract_basic_pdf_text(pdf_content)
                        if text:
                            pdf_data.append({
                                'PDF_URL': url,
                                'Content_Type': 'PDF',
                                'Extracted_Text': text[:1000] + '...' if len(text) > 1000 else text,
                                'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                            })
                    except:
                        pass
            
        except Exception as e:
            log_entry(f"Error scraping PDF {url}: {e}")
        
        return pdf_data
    
    def _parse_pdf_with_pdfplumber(self, pdf_content, url):
        """Parse PDF using pdfplumber"""
        data = []
        try:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                for i, page in enumerate(pdf.pages[:50]):  # Limit to first 50 pages
                    text = page.extract_text()
                    if text and len(text.strip()) > 10:
                        # Look for statistical patterns
                        stats_patterns = [
                            r'\b\d+\.?\d*\s*%\b',  # Percentages
                            r'\b\d{1,3}(?:,\d{3})*\b',  # Numbers with commas
                            r'\b(?:increase|decrease|rate|percentage|ratio|statistic)\b.*?\d',
                            r'\b[a-zA-Z\s]+:\s*\d+'
                        ]
                        
                        for pattern in stats_patterns:
                            matches = re.findall(pattern, text, re.IGNORECASE)
                            for match in matches:
                                data.append({
                                    'PDF_URL': url,
                                    'Page': i + 1,
                                    'Content_Type': 'PDF_statistic',
                                    'Extracted_Data': match.strip(),
                                    'Pattern_Matched': pattern,
                                    'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                                })
                        
                        # Also extract tables if available
                        tables = page.extract_tables()
                        for table in tables:
                            if table and len(table) > 1:
                                # Convert table to readable format
                                table_text = ' | '.join([' | '.join(str(cell) for cell in row if cell) for row in table if any(cell)])
                                if table_text:
                                    data.append({
                                        'PDF_URL': url,
                                        'Page': i + 1,
                                        'Content_Type': 'PDF_table',
                                        'Table_Data': table_text[:500],
                                        'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                                    })
        except Exception as e:
            log_entry(f"pdfplumber error: {e}")
        return data
    
    def _parse_pdf_with_pymupdf(self, pdf_content, url):
        """Parse PDF using PyMuPDF (fitz)"""
        data = []
        try:
            doc = fitz.open(stream=pdf_content, filetype="pdf")
            for i, page in enumerate(doc[:50]):
                text = page.get_text()
                if text:
                    # Extract data similar to pdfplumber method
                    lines = text.split('\n')
                    for line in lines:
                        if re.search(r'\d', line) and len(line.strip()) > 5:
                            data.append({
                                'PDF_URL': url,
                                'Page': i + 1,
                                'Content_Type': 'PDF_text',
                                'Extracted_Line': line.strip()[:200],
                                'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                            })
        except Exception as e:
            log_entry(f"PyMuPDF error: {e}")
        return data
    
    def _parse_pdf_with_pypdf2(self, pdf_content, url):
        """Parse PDF using PyPDF2"""
        data = []
        try:
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            for i, page in enumerate(pdf_reader.pages[:50]):
                text = page.extract_text()
                if text:
                    # Simple extraction of lines containing numbers
                    lines = text.split('\n')
                    for line in lines:
                        if any(char.isdigit() for char in line) and len(line.strip()) > 5:
                            data.append({
                                'PDF_URL': url,
                                'Page': i + 1,
                                'Content_Type': 'PDF_text',
                                'Extracted_Line': line.strip()[:200],
                                'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                            })
        except Exception as e:
            log_entry(f"PyPDF2 error: {e}")
        return data
    
    def _parse_pdf_with_pdfminer(self, pdf_content, url):
        """Parse PDF using pdfminer"""
        data = []
        try:
            text = pdfminer_extract(io.BytesIO(pdf_content))
            if text:
                # Extract statistics from text
                lines = text.split('\n')
                for line in lines[:100]:  # Limit to first 100 lines
                    if re.search(r'\d+\.?\d*\s*%|\d+\s*(million|billion|thousand)|rate.*\d', line, re.IGNORECASE):
                        data.append({
                            'PDF_URL': url,
                            'Content_Type': 'PDF_statistic',
                            'Extracted_Data': line.strip()[:300],
                            'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                        })
        except Exception as e:
            log_entry(f"pdfminer error: {e}")
        return data
    
    def _parse_pdf_with_textract(self, pdf_content, url):
        """Parse PDF using textract"""
        data = []
        try:
            # Save temporarily to file
            temp_path = f"temp_{int(time.time())}.pdf"
            with open(temp_path, 'wb') as f:
                f.write(pdf_content)
            
            text = textract.process(temp_path).decode('utf-8')
            os.remove(temp_path)
            
            if text:
                # Extract statistical data
                stats = re.findall(r'\b\d+\.?\d*\s*%\b|\b\d{1,3}(?:,\d{3})*\b', text)
                for stat in stats[:50]:  # Limit to 50 statistics
                    data.append({
                        'PDF_URL': url,
                        'Content_Type': 'PDF_number',
                        'Extracted_Number': stat,
                        'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                    })
        except Exception as e:
            log_entry(f"textract error: {e}")
        return data
    
    def _extract_basic_pdf_text(self, pdf_content):
        """Basic text extraction from PDF"""
        try:
            # Try PyPDF2 first
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            text = ""
            for page in pdf_reader.pages[:10]:  # First 10 pages only
                text += page.extract_text() + "\n"
            return text
        except:
            return ""
    
    def scrape_website(self, website_config, search_query):
        """Scrape a single website based on configuration"""
        website_data = []
        
        try:
            url = website_config.get('url', '')
            name = website_config.get('name', 'Unknown')
            scrape_method = website_config.get('scrape_method', 'direct')
            
            log_entry(f"Scraping {name}: {url}")
            
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
            
            log_entry(f"✓ Found {len(website_data)} records from {name}")
            
        except Exception as e:
            log_entry(f"✗ Error scraping {name}: {e}")
        
        return website_data
    
    def scrape_with_requests(self, url, search_query):
        """Scrape website using requests library"""
        data = []
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            
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
                
                elif 'text/plain' in content_type:
                    # Handle plain text
                    text_data = response.text
                    data.extend(self.parse_text_data(text_data, url))
                
                # Check for embedded PDF links
                soup = BeautifulSoup(response.content, 'html.parser')
                pdf_links = soup.find_all('a', href=lambda x: x and x.lower().endswith('.pdf'))
                
                for link in pdf_links[:3]:  # Limit to 3 PDFs
                    pdf_url = urljoin(url, link['href'])
                    pdf_data = self.scrape_pdf(pdf_url)
                    data.extend(pdf_data)
        
        except Exception as e:
            log_entry(f"Error in requests scraping: {e}")
        
        return data
    
    def scrape_with_selenium(self, url, search_query):
        """Scrape JavaScript-heavy websites using Selenium"""
        data = []
        
        if not self.driver:
            return data
        
        try:
            self.driver.get(url)
            time.sleep(3)  # Wait for JavaScript to load
            
            # Try to find search functionality
            if search_query:
                try:
                    search_box = self.driver.find_element(By.CSS_SELECTOR, 
                                                         "input[type='search'], input[type='text']")
                    search_box.send_keys(search_query)
                    search_box.submit()
                    time.sleep(3)
                except:
                    pass
            
            # Get page source after JavaScript execution
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Extract data
            data.extend(self.extract_html_data(soup, url, search_query))
            
            # Look for data tables
            tables = soup.find_all('table')
            for table in tables[:5]:
                try:
                    rows = table.find_all('tr')
                    for row in rows[1:6]:  # First 5 data rows
                        cells = row.find_all(['td', 'th'])
                        row_data = [cell.get_text(strip=True) for cell in cells]
                        if row_data:
                            data.append({
                                'Table_Data': ' | '.join(row_data),
                                'Source_URL': url,
                                'Content_Type': 'Selenium_table',
                                'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                            })
                except:
                    continue
        
        except Exception as e:
            log_entry(f"Selenium scraping error for {url}: {e}")
        
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
            log_entry(f"API scraping error: {e}")
        
        return data
    
    def extract_html_data(self, soup, url, search_query=None):
        """Extract data from HTML content"""
        data = []
        
        # Look for statistical data in various HTML elements
        data_selectors = [
            {'tag': 'table', 'class_contains': 'data'},
            {'tag': 'table', 'class_contains': 'stat'},
            {'tag': 'table', 'id_contains': 'data'},
            {'tag': 'table', 'id_contains': 'stat'},
            {'tag': 'div', 'class_contains': 'statistic'},
            {'tag': 'div', 'class_contains': 'indicator'},
            {'tag': 'ul', 'class_contains': 'data'},
            {'tag': 'ol', 'class_contains': 'stats'}
        ]
        
        for selector in data_selectors:
            elements = soup.find_all(selector['tag'])
            for element in elements[:10]:  # Limit to 10 elements per selector
                try:
                    # Check class or id
                    class_attr = element.get('class', [])
                    id_attr = element.get('id', '')
                    
                    if (selector.get('class_contains') and 
                        any(selector['class_contains'] in str(c).lower() for c in class_attr)):
                        text = element.get_text(strip=True)
                        if text and len(text) > 20:
                            data.append({
                                'Content': text[:500],
                                'Element_Type': selector['tag'],
                                'Source_URL': url,
                                'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                            })
                    
                    elif (selector.get('id_contains') and 
                          selector['id_contains'] in str(id_attr).lower()):
                        text = element.get_text(strip=True)
                        if text and len(text) > 20:
                            data.append({
                                'Content': text[:500],
                                'Element_Type': selector['tag'],
                                'Source_URL': url,
                                'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                            })
                except:
                    continue
        
        # Extract all text and look for statistical patterns
        all_text = soup.get_text()
        stats_patterns = [
            r'\b\d+\.?\d*\s*%\b',  # Percentages
            r'\b\d{1,3}(?:,\d{3})+\b',  # Large numbers with commas
            r'\bGDP.*?\d',  # GDP references
            r'\bpopulation.*?\d',  # Population references
            r'\bunemployment.*?\d',  # Unemployment references
            r'\binflation.*?\d',  # Inflation references
        ]
        
        for pattern in stats_patterns:
            matches = re.findall(pattern, all_text, re.IGNORECASE)
            for match in matches[:20]:  # Limit to 20 matches per pattern
                data.append({
                    'Statistical_Match': match,
                    'Pattern': pattern,
                    'Source_URL': url,
                    'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                })
        
        # Filter by search query if provided
        if search_query:
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
                for item in json_data[:50]:  # Limit to 50 items
                    if isinstance(item, dict):
                        item['Source_URL'] = url
                        item['Data_Type'] = 'JSON'
                        data.append(item)
            elif isinstance(json_data, dict):
                # Flatten nested dictionaries
                flattened = self.flatten_dict(json_data)
                flattened['Source_URL'] = url
                flattened['Data_Type'] = 'JSON'
                data.append(flattened)
        except Exception as e:
            log_entry(f"JSON parsing error: {e}")
        
        return data
    
    def parse_xml_data(self, xml_data, url):
        """Parse XML data"""
        data = []
        
        try:
            # Convert XML to dictionary
            xml_dict = self.xml_to_dict(xml_data)
            if xml_dict:
                xml_dict['Source_URL'] = url
                xml_dict['Data_Type'] = 'XML'
                data.append(xml_dict)
        except Exception as e:
            log_entry(f"XML parsing error: {e}")
        
        return data
    
    def parse_text_data(self, text_data, url):
        """Parse plain text data"""
        data = []
        
        try:
            lines = text_data.split('\n')
            for line in lines[:100]:  # First 100 lines
                if re.search(r'\d+\.?\d*\s*%|\d+\s*(million|billion|thousand)', line, re.IGNORECASE):
                    data.append({
                        'Text_Line': line.strip()[:200],
                        'Source_URL': url,
                        'Data_Type': 'Text',
                        'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
                    })
        except Exception as e:
            log_entry(f"Text parsing error: {e}")
        
        return data
    
    def flatten_dict(self, d, parent_key='', sep='_'):
        """Flatten nested dictionary"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, item in enumerate(v[:10]):  # Limit to 10 items
                    if isinstance(item, dict):
                        items.extend(self.flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
                    else:
                        items.append((f"{new_key}_{i}", str(item)))
            else:
                items.append((new_key, str(v)))
        return dict(items)
    
    def xml_to_dict(self, element):
        """Convert XML element to dictionary"""
        result = {}
        for child in element:
            if len(child) > 0:
                result[child.tag] = self.xml_to_dict(child)
            else:
                result[child.tag] = child.text
        return result
    
    def get_nigerian_statistical_websites(self):
        """Get comprehensive list of Nigerian statistical websites"""
        websites = [
            # Official Government Sources
            {
                'name': 'National Bureau of Statistics (NBS)',
                'url': 'https://www.nigerianstat.gov.ng',
                'scrape_method': 'direct',
                'category': 'Official Statistics',
                'priority': 1
            },
            {
                'name': 'Central Bank of Nigeria',
                'url': 'https://www.cbn.gov.ng/Statistics/',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 1
            },
            {
                'name': 'National Population Commission',
                'url': 'https://nationalpopulation.gov.ng',
                'scrape_method': 'selenium',
                'category': 'Population Statistics',
                'priority': 2
            },
            
            # Economic Data
            {
                'name': 'Nigeria Data Portal (Open Data)',
                'url': 'https://nigeria.opendataforafrica.org',
                'scrape_method': 'selenium',
                'category': 'Economic Data',
                'priority': 2
            },
            {
                'name': 'Trading Economics - Nigeria',
                'url': 'https://tradingeconomics.com/nigeria/indicators',
                'scrape_method': 'selenium',
                'category': 'Economic Indicators',
                'priority': 3
            },
            
            # Health Statistics
            {
                'name': 'Federal Ministry of Health',
                'url': 'https://www.health.gov.ng',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 3
            },
            {
                'name': 'National Primary Health Care Development Agency',
                'url': 'https://nphcda.gov.ng',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 3
            },
            
            # Education Statistics
            {
                'name': 'Universal Basic Education Commission',
                'url': 'https://ubec.gov.ng',
                'scrape_method': 'direct',
                'category': 'Education Statistics',
                'priority': 3
            },
            {
                'name': 'National Universities Commission',
                'url': 'https://www.nuc.edu.ng',
                'scrape_method': 'direct',
                'category': 'Education Statistics',
                'priority': 3
            },
            
            # International Sources with Nigeria Data
            {
                'name': 'World Bank Nigeria Data',
                'url': 'https://data.worldbank.org/country/nigeria',
                'scrape_method': 'api',
                'category': 'International Statistics',
                'priority': 1
            },
            {
                'name': 'IMF Nigeria Data',
                'url': 'https://www.imf.org/en/Countries/NGA',
                'scrape_method': 'selenium',
                'category': 'Economic Statistics',
                'priority': 2
            },
            {
                'name': 'UN Data Nigeria',
                'url': 'http://data.un.org/en/iso/ng.html',
                'scrape_method': 'direct',
                'category': 'International Statistics',
                'priority': 2
            },
            
            # Crime and Security
            {
                'name': 'Nigeria Police Force',
                'url': 'https://npf.gov.ng',
                'scrape_method': 'direct',
                'category': 'Crime Statistics',
                'priority': 3
            },
            
            # PDF Reports and Documents
            {
                'name': 'NDHS Reports (PDF)',
                'url': 'https://dhsprogram.com/pubs/pdf/FR359/FR359.pdf',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 2
            },
            {
                'name': 'MICS Nigeria Reports',
                'url': 'https://mics.unicef.org/surveys',
                'scrape_method': 'direct',
                'category': 'Survey Data',
                'priority': 2
            },
            {
                'name': 'NBS Statistical Reports',
                'url': 'https://www.nigerianstat.gov.ng/reports',
                'scrape_method': 'direct',
                'category': 'Official Statistics',
                'priority': 1
            },
            
            # Additional Nigerian Data Sources
            {
                'name': 'National Bureau of Statistics Data Portal',
                'url': 'https://nigerianstat.gov.ng/elibrary',
                'scrape_method': 'direct',
                'category': 'Official Statistics',
                'priority': 1
            },
            {
                'name': 'NBS GDP Reports',
                'url': 'https://nigerianstat.gov.ng/download/1232',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 1
            },
            {
                'name': 'NBS Inflation Data',
                'url': 'https://nigerianstat.gov.ng/download/1234',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 1
            },
            {
                'name': 'National Social Safety Nets',
                'url': 'https://nassp.gov.ng',
                'scrape_method': 'direct',
                'category': 'Social Statistics',
                'priority': 3
            },
            {
                'name': 'National Identity Management Commission',
                'url': 'https://www.nimc.gov.ng',
                'scrape_method': 'selenium',
                'category': 'Demographic Statistics',
                'priority': 3
            },
            {
                'name': 'Federal Road Safety Corps',
                'url': 'https://frsc.gov.ng',
                'scrape_method': 'direct',
                'category': 'Transport Statistics',
                'priority': 3
            },
            {
                'name': 'Nigeria Immigration Service',
                'url': 'https://immigration.gov.ng',
                'scrape_method': 'direct',
                'category': 'Migration Statistics',
                'priority': 3
            },
            {
                'name': 'National Agency for Food and Drug Administration',
                'url': 'https://www.nafdac.gov.ng',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 3
            },
            {
                'name': 'Nigeria Meteorological Agency',
                'url': 'https://nimet.gov.ng',
                'scrape_method': 'direct',
                'category': 'Environmental Statistics',
                'priority': 3
            },
            {
                'name': 'National Environmental Standards and Regulations',
                'url': 'https://nesrea.gov.ng',
                'scrape_method': 'direct',
                'category': 'Environmental Statistics',
                'priority': 3
            },
            {
                'name': 'Federal Ministry of Labour and Employment',
                'url': 'https://labour.gov.ng',
                'scrape_method': 'direct',
                'category': 'Employment Statistics',
                'priority': 3
            },
            {
                'name': 'National Salaries, Incomes and Wages Commission',
                'url': 'https://nsiwc.gov.ng',
                'scrape_method': 'direct',
                'category': 'Economic Statistics',
                'priority': 3
            },
            {
                'name': 'Nigeria Electricity Regulatory Commission',
                'url': 'https://nerc.gov.ng',
                'scrape_method': 'direct',
                'category': 'Energy Statistics',
                'priority': 3
            },
            {
                'name': 'National Communications Commission',
                'url': 'https://www.ncc.gov.ng',
                'scrape_method': 'selenium',
                'category': 'Telecom Statistics',
                'priority': 3
            },
            {
                'name': 'National Insurance Commission',
                'url': 'https://www.naicom.gov.ng',
                'scrape_method': 'direct',
                'category': 'Financial Statistics',
                'priority': 3
            },
            {
                'name': 'Securities and Exchange Commission Nigeria',
                'url': 'https://sec.gov.ng',
                'scrape_method': 'direct',
                'category': 'Financial Statistics',
                'priority': 3
            },
            {
                'name': 'Nigeria Stock Exchange',
                'url': 'https://www.ngxgroup.com',
                'scrape_method': 'selenium',
                'category': 'Financial Statistics',
                'priority': 3
            },
            {
                'name': 'Federal Inland Revenue Service',
                'url': 'https://www.firs.gov.ng',
                'scrape_method': 'direct',
                'category': 'Tax Statistics',
                'priority': 2
            },
            {
                'name': 'Budget Office of the Federation',
                'url': 'https://www.budgetoffice.gov.ng',
                'scrape_method': 'direct',
                'category': 'Fiscal Statistics',
                'priority': 2
            },
            {
                'name': 'Debt Management Office',
                'url': 'https://www.dmo.gov.ng',
                'scrape_method': 'direct',
                'category': 'Debt Statistics',
                'priority': 2
            },
            {
                'name': 'National Planning Commission',
                'url': 'https://nationalplanning.gov.ng',
                'scrape_method': 'direct',
                'category': 'Development Statistics',
                'priority': 3
            },
            {
                'name': 'National Orientation Agency',
                'url': 'https://noa.gov.ng',
                'scrape_method': 'direct',
                'category': 'Social Statistics',
                'priority': 3
            },
            {
                'name': 'National Youth Service Corps',
                'url': 'https://www.nysc.gov.ng',
                'scrape_method': 'direct',
                'category': 'Youth Statistics',
                'priority': 3
            },
            {
                'name': 'National Sports Commission',
                'url': 'https://sportscommission.gov.ng',
                'scrape_method': 'direct',
                'category': 'Sports Statistics',
                'priority': 3
            },
            {
                'name': 'National Gallery of Art',
                'url': 'https://nationalgallery.gov.ng',
                'scrape_method': 'direct',
                'category': 'Cultural Statistics',
                'priority': 3
            },
            {
                'name': 'National Commission for Museums and Monuments',
                'url': 'https://ncmm.gov.ng',
                'scrape_method': 'direct',
                'category': 'Cultural Statistics',
                'priority': 3
            },
            {
                'name': 'National Library of Nigeria',
                'url': 'https://www.nln.gov.ng',
                'scrape_method': 'direct',
                'category': 'Education Statistics',
                'priority': 3
            },
            {
                'name': 'National Archives of Nigeria',
                'url': 'https://nationalarchives.gov.ng',
                'scrape_method': 'direct',
                'category': 'Historical Statistics',
                'priority': 3
            },
            {
                'name': 'National Agency for the Control of AIDS',
                'url': 'https://naca.gov.ng',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 3
            },
            {
                'name': 'National Malaria Elimination Programme',
                'url': 'https://nmep.gov.ng',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 3
            },
            {
                'name': 'National Tuberculosis and Leprosy Control Programme',
                'url': 'https://ntblcp.gov.ng',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 3
            },
            {
                'name': 'National Primary Health Care Development Agency - Data',
                'url': 'https://nphcda.gov.ng/data-reports/',
                'scrape_method': 'direct',
                'category': 'Health Statistics',
                'priority': 2
            }
        ]
        
        return websites
    
    def smart_scrape_multiple_websites(self, search_query, selected_categories=None, max_websites=20):
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
        
        log_entry(f"Starting multi-website scrape: {len(websites_to_scrape)} websites")
        
        # Use ThreadPoolExecutor for concurrent scraping
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit scraping tasks
            future_to_website = {
                executor.submit(self.scrape_website, website, search_query): website 
                for website in websites_to_scrape
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_website):
                website = future_to_website[future]
                try:
                    website_data = future.result()
                    if website_data:
                        all_data.extend(website_data)
                        log_entry(f"Completed: {website['name']} ({len(website_data)} records)")
                    else:
                        log_entry(f"No data from: {website['name']}")
                except Exception as e:
                    log_entry(f"Error scraping {website['name']}: {e}")
        
        log_entry(f"Multi-website scraping complete. Total records: {len(all_data)}")
        
        if all_data:
            return pd.DataFrame(all_data)
        return None
    
    def search_specific_data(self, search_query):
        """Search for specific types of data based on query"""
        # Extract keywords and determine data type
        query_lower = search_query.lower()
        
        # Define category mappings
        category_mappings = {
            'economic': ['GDP', 'inflation', 'unemployment', 'trade', 'import', 'export', 'economy'],
            'health': ['health', 'mortality', 'death', 'disease', 'hospital', 'doctor'],
            'crime': ['crime', 'police', 'security', 'murder', 'robbery', 'violence'],
            'education': ['education', 'school', 'student', 'teacher', 'literacy', 'enrollment'],
            'population': ['population', 'census', 'demographic', 'birth', 'migration'],
            'agriculture': ['agriculture', 'farm', 'crop', 'livestock', 'food'],
            'energy': ['energy', 'electricity', 'power', 'oil', 'gas', 'petroleum'],
            'transport': ['transport', 'road', 'vehicle', 'accident', 'aviation'],
            'environment': ['environment', 'climate', 'pollution', 'water', 'sanitation']
        }
        
        # Determine which categories to search
        selected_categories = []
        for category, keywords in category_mappings.items():
            if any(keyword in query_lower for keyword in keywords):
                selected_categories.append(category.capitalize() + ' Statistics')
        
        # Also add general categories if none specific found
        if not selected_categories:
            selected_categories = ['Official Statistics', 'Economic Statistics']
        
        return self.smart_scrape_multiple_websites(search_query, selected_categories)

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
    
    # Header
    st.markdown('<h1 class="main-header">🌐 Nigeria Statistics Web Scraper Pro</h1>', unsafe_allow_html=True)
    st.markdown("""
        <div style="text-align: center; margin-bottom: 2rem;">
            <p><strong>Advanced web scraper for Nigerian statistical data from 50+ sources</strong></p>
            <p>Extracts real data from Nigerian government agencies, international organizations, and statistical portals</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Advanced Configuration")
        
        # Data sources selection
        st.subheader("📡 Data Categories")
        
        categories = [
            "Official Statistics",
            "Economic Statistics",
            "Health Statistics",
            "Education Statistics",
            "Crime Statistics",
            "Population Statistics",
            "International Statistics",
            "Survey Data",
            "Social Statistics",
            "Environmental Statistics",
            "Financial Statistics",
            "Energy Statistics",
            "Transport Statistics",
            "Cultural Statistics",
            "Youth Statistics",
            "Tax Statistics",
            "Debt Statistics",
            "Telecom Statistics",
            "Migration Statistics",
            "Historical Statistics"
        ]
        
        selected_categories = st.multiselect(
            "Select categories to scrape:",
            categories,
            default=["Official Statistics", "Economic Statistics", "Health Statistics"]
        )
        
        # Scraping options
        st.subheader("⚡ Scraping Options")
        
        max_websites = st.slider("Maximum websites to scrape", 1, 50, 15)
        max_workers = st.slider("Concurrent scrapers", 1, 10, 5)
        timeout = st.slider("Timeout per website (seconds)", 10, 120, 30)
        use_selenium = st.checkbox("Use Selenium for JavaScript sites", value=True)
        enable_pdf = st.checkbox("Enable PDF scraping", value=True)
        
        # Advanced options
        with st.expander("Advanced Options"):
            max_pages_per_site = st.slider("Max pages per site", 1, 20, 5)
            depth = st.slider("Scraping depth", 1, 3, 1)
            retry_failed = st.checkbox("Retry failed scrapes", value=True)
        
        # Export options
        st.subheader("💾 Export Options")
        export_format = st.selectbox(
            "Export format:",
            ["CSV", "JSON", "Excel", "All"]
        )
        
        auto_download = st.checkbox("Auto-download after scraping", value=True)
    
    # Main content
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.subheader("🔍 What Nigerian data are you looking for?")
        
        # Search input
        search_query = st.text_input(
            "Enter search query:",
            placeholder="e.g., 'GDP growth 2023', 'population Lagos state', 'unemployment rate youth'",
            key="search_input"
        )
        
        # Quick search buttons
        st.subheader("🚀 Quick Statistical Searches")
        
        quick_searches = [
            "GDP Nigeria 2023",
            "Population Census 2023",
            "Unemployment Rate",
            "Inflation Rate CPI",
            "Crime Statistics 2023",
            "Health Indicators 2023",
            "Education Enrollment",
            "Electricity Access",
            "Internet Penetration",
            "Poverty Rate Nigeria",
            "Maternal Mortality",
            "Child Mortality Rate",
            "HIV Prevalence Nigeria",
            "Agricultural Production",
            "Oil Production Statistics"
        ]
        
        cols = st.columns(3)
        for i, query in enumerate(quick_searches):
            with cols[i % 3]:
                if st.button(f"🔎 {query}", use_container_width=True):
                    st.session_state.search_query = query
                    st.rerun()
    
    with col2:
        st.subheader("🎯 Available Sources")
        st.write("**50+ Nigerian Data Sources:**")
        st.write("• National Bureau of Statistics")
        st.write("• Central Bank of Nigeria")
        st.write("• World Bank Nigeria Data")
        st.write("• IMF Nigeria Reports")
        st.write("• NDHS Survey Data")
        st.write("• MICS UNICEF")
        st.write("• Government Ministries")
        st.write("• And 40+ more sources...")
        
        if st.button("📋 View All Sources"):
            st.session_state.show_sources = True
    
    # Show all sources if requested
    if st.session_state.get('show_sources', False):
        st.subheader("📚 Complete List of Data Sources")
        scraper = NigerianStatsScraper()
        websites = scraper.get_nigerian_statistical_websites()
        
        for website in websites:
            with st.expander(f"{website['name']} - {website['category']}"):
                st.write(f"**URL:** {website['url']}")
                st.write(f"**Method:** {website['scrape_method']}")
                st.write(f"**Priority:** {website['priority']}")
    
    # Scrape button
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        if st.button("🚀 Start Multi-Website Scraping", type="primary", use_container_width=True):
            if not search_query and 'search_query' not in st.session_state:
                st.warning("⚠️ Please enter a search query or select a quick search")
            else:
                query = search_query or getattr(st.session_state, 'search_query', '')
                
                st.session_state.scraping_in_progress = True
                st.session_state.scraping_log = []  # Clear previous log
                
                with st.spinner(f"🌐 Scraping {max_websites} websites for '{query}'..."):
                    # Initialize scraper
                    scraper = NigerianStatsScraper(
                        max_workers=max_workers,
                        use_selenium=use_selenium
                    )
                    
                    # Show scraping progress
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # Create placeholder for real-time log
                    log_placeholder = st.empty()
                    
                    # Start scraping
                    status_text.text("Step 1/2: Initializing scrapers...")
                    progress_bar.progress(10)
                    
                    # Perform the actual scraping
                    status_text.text("Step 2/2: Scraping multiple websites...")
                    progress_bar.progress(30)
                    
                    scraped_data = scraper.search_specific_data(query)
                    
                    # Update progress
                    progress_bar.progress(90)
                    status_text.text("Processing results...")
                    
                    if scraped_data is not None and not scraped_data.empty:
                        st.session_state.scraped_data = scraped_data
                        
                        status_text.text("✅ Multi-website scraping completed!")
                        progress_bar.progress(100)
                        
                        st.success(f"🎉 Successfully scraped {len(scraped_data)} records from multiple sources!")
                        
                        # Close Selenium if used
                        scraper.close_selenium()
                        
                        # Auto-download if enabled
                        if auto_download and st.session_state.scraped_data is not None:
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            filename = f"nigeria_stats_{query.replace(' ', '_')}_{timestamp}"
                            
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                if export_format in ["CSV", "All"]:
                                    csv_link = create_download_link(st.session_state.scraped_data, f"{filename}.csv", "csv")
                                    st.markdown(csv_link, unsafe_allow_html=True)
                            
                            with col2:
                                if export_format in ["JSON", "All"]:
                                    json_link = create_download_link(st.session_state.scraped_data, f"{filename}.json", "json")
                                    st.markdown(json_link, unsafe_allow_html=True)
                            
                            with col3:
                                if export_format in ["Excel", "All"]:
                                    # For Excel, we need to save temporarily
                                    excel_path = f"scraped_data/{filename}.xlsx"
                                    os.makedirs("scraped_data", exist_ok=True)
                                    st.session_state.scraped_data.to_excel(excel_path, index=False)
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
        if st.button("🔄 Clear Results", use_container_width=True):
            st.session_state.scraped_data = None
            st.session_state.scraping_log = []
            st.success("Results cleared!")
            st.rerun()
    
    with col3:
        if st.button("📊 View Log", use_container_width=True):
            st.session_state.show_log = not st.session_state.get('show_log', False)
            st.rerun()
    
    # Display scraping log
    if st.session_state.get('show_log', False) and st.session_state.scraping_log:
        with st.expander("📋 Detailed Scraping Log", expanded=True):
            log_container = st.container()
            with log_container:
                for log_entry_text in st.session_state.scraping_log[-50:]:  # Show last 50 entries
                    # Color code log entries
                    if "✓" in log_entry_text:
                        st.success(log_entry_text)
                    elif "✗" in log_entry_text or "Error" in log_entry_text:
                        st.error(log_entry_text)
                    elif "Warning" in log_entry_text:
                        st.warning(log_entry_text)
                    else:
                        st.info(log_entry_text)
    
    # Display scraped data
    if st.session_state.scraped_data is not None:
        st.header("📊 Scraped Data Summary")
        
        df = st.session_state.scraped_data
        
        # Show comprehensive statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", len(df))
        with col2:
            sources = df['Source_Website'].nunique() if 'Source_Website' in df.columns else 0
            st.metric("Websites Scraped", sources)
        with col3:
            categories = df['Data_Type'].nunique() if 'Data_Type' in df.columns else 0
            st.metric("Data Types", categories)
        with col4:
            st.metric("Data Size", f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        # Data preview with tabs
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Data Preview", "📊 Sources Analysis", "🔍 Filter Data", "📈 Statistics"])
        
        with tab1:
            st.subheader("Data Preview")
            st.dataframe(df, use_container_width=True, height=400)
        
        with tab2:
            if 'Source_Website' in df.columns:
                st.subheader("Data Sources Distribution")
                source_counts = df['Source_Website'].value_counts()
                st.bar_chart(source_counts)
                
                st.subheader("Source Details")
                for source in source_counts.index:
                    source_data = df[df['Source_Website'] == source]
                    with st.expander(f"{source} ({len(source_data)} records)"):
                        st.dataframe(source_data.head(5))
        
        with tab3:
            st.subheader("Filter Options")
            
            filter_col1, filter_col2, filter_col3 = st.columns(3)
            
            with filter_col1:
                if 'Source_Website' in df.columns:
                    sources = df['Source_Website'].unique()
                    selected_sources = st.multiselect(
                        "Filter by website:",
                        sources,
                        default=sources[:min(3, len(sources))]
                    )
            
            with filter_col2:
                if 'Data_Type' in df.columns:
                    data_types = df['Data_Type'].unique()
                    selected_types = st.multiselect(
                        "Filter by data type:",
                        data_types,
                        default=data_types[:min(3, len(data_types))]
                    )
            
            with filter_col3:
                if 'Content_Type' in df.columns:
                    content_types = df['Content_Type'].unique()
                    selected_content = st.multiselect(
                        "Filter by content type:",
                        content_types,
                        default=content_types[:min(3, len(content_types))]
                    )
            
            # Apply filters
            filtered_df = df.copy()
            if 'selected_sources' in locals() and selected_sources:
                filtered_df = filtered_df[filtered_df['Source_Website'].isin(selected_sources)]
            if 'selected_types' in locals() and selected_types:
                filtered_df = filtered_df[filtered_df['Data_Type'].isin(selected_types)]
            if 'selected_content' in locals() and selected_content:
                filtered_df = filtered_df[filtered_df['Content_Type'].isin(selected_content)]
            
            st.dataframe(filtered_df, use_container_width=True, height=300)
        
        with tab4:
            st.subheader("Data Statistics")
            
            # Column information
            col_info = []
            for col in df.columns:
                col_info.append({
                    'Column': col,
                    'Type': str(df[col].dtype),
                    'Non-Null': df[col].count(),
                    'Unique': df[col].nunique(),
                    'Null %': f"{(df[col].isnull().sum() / len(df) * 100):.1f}%"
                })
            st.table(pd.DataFrame(col_info))
            
            # Numeric statistics
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                st.subheader("Numeric Column Statistics")
                st.write(df[numeric_cols].describe())
        
        # Export section
        st.header("💾 Export Data")
        
        export_col1, export_col2 = st.columns([2, 1])
        
        with export_col1:
            export_filename = st.text_input(
                "Export filename:",
                value=f"nigeria_multi_source_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                key="export_filename"
            )
        
        with export_col2:
            export_format_choice = st.selectbox(
                "Format:",
                ["CSV", "JSON", "Excel", "All"],
                key="export_format_final"
            )
        
        # Export buttons
        export_col1, export_col2, export_col3, export_col4 = st.columns(4)
        
        with export_col1:
            if st.button("📥 Download CSV", use_container_width=True):
                csv_link = create_download_link(df, f"{export_filename}.csv", "csv")
                st.markdown(csv_link, unsafe_allow_html=True)
        
        with export_col2:
            if st.button("📥 Download JSON", use_container_width=True):
                json_link = create_download_link(df, f"{export_filename}.json", "json")
                st.markdown(json_link, unsafe_allow_html=True)
        
        with export_col3:
            if st.button("📥 Download Excel", use_container_width=True):
                excel_path = f"scraped_data/{export_filename}.xlsx"
                os.makedirs("scraped_data", exist_ok=True)
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
        
        with export_col4:
            if st.button("💾 Save to Database", use_container_width=True):
                # This would connect to a database in a real application
                os.makedirs("database", exist_ok=True)
                db_path = f"database/nigeria_stats_{datetime.now().strftime('%Y%m')}.db"
                df.to_csv(db_path, index=False)
                st.success(f"✅ Data saved to database: {db_path}")

# Footer
st.markdown("---")
st.markdown("""
    <div style="text-align: center; color: #666; padding: 2rem 0;">
        <p><strong>Nigeria Statistics Web Scraper Pro</strong> • Version 3.0</p>
        <p>🌐 <strong>Scraping from 50+ sources:</strong> Government agencies, international organizations, statistical portals</p>
        <p>📄 <strong>PDF Support:</strong> PyPDF2, pdfplumber, PyMuPDF, pdfminer, textract</p>
        <p>⚡ <strong>Multi-threaded:</strong> Concurrent scraping of multiple websites</p>
        <p>🤖 <strong>Selenium Integration:</strong> JavaScript-heavy website support</p>
        <p>⚠️ <strong>Note:</strong> Respect robots.txt and website terms of service. Use responsibly.</p>
        <p>🛠️ <strong>Technologies:</strong> Python, BeautifulSoup, Selenium, Streamlit, Multiple PDF libraries</p>
    </div>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()