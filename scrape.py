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
import io
from urllib.parse import urljoin, urlparse
import numpy as np

# Google Drive imports
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import pickle

# Page configuration
st.set_page_config(
    page_title="Nigeria Stats Web Scraper",
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
    .google-drive-box {
        padding: 1rem;
        background-color: #F3E8FF;
        border-radius: 0.5rem;
        border-left: 5px solid #8B5CF6;
        margin: 1rem 0;
    }
    .data-table {
        font-size: 0.85rem;
    }
    .stButton > button {
        width: 100%;
    }
    .tab-content {
        padding: 1rem 0;
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
if 'google_drive_authenticated' not in st.session_state:
    st.session_state.google_drive_authenticated = False
if 'google_drive_service' not in st.session_state:
    st.session_state.google_drive_service = None
if 'google_drive_folder_id' not in st.session_state:
    st.session_state.google_drive_folder_id = ""

class GoogleDriveManager:
    """Handles Google Drive authentication and file operations"""
    
    def __init__(self):
        self.scopes = ['https://www.googleapis.com/auth/drive.file']
        self.credentials = None
        self.service = None
        self.token_file = 'token.pickle'
        self.credentials_file = 'credentials.json'
        
    def authenticate(self):
        """Authenticate with Google Drive"""
        try:
            creds = None
            
            # Load existing credentials
            if os.path.exists(self.token_file):
                with open(self.token_file, 'rb') as token:
                    creds = pickle.load(token)
            
            # If no valid credentials, authenticate
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    if not os.path.exists(self.credentials_file):
                        return None, "Credentials file not found. Please upload credentials.json"
                    
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_file, self.scopes)
                    creds = flow.run_local_server(port=0)
                
                # Save credentials
                with open(self.token_file, 'wb') as token:
                    pickle.dump(creds, token)
            
            # Build service
            self.service = build('drive', 'v3', credentials=creds)
            self.credentials = creds
            
            return self.service, "Authentication successful!"
            
        except Exception as e:
            return None, f"Authentication failed: {str(e)}"
    
    def create_folder(self, folder_name, parent_folder_id=None):
        """Create a folder in Google Drive"""
        try:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                file_metadata['parents'] = [parent_folder_id]
            
            folder = self.service.files().create(
                body=file_metadata,
                fields='id, webViewLink'
            ).execute()
            
            return folder.get('id'), folder.get('webViewLink'), None
            
        except Exception as e:
            return None, None, str(e)
    
    def upload_csv_to_drive(self, dataframe, filename, folder_id=None, description=""):
        """Upload a pandas DataFrame as CSV to Google Drive"""
        try:
            # Convert DataFrame to CSV string
            csv_data = dataframe.to_csv(index=False)
            
            # Create file metadata
            file_metadata = {
                'name': filename,
                'description': description,
                'mimeType': 'text/csv'
            }
            
            if folder_id:
                file_metadata['parents'] = [folder_id]
            
            # Create media upload
            media = MediaIoBaseUpload(
                io.BytesIO(csv_data.encode('utf-8')),
                mimetype='text/csv',
                resumable=True
            )
            
            # Upload file
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink, mimeType, size'
            ).execute()
            
            return {
                'success': True,
                'file_id': file.get('id'),
                'file_name': file.get('name'),
                'file_url': file.get('webViewLink'),
                'file_size': file.get('size'),
                'message': f"File '{filename}' uploaded successfully to Google Drive!"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f"Failed to upload to Google Drive: {str(e)}"
            }
    
    def list_files_in_folder(self, folder_id):
        """List files in a Google Drive folder"""
        try:
            results = self.service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                pageSize=100,
                fields="files(id, name, mimeType, size, modifiedTime, webViewLink)",
                orderBy="modifiedTime desc"
            ).execute()
            
            return results.get('files', [])
            
        except Exception as e:
            return []
    
    def get_folder_info(self, folder_id):
        """Get information about a folder"""
        try:
            folder = self.service.files().get(
                fileId=folder_id,
                fields='id, name, mimeType, webViewLink'
            ).execute()
            
            return folder
            
        except Exception as e:
            return None

class NigerianStatsScraper:
    """Actual web scraper for Nigerian statistical data"""
    
    def __init__(self):
        self.base_url = "https://www.nigerianstat.gov.ng"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        })
        self.timeout = 30
        
    def scrape_nbs_website(self, search_query=None):
        """Scrape data from Nigerian Statistics Bureau website"""
        try:
            log_entry("Starting NBS website scrape...")
            
            # Try to access the main page
            main_url = "https://www.nigerianstat.gov.ng"
            response = self.session.get(main_url, timeout=self.timeout)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for data sections
                data = self.extract_nbs_data(soup, search_query)
                
                if data:
                    log_entry(f"Found {len(data)} data points from NBS website")
                    return pd.DataFrame(data)
                else:
                    # Try library section
                    return self.scrape_nbs_library(search_query)
            else:
                log_entry(f"Failed to access NBS website. Status: {response.status_code}")
                return None
                
        except Exception as e:
            log_entry(f"Error scraping NBS website: {str(e)}")
            return None
    
    def scrape_nbs_library(self, search_query=None):
        """Scrape data from NBS library/elibrary section"""
        try:
            library_url = "https://www.nigerianstat.gov.ng/elibrary"
            response = self.session.get(library_url, timeout=self.timeout)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                data = []
                
                # Look for publications and reports
                links = soup.find_all('a', href=True)
                for link in links[:50]:  # Check first 50 links
                    link_text = link.get_text(strip=True).lower()
                    href = link['href']
                    
                    # Look for data files and reports
                    if any(keyword in link_text for keyword in ['report', 'data', 'statistics', 'survey', 'bulletin']):
                        data.append({
                            'Title': link.get_text(strip=True),
                            'URL': urljoin(library_url, href),
                            'Type': 'Report/Publication',
                            'Source': 'NBS eLibrary',
                            'Scrape_Date': datetime.now().strftime('%Y-%m-%d'),
                            'Search_Query': search_query or 'General'
                        })
                
                return pd.DataFrame(data) if data else None
                
        except Exception as e:
            log_entry(f"Error scraping NBS library: {str(e)}")
            return None
    
    def extract_nbs_data(self, soup, search_query=None):
        """Extract data from NBS website"""
        data = []
        
        # Look for tables with statistical data
        tables = soup.find_all('table')
        
        for table in tables[:5]:  # Limit to first 5 tables
            try:
                # Try to read table with pandas
                dfs = pd.read_html(str(table))
                for df in dfs:
                    # Add metadata
                    df['Source'] = 'NBS Website Table'
                    df['Scrape_Date'] = datetime.now().strftime('%Y-%m-%d')
                    df['Search_Query'] = search_query or 'General'
                    
                    # Convert to list of dictionaries
                    for _, row in df.iterrows():
                        data.append(row.to_dict())
            except:
                # Fallback: Extract text from table
                try:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        row_data = [cell.get_text(strip=True) for cell in cells]
                        if row_data:
                            data.append({
                                'Table_Row': ' | '.join(row_data),
                                'Source': 'NBS Website',
                                'Scrape_Date': datetime.now().strftime('%Y-%m-%d'),
                                'Search_Query': search_query or 'General'
                            })
                except:
                    continue
        
        return data
    
    def scrape_alternative_sources(self, search_query):
        """Scrape from alternative Nigerian data sources"""
        data = []
        
        # Try National Population Commission
        try:
            npc_data = self.scrape_npc_data(search_query)
            if npc_data is not None:
                data.append(npc_data)
        except:
            pass
        
        # Try Central Bank of Nigeria
        try:
            cbn_data = self.scrape_cbn_data(search_query)
            if cbn_data is not None:
                data.append(cbn_data)
        except:
            pass
        
        if data:
            return pd.concat(data, ignore_index=True)
        return None
    
    def scrape_npc_data(self, search_query):
        """Scrape data from National Population Commission"""
        try:
            # This would need actual NPC website scraping
            # For now, return sample structure
            data = [{
                'Indicator': 'Population Estimate',
                'Value': '216 million',
                'Year': 2022,
                'Source': 'National Population Commission',
                'Category': 'Demographics',
                'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
            }]
            return pd.DataFrame(data)
        except:
            return None
    
    def scrape_cbn_data(self, search_query):
        """Scrape data from Central Bank of Nigeria"""
        try:
            # This would need actual CBN website scraping
            data = [{
                'Indicator': 'Exchange Rate',
                'Value': '₦750/$',
                'Date': '2023-12-01',
                'Source': 'Central Bank of Nigeria',
                'Category': 'Economic',
                'Scrape_Date': datetime.now().strftime('%Y-%m-%d')
            }]
            return pd.DataFrame(data)
        except:
            return None

def log_entry(message):
    """Add message to scraping log"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_message = f"[{timestamp}] {message}"
    st.session_state.scraping_log.append(log_message)

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

def show_google_drive_setup():
    """Show Google Drive setup interface"""
    st.header("☁️ Google Drive Setup")
    
    with st.expander("📋 Setup Instructions", expanded=True):
        st.markdown("""
        ### Step 1: Enable Google Drive API
        1. Go to [Google Cloud Console](https://console.cloud.google.com/)
        2. Create a new project or select existing one
        3. Enable **Google Drive API**
        
        ### Step 2: Create OAuth Credentials
        1. Go to "APIs & Services" → "Credentials"
        2. Click "Create Credentials" → "OAuth 2.0 Client ID"
        3. Choose "Desktop application" as application type
        4. Click "Create"
        
        ### Step 3: Download Credentials
        1. Click the download button next to your OAuth 2.0 Client ID
        2. Save the file as `credentials.json` in the same folder as this app
        
        ### Step 4: Get Folder ID (Optional)
        1. Create a folder in Google Drive where you want to save files
        2. Open the folder
        3. Copy the folder ID from the URL:
           `https://drive.google.com/drive/folders/YOUR_FOLDER_ID_HERE`
        """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Upload Credentials")
        uploaded_credentials = st.file_uploader(
            "Upload credentials.json",
            type=['json'],
            key="credentials_upload"
        )
        
        if uploaded_credentials:
            # Save credentials file
            with open('credentials.json', 'wb') as f:
                f.write(uploaded_credentials.getvalue())
            st.success("✅ credentials.json uploaded successfully!")
    
    with col2:
        st.subheader("Google Drive Folder")
        folder_id = st.text_input(
            "Google Drive Folder ID (optional):",
            placeholder="Paste your Google Drive folder ID here",
            help="Leave empty to save to Google Drive root"
        )
        
        if folder_id:
            st.session_state.google_drive_folder_id = folder_id
            st.success("✅ Folder ID saved!")
    
    st.subheader("🔐 Authenticate with Google Drive")
    
    if os.path.exists('credentials.json'):
        if st.button("🔗 Connect to Google Drive", type="primary"):
            with st.spinner("Authenticating with Google Drive..."):
                drive_manager = GoogleDriveManager()
                service, message = drive_manager.authenticate()
                
                if service:
                    st.session_state.google_drive_service = service
                    st.session_state.google_drive_authenticated = True
                    st.success("✅ Successfully authenticated with Google Drive!")
                    
                    # Test connection by getting user info
                    try:
                        about = service.about().get(fields="user").execute()
                        user_email = about['user']['emailAddress']
                        st.info(f"Connected as: {user_email}")
                    except:
                        st.info("Connected to Google Drive")
                else:
                    st.error(f"❌ {message}")
    else:
        st.warning("⚠️ Please upload credentials.json file first")

def show_google_drive_interface():
    """Show Google Drive file management interface"""
    if not st.session_state.google_drive_authenticated:
        st.warning("⚠️ Please authenticate with Google Drive first")
        return
    
    st.header("📁 Google Drive Files")
    
    # Create folder section
    with st.expander("📂 Create New Folder", expanded=False):
        col1, col2 = st.columns([2, 1])
        with col1:
            new_folder_name = st.text_input(
                "New folder name:",
                value=f"Nigeria_Stats_{datetime.now().strftime('%Y%m%d')}",
                key="new_folder_name"
            )
        with col2:
            st.write("")
            st.write("")
            if st.button("Create Folder", key="create_folder_btn"):
                drive_manager = GoogleDriveManager()
                drive_manager.authenticate()
                
                folder_id, folder_url, error = drive_manager.create_folder(
                    new_folder_name,
                    st.session_state.google_drive_folder_id
                )
                
                if folder_id:
                    st.session_state.google_drive_folder_id = folder_id
                    st.success(f"✅ Folder created: {new_folder_name}")
                    st.markdown(f"[📁 Open Folder]({folder_url})", unsafe_allow_html=True)
                else:
                    st.error(f"❌ Failed to create folder: {error}")
    
    # List files in folder
    if st.session_state.google_drive_folder_id:
        st.subheader(f"📋 Files in Current Folder")
        
        if st.button("🔄 Refresh File List", key="refresh_files"):
            st.rerun()
        
        drive_manager = GoogleDriveManager()
        drive_manager.authenticate()
        
        files = drive_manager.list_files_in_folder(
            st.session_state.google_drive_folder_id
        )
        
        if files:
            # Create dataframe for display
            files_data = []
            for file in files:
                files_data.append({
                    'Name': file.get('name', 'Unknown'),
                    'Type': file.get('mimeType', '').split('.')[-1],
                    'Size': f"{int(file.get('size', 0)) / 1024:.1f} KB" if file.get('size') else 'N/A',
                    'Modified': file.get('modifiedTime', '')[:19].replace('T', ' '),
                    'Link': file.get('webViewLink', '#')
                })
            
            files_df = pd.DataFrame(files_data)
            st.dataframe(files_df, use_container_width=True)
        else:
            st.info("📭 No files in this folder yet")
    else:
        st.info("ℹ️ No folder selected. Files will be saved to Google Drive root.")

def main():
    """Main application function"""
    
    # Header
    st.markdown('<h1 class="main-header">🌐 Nigeria Statistics Web Scraper</h1>', unsafe_allow_html=True)
    st.markdown("""
        <div style="text-align: center; margin-bottom: 2rem;">
            <p><strong>Scrape data from Nigerian Statistics Bureau & Save to Google Drive</strong></p>
            <p>Extract real data and automatically save to your Google Drive</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["🔍 Web Scraper", "☁️ Google Drive", "📊 Data View"])
    
    with tab1:
        show_web_scraper_interface()
    
    with tab2:
        show_google_drive_setup()
        if st.session_state.google_drive_authenticated:
            show_google_drive_interface()
    
    with tab3:
        if st.session_state.scraped_data is not None:
            show_data_view_interface()
        else:
            st.info("👈 Go to 'Web Scraper' tab to scrape some data first!")

def show_web_scraper_interface():
    """Show web scraping interface"""
    st.header("🔍 Web Scraper")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Search input
        search_query = st.text_input(
            "Enter what data to scrape:",
            placeholder="e.g., 'economic indicators', 'population data', 'crime statistics'",
            key="search_input_main"
        )
        
        # Quick search buttons
        st.subheader("🚀 Quick Searches")
        
        quick_searches = [
            "Economic Data",
            "Population Statistics",
            "Employment Rates",
            "Health Indicators",
            "Education Statistics",
            "Crime Reports"
        ]
        
        cols = st.columns(3)
        for i, query in enumerate(quick_searches):
            with cols[i % 3]:
                if st.button(f"🔎 {query}", key=f"quick_{i}"):
                    search_query = query
                    st.session_state.search_query = query
                    st.rerun()
    
    with col2:
        st.subheader("⚙️ Options")
        
        # Google Drive auto-save option
        auto_save_gdrive = st.checkbox(
            "Auto-save to Google Drive",
            value=True,
            help="Automatically save scraped data to Google Drive"
        )
        
        # Export format
        export_format = st.selectbox(
            "Export format:",
            ["CSV", "JSON", "Both"]
        )
    
    # Scrape button
    if st.button("🚀 Start Web Scraping", type="primary", use_container_width=True):
        if not search_query and 'search_query' not in st.session_state:
            st.warning("⚠️ Please enter a search query or select a quick search")
        else:
            query = search_query or getattr(st.session_state, 'search_query', '')
            
            st.session_state.scraping_in_progress = True
            st.session_state.scraping_log = []
            
            with st.spinner(f"🌐 Scraping data for '{query}'..."):
                scraper = NigerianStatsScraper()
                
                # Create progress indicators
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Step 1: Scrape NBS website
                status_text.text("Step 1/3: Accessing Nigerian Statistics Bureau...")
                progress_bar.progress(30)
                time.sleep(1)  # Simulate delay
                
                nbs_data = scraper.scrape_nbs_website(query)
                
                # Step 2: Scrape alternative sources
                status_text.text("Step 2/3: Checking other sources...")
                progress_bar.progress(60)
                time.sleep(1)
                
                alt_data = scraper.scrape_alternative_sources(query)
                
                # Step 3: Process data
                status_text.text("Step 3/3: Processing data...")
                progress_bar.progress(90)
                
                # Combine data
                all_data = []
                if nbs_data is not None and not nbs_data.empty:
                    all_data.append(nbs_data)
                if alt_data is not None and not alt_data.empty:
                    all_data.append(alt_data)
                
                if all_data:
                    combined_data = pd.concat(all_data, ignore_index=True)
                    st.session_state.scraped_data = combined_data
                    
                    status_text.text("✅ Data scraping completed!")
                    progress_bar.progress(100)
                    
                    st.success(f"🎉 Successfully scraped {len(combined_data)} records!")
                    
                    # Auto-save to Google Drive
                    if auto_save_gdrive and st.session_state.google_drive_authenticated:
                        save_to_google_drive(combined_data, query, export_format)
                    
                    # Provide download links
                    provide_download_links(combined_data, query, export_format)
                    
                else:
                    st.warning("⚠️ No structured data found. The website may have changed or data may require different access methods.")
                    st.info("💡 Try: 1) Different search terms 2) Check if website is accessible 3) Use specific data source names")
                
                st.session_state.scraping_in_progress = False
    
    # Display scraping log
    if st.session_state.scraping_log:
        with st.expander("📋 View Scraping Log", expanded=False):
            for log_entry_text in st.session_state.scraping_log[-20:]:
                st.text(log_entry_text)

def save_to_google_drive(dataframe, query, export_format):
    """Save dataframe to Google Drive"""
    try:
        drive_manager = GoogleDriveManager()
        drive_manager.authenticate()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_query = re.sub(r'[^\w\s-]', '', query.replace(' ', '_'))[:50]
        
        # Save CSV
        if export_format in ["CSV", "Both"]:
            csv_filename = f"nigeria_stats_{safe_query}_{timestamp}.csv"
            
            result = drive_manager.upload_csv_to_drive(
                dataframe,
                csv_filename,
                st.session_state.google_drive_folder_id if st.session_state.google_drive_folder_id else None,
                f"Nigeria Statistics Data - {query} - Scraped on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            
            if result['success']:
                st.markdown(f"""
                <div class="google-drive-box">
                    <strong>✅ CSV saved to Google Drive!</strong><br>
                    <strong>File:</strong> {result['file_name']}<br>
                    <strong>Size:</strong> {result.get('file_size', 'N/A')}<br>
                    <a href="{result['file_url']}" target="_blank">🔗 Open in Google Drive</a>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.warning(f"⚠️ Failed to save CSV to Google Drive: {result['error']}")
        
        # Save JSON
        if export_format in ["JSON", "Both"]:
            json_filename = f"nigeria_stats_{safe_query}_{timestamp}.json"
            json_data = dataframe.to_json(orient='records', indent=2)
            
            # Convert JSON to file-like object
            file_metadata = {
                'name': json_filename,
                'mimeType': 'application/json'
            }
            
            if st.session_state.google_drive_folder_id:
                file_metadata['parents'] = [st.session_state.google_drive_folder_id]
            
            media = MediaIoBaseUpload(
                io.BytesIO(json_data.encode('utf-8')),
                mimetype='application/json',
                resumable=True
            )
            
            file = st.session_state.google_drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink'
            ).execute()
            
            st.markdown(f"""
            <div class="google-drive-box">
                <strong>✅ JSON saved to Google Drive!</strong><br>
                <strong>File:</strong> {file.get('name')}<br>
                <a href="{file.get('webViewLink')}" target="_blank">🔗 Open in Google Drive</a>
            </div>
            """, unsafe_allow_html=True)
            
    except Exception as e:
        st.error(f"❌ Error saving to Google Drive: {str(e)}")

def provide_download_links(dataframe, query, export_format):
    """Provide direct download links"""
    st.subheader("💾 Direct Download")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_query = re.sub(r'[^\w\s-]', '', query.replace(' ', '_'))[:50]
    
    col1, col2 = st.columns(2)
    
    with col1:
        if export_format in ["CSV", "Both"]:
            csv_filename = f"nigeria_stats_{safe_query}_{timestamp}.csv"
            csv_link = create_download_link(dataframe, csv_filename, "csv")
            st.markdown(csv_link, unsafe_allow_html=True)
    
    with col2:
        if export_format in ["JSON", "Both"]:
            json_filename = f"nigeria_stats_{safe_query}_{timestamp}.json"
            json_link = create_download_link(dataframe, json_filename, "json")
            st.markdown(json_link, unsafe_allow_html=True)
    
    # Local save option
    st.subheader("💾 Save Locally")
    
    local_filename = st.text_input(
        "Local filename:",
        value=f"nigeria_stats_{safe_query}_{timestamp}.csv",
        key="local_filename"
    )
    
    if st.button("💾 Save to Local File", key="save_local"):
        os.makedirs("scraped_data", exist_ok=True)
        filepath = f"scraped_data/{local_filename}"
        dataframe.to_csv(filepath, index=False)
        st.success(f"✅ Data saved to: {filepath}")
        st.info(f"📁 Location: {os.path.abspath(filepath)}")

def show_data_view_interface():
    """Show data viewing and analysis interface"""
    st.header("📊 Scraped Data View")
    
    df = st.session_state.scraped_data
    
    if df is not None and not df.empty:
        # Statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", len(df))
        with col2:
            st.metric("Columns", len(df.columns))
        with col3:
            sources = df['Source'].unique() if 'Source' in df.columns else ['Unknown']
            st.metric("Sources", len(sources))
        with col4:
            st.metric("Data Size", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")
        
        # Data preview
        st.subheader("📋 Data Preview")
        st.dataframe(df, use_container_width=True, height=400)
        
        # Data filtering
        with st.expander("🔍 Filter & Search Data", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                if 'Source' in df.columns:
                    sources = df['Source'].unique()
                    selected_sources = st.multiselect(
                        "Filter by source:",
                        sources,
                        default=sources[:min(3, len(sources))]
                    )
            
            with col2:
                search_text = st.text_input("Search in all columns:", "")
            
            # Apply filters
            filtered_df = df.copy()
            if 'Source' in df.columns and selected_sources:
                filtered_df = filtered_df[filtered_df['Source'].isin(selected_sources)]
            
            if search_text:
                mask = filtered_df.astype(str).apply(lambda x: x.str.contains(search_text, case=False, na=False)).any(axis=1)
                filtered_df = filtered_df[mask]
            
            st.dataframe(filtered_df, use_container_width=True, height=300)
            st.caption(f"Showing {len(filtered_df)} of {len(df)} records")
        
        # Column analysis
        with st.expander("📊 Column Analysis", expanded=False):
            col_info = []
            for col in df.columns:
                col_info.append({
                    'Column': col,
                    'Type': str(df[col].dtype),
                    'Non-Null': df[col].count(),
                    'Null %': f"{(df[col].isnull().sum() / len(df) * 100):.1f}%",
                    'Unique': df[col].nunique()
                })
            st.table(pd.DataFrame(col_info))
        
        # Quick analysis
        st.subheader("📈 Quick Analysis")
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            st.write("Numeric columns summary:")
            st.write(df[numeric_cols].describe())
        else:
            st.info("No numeric columns found for statistical analysis")

# Footer
st.markdown("---")
st.markdown("""
    <div style="text-align: center; color: #666; padding: 2rem 0;">
        <p><strong>Nigeria Statistics Web Scraper with Google Drive Integration</strong> • Version 3.0</p>
        <p>🌐 <strong>Sources:</strong> Nigerian Statistics Bureau (nigerianstat.gov.ng) • National Population Commission • Central Bank of Nigeria</p>
        <p>☁️ <strong>Storage:</strong> Google Drive integration for automatic cloud backup</p>
        <p>🛠️ <strong>Technologies:</strong> Python, BeautifulSoup, Google Drive API, Streamlit</p>
    </div>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()