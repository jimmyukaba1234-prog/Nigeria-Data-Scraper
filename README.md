# 🇳🇬 Nigeria Statistics Web Scraper Pro

**A powerful, production-grade web scraper** built to extract official statistical data, reports, and PDFs from Nigerian government agencies and international organizations.

---

## ✨ Project Overview

This is a **single-file Streamlit application** that intelligently scrapes, crawls, downloads, and organizes Nigerian statistical data from **22+ authoritative sources**.

You simply enter a search term (e.g., "GDP 2024", "Unemployment Rate", "Inflation"), and the tool automatically:
- Searches across multiple official websites
- Crawls relevant pages
- Downloads PDFs and extracts statistical data
- Organizes everything locally and optionally uploads to Google Drive

---

## 🚀 Key Features

- **Smart Multi-Source Scraping** — 22+ official sources (NBS, CBN, World Bank, IMF, etc.)
- **Intelligent Crawling** — Multi-page BFS crawling with configurable depth
- **Advanced PDF Handling** — Robust downloader with 403 bypass, retries, and rotating headers
- **Concurrent Scraping** — Uses `ThreadPoolExecutor` for high performance
- **Google Drive Integration** — One-click upload with automatic folder organization
- **Rich Export Options** — CSV, JSON, Excel, PDF Report, ZIP of all PDFs
- **Real-time Logging** — Thread-safe scraping log
- **Beautiful Streamlit Dashboard** — Clean, responsive UI with quick search buttons

---

## 🛠️ Tech Stack

- **Frontend**: Streamlit
- **Scraping**: Requests, BeautifulSoup4, Selenium (optional)
- **Concurrency**: `concurrent.futures.ThreadPoolExecutor`
- **Data Processing**: Pandas, NumPy
- **PDF Handling**: PyPDF2, pdfplumber, PyMuPDF (fitz)
- **Export**: ReportLab (PDF reports), zipfile
- **Cloud**: Google Drive API v3 (OAuth2 + resumable uploads)
- **Others**: Thread-safe logging, graceful optional dependencies

---

## 📁 Project Structure

Nigeria.py                  # Main Streamlit app (Everything in one file)
├── ThreadSafeLogger
├── GoogleDriveManager
├── NigerianStatsScraper (Base)
├── EnhancedNigerianStatsScraper (Advanced crawling)
├── PDF & Export Helpers
└── Streamlit UI + Session State
text---

## ⚙️ How to Run Locally

### Prerequisites
- Python 3.9+
- Chrome (for optional Selenium)

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/Nigeria-Data-Scraper.git
   cd Nigeria-Data-Scraper

Install dependencies:Bashpip install -r requirements.txt
Google Drive Setup (Optional but recommended):
Follow the Google Drive Setup Guide
Place credentials.json in the project root

Run the app:Bashstreamlit run Nigeria.py



Key Technical Highlights

Robust PDF Downloader with multiple user agents and exponential backoff
BFS Crawling Engine using collections.deque
ThreadSafeLogger for concurrent operations
Graceful Degradation — Works even if optional libraries (Selenium, PDF tools) are missing
Smart Link Prioritization — PDFs and statistical pages ranked higher


🎯 Use Cases

Economic research & policy analysis
Academic studies on Nigeria
Journalism and data reporting
Automated data collection for dashboards
Personal research and monitoring


📌 Future Roadmap

Deploy to Streamlit Community Cloud (Service Account)
Scheduled daily scraping
Better PDF text extraction (OCR fallback)
Docker support
API version (FastAPI)
More data sources


🤝 Contributing
Contributions, issues, and feature requests are welcome!
Feel free to open an issue or submit a pull request.
