# Nigeria-Data-Scraper
A modular, topic-based web scraping system built with Python + Streamlit for extracting structured Nigerian data (tables, statistics, and reports) from multiple authoritative sources.

This project scrapes, processes, and exports data across key Nigerian sectors such as:

- GDP

- Agriculture

- Banking

- Oil & Gas

- Budget & Fiscal Data

- Education

- Health

- Trade

- Inflation

- Unemployment

- Population

- And many more…

* Project Overview

This repository contains multiple Python modules designed for different but connected purposes:

🔎 Web scraping (HTML + table extraction)

📊 Table parsing (pandas + manual extraction)

📄 PDF processing (PyPDF2, pdfplumber, pdfminer, PyMuPDF)

📁 Data export (CSV, Excel, JSON)

☁️ Optional Google Drive integration

🖥️ Interactive Streamlit dashboard interface

Each Python file handles a specific responsibility (scraping, table extraction, export, PDF handling, UI, etc.), making the system modular and scalable.

⚙️ Key Features

- Topic-based scraping system for Nigerian data

- Automatic table detection using:

- pandas.read_html

- BeautifulSoup manual parsing

- Smart keyword filtering

- Multi-source scraping (World Bank, NBS, IMF, FAO, CBN, etc.)

- Text pattern extraction for statistics (%, large numbers, million/billion)

Export to:

CSV

Excel

JSON

Metadata generation for each table

Downloadable ZIP packages

Clean Streamlit dashboard interface


* Architecture

The system is structured into:

- Scraper Layer – Handles HTTP requests and source parsing

- Table Extraction Layer – Extracts and processes structured HTML tables

- Text Extraction Layer – Extracts statistical text-based data

- Export Layer – Saves results to CSV, Excel, JSON

- UI Layer (Streamlit) – Interactive dashboard for scraping and downloads

* Tech Stack

Python, Streamlit, Pandas, BeautifulSoup, Requests, NumPy, pdfplumber / PyPDF2 / pdfminer / PyMuPDF


* Use Cases

Economic research, Policy analysis, Nigerian sectoral data collection, Academic research, Journalism & reporting
Dashboard data pipelines, Automated data gathering
