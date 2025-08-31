# Codal Stock Market Scraper

A high-performance web scraper for extracting stock market data from the Iranian Codal website (codal.ir). This project provides fast, reliable scraping of company notices, financial reports, and detailed content analysis.

## 🚀 Features

- **Ultra-fast scraping** with optimized Chrome driver settings
- **Duplicate detection** based on publish time for each symbol
- **Robust error handling** with stale element recovery
- **RESTful API** with FastAPI for easy integration
- **PostgreSQL database** for reliable data storage
- **Content analysis** for extracting tables, numbers, and financial data
- **Batch processing** for maximum performance
- **Background tasks** for non-blocking operations

## 📋 Requirements

- Python 3.8+
- PostgreSQL database
- Chrome browser
- ChromeDriver

## 🛠️ Installation

1. **Clone the repository:**
```bash
git clone https://github.com/yourusername/codal-scraper.git
cd codal-scraper 
```

2. **Create virtual environment:**
```bash
python -m venv venv
source venv/bin/activate 
```
3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Setup PostgreSQL database:**
```bash
CREATE DATABASE codal_db;
CREATE USER codal_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE codal_db TO codal_user;
```

5. **Start the API Server:**
```bash
python main.py
``` 


The API will be available at http://localhost:8000

## API Documentation
Visit http://localhost:8000/docs for interactive Swagger documentation.

Main Endpoints
Scraping Operations
POST /scrape/{symbol} - Scrape notices for a symbol
POST /refresh/{symbol} - Delete existing data and scrape fresh
POST /append/{symbol} - Add new notices without duplicates
Data Management
GET /symbol/{symbol} - Get symbol information and recent notices
GET /symbols - List all unique symbols
GET /count - Get total record count
DELETE /symbol/{symbol} - Delete all records for a symbol
DELETE /notice/{notice_id} - Delete specific notice
