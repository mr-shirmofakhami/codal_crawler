# main.py
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from routes.auth_routes import router as auth_router  # Fixed import
from auth.auth import get_current_active_user  # Fixed import

import uvicorn
import logging

# Import your modules
from database import engine
from models import Base

from config.settings import get_settings
from routes import health, financial, scraping, notices, financial_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get settings
settings = get_settings()

# Create tables
Base.metadata.create_all(bind=engine)

# Initialize FastAPI
app = FastAPI(title="Ultra-Fast Codal Scraper with Financial Statements", version="2.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include authentication routes
app.include_router(auth_router, prefix="/auth", tags=["authentication"])

# ============================================================================
# BASIC API ROUTES
# ============================================================================

@app.get("/")
async def root():
    return {
        "message": "Ultra-Fast Codal Scraper with Financial Statements API v2.0",
        "status": "running",
        "features": ["scraping", "financial_statements", "postgresql_storage", "detailed_normalization", "authentication"]
    }

# Include routers (you can protect these later by adding Depends(get_current_active_user))
app.include_router(health.router, tags=["Health"])
app.include_router(financial.router, prefix="/financial-statement", tags=["Financial Statements"])
app.include_router(financial_data.router, prefix="/financial-data", tags=["Financial data"])
app.include_router(scraping.router, tags=["Scraping"])
app.include_router(notices.router, tags=["Notice Management"])

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3000)