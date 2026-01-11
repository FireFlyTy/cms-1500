"""
CMS-1500 Rule Builder API Server

Usage:
    uvicorn api.main:app --reload --port 8000
"""

import os
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

load_dotenv()

# Import routes
from api.kb_routes import router as kb_router
from api.rule_routes import router as rule_router


# ============================================================
# APP SETUP
# ============================================================

app = FastAPI(
    title="CMS-1500 Rule Builder",
    description="Knowledge Base and Rule Generation API",
    version="0.1.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(kb_router)
app.include_router(rule_router)

#print("kb_router:", kb_router.prefix)
#print("rule_router:", rule_router.prefix)


# ============================================================
# STATIC FILES
# ============================================================

# Serve PDFs
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "data/raw/documents")
if os.path.exists(UPLOAD_DIR):
    app.mount("/files", StaticFiles(directory=UPLOAD_DIR), name="files")


# ============================================================
# ROOT ENDPOINTS
# ============================================================

@app.get("/")
async def root():
    return {
        "name": "CMS-1500 Rule Builder API",
        "version": "0.1.0",
        "docs": "/docs"
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
async def startup():
    """Initialize on startup"""
    
    # Ensure directories exist
    dirs = [
        "data/processed/documents",
        "data/raw/documents/guidelines",
        "data/raw/documents/policies",
        "data/raw/documents/coding",
        "data/rules",
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
    
    # Initialize database
    from src.db.connection import init_database
    try:
        init_database()
    except Exception as e:
        print(f"DB init warning: {e}")
    
    print("API Server started")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
