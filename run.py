#!/usr/bin/env python
"""
Run the API server.

Usage:
    python run.py

Or with uvicorn directly:
    python -m uvicorn run:app --reload --port 8000
"""

import os
import sys
from pathlib import Path

# Ensure project root is in path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Import routes after path is set
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

# ============================================================
# STATIC FILES
# ============================================================

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
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)

    # Initialize database
    from src.db.connection import init_database
    try:
        init_database()
    except Exception as e:
        print(f"DB init warning: {e}")

    print("✓ API Server started at http://localhost:8000")
    print("✓ Docs available at http://localhost:8000/docs")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "run:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=[str(PROJECT_ROOT)]
    )