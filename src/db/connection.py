"""
Database connection helper for SQLite.
"""

import os
import sqlite3
from pathlib import Path


# Get project root (this file is in src/db/, so go up 2 levels)
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Default database path relative to project root
_default_db_path = PROJECT_ROOT / "data" / "db" / "reference.db"
DATABASE_PATH = os.getenv("DATABASE_PATH", str(_default_db_path))


def get_db_connection() -> sqlite3.Connection:
    """Get SQLite connection with row factory"""
    
    # Ensure directory exists
    db_path = Path(DATABASE_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    
    return conn


def init_database():
    """Initialize database with schema"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Read and execute schema from models.py
    schema_path = Path(__file__).parent / "models.py"
    
    if schema_path.exists():
        with open(schema_path, 'r') as f:
            content = f.read()
            
        # Extract SQL from SCHEMA variable's triple-quoted string
        import re
        sql_match = re.search(r'SCHEMA\s*=\s*"""(.*?)"""', content, re.DOTALL)
        if sql_match:
            sql = sql_match.group(1)
            cursor.executescript(sql)
            conn.commit()
            print("Database initialized")
    
    conn.close()


def execute_query(query: str, params: tuple = None):
    """Execute a query and return results"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        results = cursor.fetchall()
        conn.commit()
        return results
        
    finally:
        conn.close()


def execute_many(query: str, params_list: list):
    """Execute query with multiple parameter sets"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.executemany(query, params_list)
        conn.commit()
        return cursor.rowcount
        
    finally:
        conn.close()