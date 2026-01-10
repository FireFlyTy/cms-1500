#!/usr/bin/env python
"""
Добавляет новые таблицы в существующую базу reference.db
Не затрагивает существующие данные (hcpcs, ncci, icd10)
"""

import sqlite3
from pathlib import Path

DB_PATH = Path("data/processed/reference.db")

MIGRATION = """
-- Documents metadata
CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,
    filename TEXT NOT NULL,
    filepath TEXT,
    doc_type TEXT,
    doc_subtype TEXT,
    payer TEXT,
    total_pages INTEGER,
    parsed_at TEXT,
    analyzed_at TEXT,
    content_path TEXT,
    notes TEXT
);

-- Categories
CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_type TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    UNIQUE(category_type, name)
);

-- Document-Category relationship
CREATE TABLE IF NOT EXISTS document_categories (
    document_id TEXT NOT NULL,
    category_id INTEGER NOT NULL,
    PRIMARY KEY (document_id, category_id),
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
);

-- Document codes
CREATE TABLE IF NOT EXISTS document_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT NOT NULL,
    code_pattern TEXT NOT NULL,
    code_type TEXT,
    description TEXT,
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_document_codes_doc ON document_codes(document_id);
CREATE INDEX IF NOT EXISTS idx_document_codes_pattern ON document_codes(code_pattern);

-- Document stages
CREATE TABLE IF NOT EXISTS document_stages (
    document_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    purpose TEXT,
    PRIMARY KEY (document_id, stage),
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

-- Rules
CREATE TABLE IF NOT EXISTS rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    code_type TEXT,
    rule_level TEXT,
    status TEXT DEFAULT 'pending',
    rule_path TEXT,
    source_documents TEXT,
    generated_at TEXT,
    validated_at TEXT,
    validation_errors TEXT,
    UNIQUE(code, rule_level)
);

CREATE INDEX IF NOT EXISTS idx_rules_code ON rules(code);
CREATE INDEX IF NOT EXISTS idx_rules_status ON rules(status);

-- Default categories
INSERT OR IGNORE INTO categories (category_type, name, description) VALUES
    ('medical', 'Diabetes', 'Type 1, Type 2, gestational diabetes'),
    ('medical', 'Cardiology', 'Heart and cardiovascular'),
    ('medical', 'Oncology', 'Cancer'),
    ('medical', 'Mental Health', 'Psychiatric'),
    ('medical', 'Pain Management', 'Chronic pain, opioids'),
    ('medical', 'Respiratory', 'Lung and breathing'),
    ('medical', 'Musculoskeletal', 'Bones, joints'),
    ('medical', 'Neurology', 'Brain and nervous system'),
    ('medical', 'Endocrine', 'Hormonal disorders'),
    ('medical', 'Renal', 'Kidney disease'),
    ('service', 'Pharmacy', 'Prescription drugs'),
    ('service', 'DME', 'Durable Medical Equipment'),
    ('service', 'Labs', 'Laboratory tests'),
    ('service', 'Procedures', 'Surgical procedures'),
    ('service', 'E&M', 'Evaluation and Management'),
    ('service', 'Imaging', 'X-ray, MRI, CT'),
    ('service', 'Therapy', 'PT, OT, speech');
"""

def migrate():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    
    # Check existing tables
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing = {row[0] for row in cursor.fetchall()}
    print(f"Existing tables: {existing}")
    
    # Run migration
    conn.executescript(MIGRATION)
    conn.commit()
    
    # Verify new tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    after = {row[0] for row in cursor.fetchall()}
    new_tables = after - existing
    
    print(f"Added tables: {new_tables}")
    print("Migration complete!")
    
    conn.close()

if __name__ == "__main__":
    migrate()
