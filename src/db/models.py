"""
SQLite database models and schema
"""
import sqlite3
from pathlib import Path

SCHEMA = """
-- HCPCS codes table
CREATE TABLE IF NOT EXISTS hcpcs (
    code TEXT PRIMARY KEY,
    long_description TEXT,
    short_description TEXT,
    betos TEXT,
    tos TEXT,
    coverage TEXT,
    proc_note TEXT,
    add_date TEXT,
    term_date TEXT
);

CREATE INDEX IF NOT EXISTS idx_hcpcs_betos ON hcpcs(betos);
CREATE INDEX IF NOT EXISTS idx_hcpcs_proc_note ON hcpcs(proc_note);

-- HCPCS processing notes
CREATE TABLE IF NOT EXISTS hcpcs_notes (
    note_id TEXT PRIMARY KEY,
    note_text TEXT
);

-- NCCI PTP edits (Procedure-to-Procedure)
CREATE TABLE IF NOT EXISTS ncci_ptp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    column1 TEXT NOT NULL,
    column2 TEXT NOT NULL,
    modifier_indicator INTEGER,  -- 0=not allowed, 1=allowed, 9=N/A
    effective_date TEXT,
    deletion_date TEXT,
    rationale TEXT,
    UNIQUE(column1, column2)
);

CREATE INDEX IF NOT EXISTS idx_ncci_ptp_col1 ON ncci_ptp(column1);
CREATE INDEX IF NOT EXISTS idx_ncci_ptp_col2 ON ncci_ptp(column2);

-- NCCI MUE edits (Medically Unlikely Edits) - Practitioner
CREATE TABLE IF NOT EXISTS ncci_mue_pra (
    code TEXT PRIMARY KEY,
    mue_value INTEGER,
    adjudication_indicator TEXT,
    rationale TEXT
);

-- NCCI MUE edits - DME Supplier
CREATE TABLE IF NOT EXISTS ncci_mue_dme (
    code TEXT PRIMARY KEY,
    mue_value INTEGER,
    adjudication_indicator TEXT,
    rationale TEXT
);

-- ICD-10 codes
CREATE TABLE IF NOT EXISTS icd10 (
    code TEXT PRIMARY KEY,
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_icd10_code ON icd10(code);

-- CPT codes (procedure codes)
CREATE TABLE IF NOT EXISTS cpt (
    code TEXT PRIMARY KEY,
    description TEXT,
    category TEXT,              -- Procedure category (AAA, AMP, etc.)
    status TEXT                 -- Code status (No change, New, etc.)
);

CREATE INDEX IF NOT EXISTS idx_cpt_code ON cpt(code);
CREATE INDEX IF NOT EXISTS idx_cpt_category ON cpt(category);

-- Documents metadata
CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,              -- file hash
    filename TEXT NOT NULL,
    filepath TEXT,                    -- relative path from documents/
    doc_type TEXT,                    -- clinical_guideline | pa_policy | coding_rules | provider_manual
    doc_subtype TEXT,                 -- odg | ncci | anthem | icd10 | etc
    payer TEXT,                       -- NULL = generic/all payers
    total_pages INTEGER,
    parsed_at TEXT,
    analyzed_at TEXT,
    content_path TEXT,                -- path to extracted text
    notes TEXT                        -- manual notes/description
);

-- Categories (medical conditions & service types)
CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_type TEXT NOT NULL,      -- medical | service
    name TEXT NOT NULL,
    description TEXT,
    UNIQUE(category_type, name)
);

-- Document-Category relationship (many-to-many)
CREATE TABLE IF NOT EXISTS document_categories (
    document_id TEXT NOT NULL,
    category_id INTEGER NOT NULL,
    PRIMARY KEY (document_id, category_id),
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
);

-- Codes covered by document (many-to-many)
CREATE TABLE IF NOT EXISTS document_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT NOT NULL,
    code_pattern TEXT NOT NULL,       -- E11.*, J1950, A4*, 99213-99215
    code_type TEXT,                   -- ICD-10 | CPT | HCPCS | NDC
    description TEXT,                 -- optional description
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_document_codes_doc ON document_codes(document_id);
CREATE INDEX IF NOT EXISTS idx_document_codes_pattern ON document_codes(code_pattern);

-- Pipeline stages for document
CREATE TABLE IF NOT EXISTS document_stages (
    document_id TEXT NOT NULL,
    stage TEXT NOT NULL,              -- level_1 | level_2 | level_3
    purpose TEXT,                     -- description of how document is used at this stage
    PRIMARY KEY (document_id, stage),
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

-- Generated rules tracking
CREATE TABLE IF NOT EXISTS rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    code_type TEXT,                   -- ICD-10 | CPT | HCPCS
    rule_level TEXT,                  -- level_1 | level_2 | level_3
    status TEXT DEFAULT 'pending',    -- pending | ready | error
    rule_path TEXT,                   -- path to generated YAML
    source_documents TEXT,            -- JSON array of document IDs
    generated_at TEXT,
    validated_at TEXT,
    validation_errors TEXT,           -- JSON array of errors if any
    UNIQUE(code, rule_level)
);

CREATE INDEX IF NOT EXISTS idx_rules_code ON rules(code);
CREATE INDEX IF NOT EXISTS idx_rules_status ON rules(status);

-- ============================================================
-- CODE HIERARCHY (Category → Subcategory → Code)
-- ============================================================

CREATE TABLE IF NOT EXISTS code_hierarchy (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code_type TEXT NOT NULL,           -- 'ICD-10', 'CPT', 'HCPCS'
    level INTEGER NOT NULL,            -- 1=category, 2=subcategory, 3=code
    pattern TEXT NOT NULL,             -- 'E11', 'E11.6', 'E11.65'
    parent_pattern TEXT,               -- NULL for level 1, parent for others
    description TEXT,
    chapter TEXT,                      -- For ICD-10: chapter name (e.g., "Endocrine")
    meta_category TEXT,                -- First letter/digit: 'E', 'F', '9', 'J' etc.
    UNIQUE(code_type, pattern)
);

CREATE INDEX IF NOT EXISTS idx_code_hierarchy_pattern ON code_hierarchy(pattern);
CREATE INDEX IF NOT EXISTS idx_code_hierarchy_parent ON code_hierarchy(parent_pattern);
CREATE INDEX IF NOT EXISTS idx_code_hierarchy_level ON code_hierarchy(level);
CREATE INDEX IF NOT EXISTS idx_code_hierarchy_type_level ON code_hierarchy(code_type, level);
CREATE INDEX IF NOT EXISTS idx_code_hierarchy_meta ON code_hierarchy(code_type, meta_category);

-- ============================================================
-- TOPICS DICTIONARY (Predefined medical topics)
-- ============================================================

CREATE TABLE IF NOT EXISTS topics_dictionary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL,            -- 'disease', 'procedure', 'anatomy', 'treatment'
    aliases TEXT,                      -- JSON array: ["T2DM", "Type II Diabetes"]
    description TEXT,
    parent_topic_id INTEGER,           -- For hierarchical topics
    icd10_patterns TEXT,               -- JSON array: ["E11%", "E08%"]
    cpt_patterns TEXT,                 -- JSON array: ["9921%"]
    hcpcs_patterns TEXT,               -- JSON array: ["J195%"]
    FOREIGN KEY (parent_topic_id) REFERENCES topics_dictionary(id)
);

CREATE INDEX IF NOT EXISTS idx_topics_category ON topics_dictionary(category);
CREATE INDEX IF NOT EXISTS idx_topics_name ON topics_dictionary(name);

-- ============================================================
-- DOCUMENT TOPICS (Links documents to predefined topics)
-- ============================================================

CREATE TABLE IF NOT EXISTS document_topics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT NOT NULL,
    topic_id INTEGER NOT NULL,
    anchor_start TEXT,                 -- First 5-10 words of paragraph
    anchor_end TEXT,                   -- Last 5-10 words of paragraph
    page INTEGER,
    confidence REAL DEFAULT 1.0,       -- Extraction confidence
    extracted_by TEXT,                 -- 'gemini', 'gpt5', 'gpt4', 'manual'
    validated INTEGER DEFAULT 0,       -- 0=draft, 1=validated
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY (topic_id) REFERENCES topics_dictionary(id)
);

CREATE INDEX IF NOT EXISTS idx_document_topics_doc ON document_topics(document_id);
CREATE INDEX IF NOT EXISTS idx_document_topics_topic ON document_topics(topic_id);

-- ============================================================
-- RULES HIERARCHY (Track rule inheritance)
-- ============================================================

CREATE TABLE IF NOT EXISTS rules_hierarchy (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern TEXT NOT NULL,             -- 'E11', 'E11.6', 'E11.65' (from code_hierarchy)
    pattern_type TEXT NOT NULL,        -- 'meta_category', 'category', 'subcategory', 'code'
    code_type TEXT NOT NULL,           -- 'ICD-10', 'CPT', 'HCPCS'
    rule_type TEXT NOT NULL DEFAULT 'guideline',  -- 'guideline' or 'cms1500'
    parent_pattern TEXT,               -- Parent in hierarchy (from code_hierarchy)
    rule_id INTEGER,                   -- FK to rules table (NULL if same_as_parent)
    has_own_rule INTEGER DEFAULT 0,    -- 1 if this pattern has its own generated rule
    inherits_from TEXT,                -- Pattern from which rule is inherited (for same_as_parent)
    status TEXT DEFAULT 'pending',     -- 'pending', 'generating', 'ready', 'same_as_parent', 'failed'
    claimed_at TIMESTAMP,              -- When generation started (for timeout detection)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code_type, pattern, rule_type),
    FOREIGN KEY (rule_id) REFERENCES rules(id)
);

CREATE INDEX IF NOT EXISTS idx_rules_hierarchy_pattern ON rules_hierarchy(pattern);
CREATE INDEX IF NOT EXISTS idx_rules_hierarchy_type ON rules_hierarchy(code_type);
CREATE INDEX IF NOT EXISTS idx_rules_hierarchy_rule_type ON rules_hierarchy(rule_type);

-- Insert default categories
INSERT OR IGNORE INTO categories (category_type, name, description) VALUES
    -- Medical categories
    ('medical', 'Diabetes', 'Type 1, Type 2, gestational diabetes and related conditions'),
    ('medical', 'Cardiology', 'Heart and cardiovascular conditions'),
    ('medical', 'Oncology', 'Cancer diagnosis and treatment'),
    ('medical', 'Mental Health', 'Psychiatric and psychological conditions'),
    ('medical', 'Pain Management', 'Chronic pain, opioids, pain procedures'),
    ('medical', 'Respiratory', 'Lung and breathing conditions'),
    ('medical', 'Musculoskeletal', 'Bones, joints, muscles'),
    ('medical', 'Neurology', 'Brain and nervous system'),
    ('medical', 'Endocrine', 'Hormonal disorders beyond diabetes'),
    ('medical', 'Renal', 'Kidney disease and dialysis'),
    -- Service types
    ('service', 'Pharmacy', 'Prescription drugs, injectable medications'),
    ('service', 'DME', 'Durable Medical Equipment'),
    ('service', 'Labs', 'Laboratory tests and panels'),
    ('service', 'Procedures', 'Surgical and non-surgical procedures'),
    ('service', 'E&M', 'Evaluation and Management visits'),
    ('service', 'Imaging', 'X-ray, MRI, CT, ultrasound'),
    ('service', 'Therapy', 'Physical, occupational, speech therapy');
"""


def init_db(db_path: Path) -> sqlite3.Connection:
    """Initialize database with schema"""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def get_db(db_path: Path) -> sqlite3.Connection:
    """Get database connection"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def migrate_db(conn: sqlite3.Connection) -> None:
    """Run migrations for existing databases."""
    cursor = conn.cursor()

    # Check if claimed_at column exists in rules_hierarchy
    cursor.execute("PRAGMA table_info(rules_hierarchy)")
    columns = [row[1] for row in cursor.fetchall()]

    if 'claimed_at' not in columns:
        cursor.execute("ALTER TABLE rules_hierarchy ADD COLUMN claimed_at TIMESTAMP")
        conn.commit()


# ============================================================
# GENERATION LOCK (Race condition prevention)
# ============================================================

GENERATION_TIMEOUT_MINUTES = 10


def try_claim_generation(
    conn: sqlite3.Connection,
    pattern: str,
    rule_type: str,
    code_type: str
) -> str:
    """
    Try to claim generation lock for a pattern.
    Uses BEGIN IMMEDIATE for atomic check-and-claim.
    Works for both 'guideline' and 'cms1500' rule types.

    Args:
        conn: Database connection
        pattern: Code pattern (E11, E11.65, etc.)
        rule_type: 'guideline' or 'cms1500'
        code_type: 'ICD-10', 'CPT', 'HCPCS'

    Returns:
        'claimed' - We got the lock, proceed with generation
        'exists' - Rule already exists (status='ready'), skip
        'wait' - Someone else is generating, wait and retry
    """
    from datetime import datetime, timedelta

    cursor = conn.cursor()

    # Use IMMEDIATE transaction for write lock
    conn.execute("BEGIN IMMEDIATE")
    try:
        cursor.execute("""
            SELECT status, claimed_at FROM rules_hierarchy
            WHERE pattern = ? AND rule_type = ? AND code_type = ?
        """, (pattern.upper(), rule_type, code_type))
        row = cursor.fetchone()

        if row:
            status = row[0]
            claimed_at_str = row[1]

            # Already has a ready rule
            if status in ('ready', 'active'):
                conn.commit()
                return 'exists'

            # Someone is generating
            if status == 'generating':
                # Check for timeout (stale lock)
                if claimed_at_str:
                    try:
                        claimed_at = datetime.fromisoformat(claimed_at_str)
                        if datetime.now() - claimed_at > timedelta(minutes=GENERATION_TIMEOUT_MINUTES):
                            # Stale lock - take over
                            cursor.execute("""
                                UPDATE rules_hierarchy
                                SET status = 'generating', claimed_at = ?
                                WHERE pattern = ? AND rule_type = ? AND code_type = ?
                            """, (datetime.now().isoformat(), pattern.upper(), rule_type, code_type))
                            conn.commit()
                            return 'claimed'
                    except:
                        pass

                conn.commit()
                return 'wait'

            # Other status (pending, failed, same_as_parent) - claim it
            cursor.execute("""
                UPDATE rules_hierarchy
                SET status = 'generating', claimed_at = ?
                WHERE pattern = ? AND rule_type = ? AND code_type = ?
            """, (datetime.now().isoformat(), pattern.upper(), rule_type, code_type))
            conn.commit()
            return 'claimed'

        # No row exists - insert new with 'generating' status
        pattern_type = _get_pattern_type_simple(pattern, code_type)
        cursor.execute("""
            INSERT INTO rules_hierarchy
            (pattern, pattern_type, code_type, rule_type, status, claimed_at)
            VALUES (?, ?, ?, ?, 'generating', ?)
        """, (pattern.upper(), pattern_type, code_type, rule_type, datetime.now().isoformat()))
        conn.commit()
        return 'claimed'

    except Exception as e:
        conn.rollback()
        raise


def release_generation_lock(
    conn: sqlite3.Connection,
    pattern: str,
    rule_type: str,
    code_type: str,
    success: bool = False
) -> None:
    """
    Release generation lock after completion or failure.
    Works for both 'guideline' and 'cms1500' rule types.

    Args:
        conn: Database connection
        pattern: Code pattern
        rule_type: 'guideline' or 'cms1500'
        code_type: 'ICD-10', 'CPT', 'HCPCS'
        success: True if generation succeeded (register_rule will set 'ready')
                 False if generation failed (reset to 'pending' for retry)
    """
    cursor = conn.cursor()

    if success:
        # Clear claimed_at, status will be set by register_rule()
        cursor.execute("""
            UPDATE rules_hierarchy
            SET claimed_at = NULL
            WHERE pattern = ? AND rule_type = ? AND code_type = ? AND status = 'generating'
        """, (pattern.upper(), rule_type, code_type))
    else:
        # Failed - reset to pending for retry
        cursor.execute("""
            UPDATE rules_hierarchy
            SET status = 'pending', claimed_at = NULL
            WHERE pattern = ? AND rule_type = ? AND code_type = ? AND status = 'generating'
        """, (pattern.upper(), rule_type, code_type))

    conn.commit()


def _get_pattern_type_simple(pattern: str, code_type: str) -> str:
    """Simple pattern type detection for lock insertion."""
    if not pattern:
        return "unknown"
    if len(pattern) == 1:
        return "meta_category"
    if code_type == "ICD-10":
        if '.' not in pattern:
            return "category"
        parts = pattern.split('.')
        suffix_len = len(parts[1]) if len(parts) > 1 else 0
        if suffix_len == 1:
            return "subcategory"
        elif suffix_len >= 2:
            return "code"
        return "category"
    else:
        if len(pattern) <= 2:
            return "category"
        elif len(pattern) <= 4:
            return "subcategory"
        else:
            return "code"
