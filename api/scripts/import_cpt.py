"""
Import CPT codes from xlsx file into database.
"""

import pandas as pd
import sqlite3
from pathlib import Path


def import_cpt_codes(xlsx_path: str, db_path: str):
    """
    Import CPT codes from xlsx file into cpt table.

    Args:
        xlsx_path: Path to the CPT xlsx file
        db_path: Path to the SQLite database
    """
    print(f"Reading CPT data from {xlsx_path}...")

    # Read the main sheet with all CPT codes
    df = pd.read_excel(xlsx_path, sheet_name='ALL 2026 CPT Codes')

    print(f"Found {len(df)} CPT codes")
    print(f"Columns: {list(df.columns)}")

    # Rename columns for clarity
    df = df.rename(columns={
        'Procedure Code Category': 'category',
        'CPT Codes': 'code',
        'Procedure Code Descriptions': 'description',
        'Code Status': 'status'
    })

    # Convert code to string (some might be read as int)
    df['code'] = df['code'].astype(str).str.strip()

    # Clean up data
    df['description'] = df['description'].fillna('').str.strip()
    df['category'] = df['category'].fillna('').str.strip()
    df['status'] = df['status'].fillna('').str.strip()

    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cpt (
            code TEXT PRIMARY KEY,
            description TEXT,
            category TEXT,
            status TEXT
        )
    """)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cpt_code ON cpt(code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cpt_category ON cpt(category)")

    # Clear existing data
    cursor.execute("DELETE FROM cpt")

    # Insert data
    records = df[['code', 'description', 'category', 'status']].values.tolist()
    cursor.executemany(
        "INSERT OR REPLACE INTO cpt (code, description, category, status) VALUES (?, ?, ?, ?)",
        records
    )

    conn.commit()

    # Verify
    cursor.execute("SELECT COUNT(*) FROM cpt")
    count = cursor.fetchone()[0]
    print(f"Imported {count} CPT codes into database")

    # Show sample
    cursor.execute("SELECT code, description FROM cpt LIMIT 5")
    print("\nSample data:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1][:60]}...")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    # Default paths
    project_root = Path(__file__).parent.parent.parent
    xlsx_path = project_root / "data" / "raw" / "reference" / "cpt" / "cpt-pcm-nhsn.xlsx"
    db_path = project_root / "data" / "db" / "reference.db"

    import_cpt_codes(str(xlsx_path), str(db_path))
