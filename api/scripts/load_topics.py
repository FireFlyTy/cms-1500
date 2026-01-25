#!/usr/bin/env python3
"""
Load topics dictionary from seed file into SQLite database
"""
import sys
import json
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import REFERENCE_DB_PATH, DATA_DIR
from src.db.models import init_db


def load_topics_dictionary(conn, seed_file: Path):
    """Load topics from JSON seed file into topics_dictionary table."""
    print(f"Loading topics from {seed_file.name}...")

    with open(seed_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    topics = data.get('topics', [])
    cursor = conn.cursor()

    # Clear existing topics
    cursor.execute("DELETE FROM topics_dictionary")

    count = 0
    for topic in topics:
        name = topic.get('name')
        if not name:
            continue

        # Convert patterns lists to JSON strings
        icd10_patterns = json.dumps(topic.get('icd10_patterns', [])) if topic.get('icd10_patterns') else None
        cpt_patterns = json.dumps(topic.get('cpt_patterns', [])) if topic.get('cpt_patterns') else None
        hcpcs_patterns = json.dumps(topic.get('hcpcs_patterns', [])) if topic.get('hcpcs_patterns') else None
        aliases = json.dumps(topic.get('aliases', [])) if topic.get('aliases') else None

        cursor.execute("""
            INSERT OR REPLACE INTO topics_dictionary
            (name, category, aliases, description, icd10_patterns, cpt_patterns, hcpcs_patterns)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            name,
            topic.get('category', 'other'),
            aliases,
            topic.get('description'),
            icd10_patterns,
            cpt_patterns,
            hcpcs_patterns,
        ))
        count += 1

    conn.commit()

    # Summary by category
    cursor.execute("""
        SELECT category, COUNT(*)
        FROM topics_dictionary
        GROUP BY category
        ORDER BY category
    """)
    categories = cursor.fetchall()

    print(f"  ✓ Loaded {count} topics:")
    for cat, cnt in categories:
        print(f"    {cat}: {cnt}")

    return count


def main():
    print("=" * 60)
    print("Loading Topics Dictionary")
    print("=" * 60)

    # Initialize database (creates tables if needed)
    conn = init_db(REFERENCE_DB_PATH)

    # Load topics from seed file
    seed_file = DATA_DIR / "seed" / "topics_dictionary.json"

    if not seed_file.exists():
        print(f"Error: Seed file not found: {seed_file}")
        sys.exit(1)

    count = load_topics_dictionary(conn, seed_file)

    conn.close()

    print(f"\nTopics loaded: {count}")
    print(f"Database: {REFERENCE_DB_PATH}")


if __name__ == "__main__":
    main()
