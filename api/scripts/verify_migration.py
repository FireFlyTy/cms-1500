#!/usr/bin/env python3
"""
Migration Verification Script

Verifies the V2 migration results:
1. Document statistics (codes, topics, medications per document)
2. Entity statistics (which documents have each code/topic)
3. Artifact verification (content.json, content.txt exist)
4. Database integrity checks

Output: JSON report saved to data/reports/
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import DOCUMENTS_STORE_DIR
from src.db.connection import get_db_connection


def get_all_documents():
    """Get all documents from database."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT file_hash, filename, doc_type, total_pages
        FROM documents
        WHERE source_path IS NOT NULL
    """)

    docs = []
    for row in cursor.fetchall():
        docs.append({
            "id": row[0],
            "filename": row[1],
            "doc_type": row[2],
            "total_pages": row[3]
        })

    conn.close()
    return docs


def check_document_artifacts(doc_id: str):
    """Check if document has content.json and content.txt."""
    doc_dir = DOCUMENTS_STORE_DIR / doc_id

    json_exists = (doc_dir / "content.json").exists()
    txt_exists = (doc_dir / "content.txt").exists()

    json_size = (doc_dir / "content.json").stat().st_size if json_exists else 0
    txt_size = (doc_dir / "content.txt").stat().st_size if txt_exists else 0

    return {
        "dir_exists": doc_dir.exists(),
        "json_exists": json_exists,
        "txt_exists": txt_exists,
        "json_size": json_size,
        "txt_size": txt_size
    }


def load_document_json(doc_id: str):
    """Load document content.json."""
    json_path = DOCUMENTS_STORE_DIR / doc_id / "content.json"
    if not json_path.exists():
        return None

    with open(json_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_document_codes_from_db(doc_id: str):
    """Get codes for document from database."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT code_pattern, code_type, description, page_numbers
        FROM document_codes
        WHERE document_id = ?
    """, (doc_id,))

    codes = []
    for row in cursor.fetchall():
        # Parse page_numbers - can be JSON array or comma-separated
        pages = []
        if row[3]:
            if row[3].startswith('['):
                pages = json.loads(row[3])
            else:
                pages = [int(p.strip()) for p in row[3].split(',') if p.strip()]
        codes.append({
            "code": row[0],
            "type": row[1],
            "description": row[2],
            "pages": pages
        })

    conn.close()
    return codes


def get_document_topics_from_db(doc_id: str):
    """Get topics for document from database."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT dt.topic_id, td.name, dt.page
        FROM document_topics dt
        LEFT JOIN topics_dictionary td ON dt.topic_id = td.id
        WHERE dt.document_id = ?
    """, (doc_id,))

    topics = []
    for row in cursor.fetchall():
        topics.append({
            "topic_id": row[0],
            "name": row[1],
            "page": row[2]
        })

    conn.close()
    return topics


def check_database_integrity():
    """Check database integrity."""
    conn = get_db_connection()
    cursor = conn.cursor()

    results = {
        "document_codes": {},
        "document_topics": {},
        "issues": []
    }

    # Document codes stats
    cursor.execute("SELECT COUNT(*) FROM document_codes")
    results["document_codes"]["total"] = cursor.fetchone()[0]

    cursor.execute("SELECT code_type, COUNT(*) FROM document_codes GROUP BY code_type")
    results["document_codes"]["by_type"] = {row[0]: row[1] for row in cursor.fetchall()}

    # Check for NULL descriptions
    cursor.execute("SELECT COUNT(*) FROM document_codes WHERE description IS NULL")
    null_desc_count = cursor.fetchone()[0]
    results["document_codes"]["null_descriptions"] = null_desc_count
    if null_desc_count > 0:
        results["issues"].append(f"{null_desc_count} codes with NULL description")

    # Document topics stats
    cursor.execute("SELECT COUNT(*) FROM document_topics")
    results["document_topics"]["total"] = cursor.fetchone()[0]

    # Check for orphan topic_ids
    cursor.execute("""
        SELECT COUNT(*) FROM document_topics dt
        LEFT JOIN topics_dictionary td ON dt.topic_id = td.id
        WHERE td.id IS NULL
    """)
    orphan_count = cursor.fetchone()[0]
    results["document_topics"]["orphan_topic_ids"] = orphan_count
    if orphan_count > 0:
        results["issues"].append(f"{orphan_count} topics with orphan topic_id")

    # Topics dictionary stats
    cursor.execute("SELECT COUNT(*) FROM topics_dictionary")
    results["topics_dictionary_count"] = cursor.fetchone()[0]

    # Code hierarchy stats
    cursor.execute("SELECT COUNT(*) FROM code_hierarchy")
    results["code_hierarchy_count"] = cursor.fetchone()[0]

    conn.close()
    return results


def build_entity_index(documents):
    """Build index: which documents have each code/topic."""
    code_to_docs = defaultdict(list)
    topic_to_docs = defaultdict(list)

    for doc in documents:
        doc_id = doc["id"]
        doc_name = doc["filename"]

        # Get codes from JSON
        doc_json = load_document_json(doc_id)
        if doc_json:
            # Collect unique codes
            codes_seen = set()
            for page in doc_json.get("pages", []):
                for code in page.get("codes", []):
                    code_key = f"{code.get('code')} ({code.get('type')})"
                    if code_key not in codes_seen:
                        codes_seen.add(code_key)
                        code_to_docs[code_key].append(doc_name)

            # Collect unique topics
            topics_seen = set()
            for page in doc_json.get("pages", []):
                for topic in page.get("topics", []):
                    topic_name = topic.get("name") if isinstance(topic, dict) else topic
                    if topic_name and topic_name not in topics_seen:
                        topics_seen.add(topic_name)
                        topic_to_docs[topic_name].append(doc_name)

    return dict(code_to_docs), dict(topic_to_docs)


def main():
    print("=" * 60)
    print("Migration Verification")
    print("=" * 60)
    print(f"Started: {datetime.now().isoformat()}")
    print()

    report = {
        "timestamp": datetime.now().isoformat(),
        "documents": {},
        "entity_index": {
            "codes": {},
            "topics": {}
        },
        "database": {},
        "summary": {
            "total_documents": 0,
            "documents_with_artifacts": 0,
            "documents_with_codes": 0,
            "documents_with_topics": 0,
            "total_codes_in_json": 0,
            "total_topics_in_json": 0,
            "issues": []
        }
    }

    # 1. Get all documents
    print("1. Loading documents...")
    documents = get_all_documents()
    report["summary"]["total_documents"] = len(documents)
    print(f"   Found {len(documents)} documents")
    print()

    # 2. Check each document
    print("2. Checking documents...")
    for doc in documents:
        doc_id = doc["id"]
        doc_name = doc["filename"]
        print(f"   - {doc_name}")

        doc_report = {
            "id": doc_id,
            "doc_type": doc["doc_type"],
            "total_pages": doc["total_pages"],
            "artifacts": check_document_artifacts(doc_id),
            "codes_db": [],
            "topics_db": [],
            "codes_json": [],
            "topics_json": [],
            "medications_json": []
        }

        # Get from DB
        doc_report["codes_db"] = get_document_codes_from_db(doc_id)
        doc_report["topics_db"] = get_document_topics_from_db(doc_id)

        # Get from JSON
        doc_json = load_document_json(doc_id)
        if doc_json:
            # Count unique codes
            codes_seen = {}
            topics_seen = {}
            meds_seen = {}

            for page in doc_json.get("pages", []):
                for code in page.get("codes", []):
                    code_key = code.get("code")
                    if code_key and code_key not in codes_seen:
                        codes_seen[code_key] = {
                            "code": code.get("code"),
                            "type": code.get("type"),
                            "has_anchor": bool(code.get("anchor_start") or code.get("start"))
                        }

                for topic in page.get("topics", []):
                    topic_name = topic.get("name") if isinstance(topic, dict) else topic
                    if topic_name and topic_name not in topics_seen:
                        has_anchor = False
                        if isinstance(topic, dict):
                            has_anchor = bool(topic.get("anchor_start") or topic.get("start"))
                        topics_seen[topic_name] = {
                            "name": topic_name,
                            "has_anchor": has_anchor
                        }

                for med in page.get("medications", []):
                    med_name = med.get("name") if isinstance(med, dict) else med
                    if med_name and med_name not in meds_seen:
                        has_anchor = False
                        if isinstance(med, dict):
                            has_anchor = bool(med.get("anchor_start") or med.get("start"))
                        meds_seen[med_name] = {
                            "name": med_name,
                            "has_anchor": has_anchor
                        }

            doc_report["codes_json"] = list(codes_seen.values())
            doc_report["topics_json"] = list(topics_seen.values())
            doc_report["medications_json"] = list(meds_seen.values())

        # Update summary
        if doc_report["artifacts"]["json_exists"] and doc_report["artifacts"]["txt_exists"]:
            report["summary"]["documents_with_artifacts"] += 1
        else:
            report["summary"]["issues"].append(f"{doc_name}: missing artifacts")

        if doc_report["codes_json"]:
            report["summary"]["documents_with_codes"] += 1
            report["summary"]["total_codes_in_json"] += len(doc_report["codes_json"])

        if doc_report["topics_json"]:
            report["summary"]["documents_with_topics"] += 1
            report["summary"]["total_topics_in_json"] += len(doc_report["topics_json"])

        # Check: document should have at least one code or topic
        if not doc_report["codes_json"] and not doc_report["topics_json"]:
            report["summary"]["issues"].append(f"{doc_name}: no codes or topics extracted")

        report["documents"][doc_name] = doc_report

        print(f"     Codes: {len(doc_report['codes_json'])} (DB: {len(doc_report['codes_db'])})")
        print(f"     Topics: {len(doc_report['topics_json'])} (DB: {len(doc_report['topics_db'])})")
        print(f"     Meds: {len(doc_report['medications_json'])}")

    print()

    # 3. Build entity index
    print("3. Building entity index...")
    code_to_docs, topic_to_docs = build_entity_index(documents)
    report["entity_index"]["codes"] = code_to_docs
    report["entity_index"]["topics"] = topic_to_docs
    print(f"   Unique codes: {len(code_to_docs)}")
    print(f"   Unique topics: {len(topic_to_docs)}")
    print()

    # 4. Check database integrity
    print("4. Checking database integrity...")
    db_check = check_database_integrity()
    report["database"] = db_check
    report["summary"]["issues"].extend(db_check["issues"])

    print(f"   document_codes: {db_check['document_codes']['total']}")
    print(f"   document_topics: {db_check['document_topics']['total']}")
    print(f"   topics_dictionary: {db_check['topics_dictionary_count']}")
    print(f"   code_hierarchy: {db_check['code_hierarchy_count']}")
    if db_check["issues"]:
        print(f"   Issues: {db_check['issues']}")
    print()

    # 5. Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Documents: {report['summary']['total_documents']}")
    print(f"  With artifacts: {report['summary']['documents_with_artifacts']}")
    print(f"  With codes: {report['summary']['documents_with_codes']}")
    print(f"  With topics: {report['summary']['documents_with_topics']}")
    print(f"Total codes in JSON: {report['summary']['total_codes_in_json']}")
    print(f"Total topics in JSON: {report['summary']['total_topics_in_json']}")
    print()

    if report["summary"]["issues"]:
        print("ISSUES:")
        for issue in report["summary"]["issues"]:
            print(f"  - {issue}")
    else:
        print("No issues found!")
    print()

    # 6. Save report
    report_dir = Path(__file__).parent.parent.parent / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    report_path = report_dir / f"migration_verification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"Report saved: {report_path}")

    # Return success/failure
    return len(report["summary"]["issues"]) == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
