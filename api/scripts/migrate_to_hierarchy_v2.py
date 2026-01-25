#!/usr/bin/env python3
"""
Migration script for hierarchy-based extraction V2.

FIXES from V1:
1. Saves content.json and content.txt for each document
2. Validates code_type (must be ICD-10, CPT, HCPCS)
3. Filters garbage from parsing
4. Consistent file structure

Run this AFTER:
- load_reference_db.py (to populate code_hierarchy)
- load_topics.py (to populate topics_dictionary)
"""

import sys
import os
import json
import asyncio
from pathlib import Path
from datetime import datetime
from dataclasses import asdict

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import REFERENCE_DB_PATH, RAW_DOCUMENTS_DIR, DOCUMENTS_STORE_DIR
from src.db.connection import get_db_connection
from src.parsers.multi_model_pipeline import (
    run_extraction_pipeline,
    load_topics_from_db,
    parse_pipeline_result
)
from src.parsers.document_parser import (
    load_meta_categories_from_json,
    build_document_data,
    validate_document,
    PageData,
    CodeInfo,
    TopicInfo
)

# Valid code types
VALID_CODE_TYPES = {'ICD-10', 'CPT', 'HCPCS', 'NDC'}


def get_all_documents():
    """Get all documents from database."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT file_hash, filename, source_path, doc_type, total_pages
        FROM documents
        WHERE source_path IS NOT NULL
    """)

    docs = []
    for row in cursor.fetchall():
        docs.append({
            "id": row[0],  # file_hash is the PK
            "filename": row[1],
            "filepath": row[2],  # source_path
            "doc_type": row[3],
            "total_pages": row[4]
        })

    conn.close()
    return docs


def clear_document_extractions():
    """Clear existing document_codes and document_topics."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Count before clearing
    cursor.execute("SELECT COUNT(*) FROM document_codes")
    codes_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM document_topics")
    topics_count = cursor.fetchone()[0]

    print(f"  Clearing {codes_count} document_codes entries...")
    cursor.execute("DELETE FROM document_codes")

    print(f"  Clearing {topics_count} document_topics entries...")
    cursor.execute("DELETE FROM document_topics")

    conn.commit()
    conn.close()

    return codes_count, topics_count


def get_pdf_text_chunks(filepath: str) -> list:
    """Extract text from PDF as list of page texts."""
    import fitz  # PyMuPDF

    full_path = RAW_DOCUMENTS_DIR / filepath
    if not full_path.exists():
        print(f"    Warning: File not found: {full_path}")
        return []

    chunks = []
    try:
        doc = fitz.open(str(full_path))
        for page_num in range(len(doc)):
            page = doc[page_num]
            text = page.get_text()
            chunks.append(text)
        doc.close()
    except Exception as e:
        print(f"    Error reading PDF: {e}")
        return []

    return chunks


def validate_code_info(code_info: CodeInfo) -> bool:
    """Validate that code_info has valid type and pattern."""
    import re

    if not code_info.type or not code_info.code:
        return False

    code_type = code_info.type.upper().strip()
    code = code_info.code.upper().strip()

    # Must be valid type
    if code_type not in VALID_CODE_TYPES:
        return False

    # Code pattern must be short (meta-category is 1-3 chars typically)
    if len(code) > 10:
        return False

    # Code should not contain quotes or long text
    if '"' in code or len(code.split()) > 1:
        return False

    # Must match valid code patterns
    # ICD-10: letter + optional digits/dot (A, E11, E11.6)
    # CPT: 5 digits or single digit for meta-category (99213, 9)
    # HCPCS: letter + 4 digits or single letter (J1950, J)

    if code_type == 'ICD-10':
        # Must start with letter, optionally followed by digits and dot
        # Valid: A, E11, E11.6, E11.65 (dot requires digit after)
        if not re.match(r'^[A-Z](\d{1,2}(\.\d{1,3})?)?$', code):
            return False
    elif code_type == 'CPT':
        # Must be digits only (1-5 chars) or single digit
        if not re.match(r'^\d{1,5}$', code):
            return False
    elif code_type == 'HCPCS':
        # Must start with letter, followed by up to 4 digits
        if not re.match(r'^[A-Z]\d{0,4}$', code):
            return False

    return True


def filter_valid_codes(pages_data: list) -> tuple:
    """Filter pages_data to keep only valid codes.

    Returns:
        (filtered_pages_data, stats_dict)
    """
    stats = {
        "total_codes": 0,
        "valid_codes": 0,
        "invalid_codes": 0,
        "invalid_examples": []
    }

    for page in pages_data:
        valid_codes = []
        for code_info in page.codes:
            stats["total_codes"] += 1

            if validate_code_info(code_info):
                # Normalize code type
                code_info.type = code_info.type.upper().strip()
                if code_info.type in ['ICD10', 'ICD']:
                    code_info.type = 'ICD-10'
                valid_codes.append(code_info)
                stats["valid_codes"] += 1
            else:
                stats["invalid_codes"] += 1
                if len(stats["invalid_examples"]) < 5:
                    stats["invalid_examples"].append({
                        "code": code_info.code[:50],
                        "type": str(code_info.type)[:50]
                    })

        page.codes = valid_codes

    return pages_data, stats


def save_content_files(doc_id: str, filename: str, pages_data: list, total_pages: int):
    """Save content.json and content.txt files using DocumentData for proper summary."""
    from src.parsers.document_parser import DocumentData

    doc_dir = DOCUMENTS_STORE_DIR / doc_id
    doc_dir.mkdir(parents=True, exist_ok=True)

    # Use DocumentData to build proper summary with anchors
    doc_data = DocumentData(
        file_hash=doc_id,
        filename=filename,
        total_pages=total_pages,
        parsed_at=datetime.utcnow().isoformat() + "Z",
        pages=pages_data
    )

    # Convert to dict (includes properly built summary with anchors)
    content_json = doc_data.to_dict()

    # Build content.txt
    content_txt_parts = []
    for page in pages_data:
        content_txt_parts.append(f"## Page {page.page}")
        if page.content:
            content_txt_parts.append(page.content)
        content_txt_parts.append("")

    # Write files
    json_path = doc_dir / "content.json"
    txt_path = doc_dir / "content.txt"

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(content_json, f, indent=2, ensure_ascii=False)

    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(content_txt_parts))

    return json_path, txt_path


def get_category_description(pattern: str, code_type: str, meta_categories: dict) -> str:
    """Look up category description from meta_categories.json.

    Args:
        pattern: Code pattern like 'E', 'J', '9', '00'
        code_type: 'ICD-10', 'CPT', 'HCPCS'
        meta_categories: Loaded meta_categories dict

    Returns:
        Description string or None
    """
    if not pattern:
        return None

    # Map code_type to meta_categories key
    type_map = {
        'ICD-10': 'ICD-10',
        'CPT': 'CPT',
        'HCPCS': 'HCPCS',
        'NDC': 'NDC'
    }

    mc_key = type_map.get(code_type)
    if not mc_key or mc_key not in meta_categories:
        return None

    categories = meta_categories[mc_key].get('categories', {})

    # Determine category key
    # CPT: check for "00" (Anesthesia) first, then single digit
    # ICD-10/HCPCS: first letter
    if code_type == 'CPT':
        if pattern.startswith('0') and '00' in categories:
            cat_key = '00'
        else:
            cat_key = pattern[0]
    else:
        cat_key = pattern[0].upper()

    if cat_key in categories:
        cat_info = categories[cat_key]
        name = cat_info.get('name', '')
        range_str = cat_info.get('range', '')
        if name and range_str:
            return f"{name} ({range_str})"
        return name or range_str

    return None


def save_document_codes(doc_id: str, pages_data: list, meta_categories: dict):
    """Save extracted code categories to document_codes.

    Aggregates page numbers for codes that appear on multiple pages.
    Looks up description from meta_categories.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Aggregate codes: {(pattern, type): {"pages": set}}
    code_aggregates = {}

    for page in pages_data:
        for code_info in page.codes:
            pattern = code_info.code.upper().strip()
            code_type = code_info.type.upper().strip()

            key = (pattern, code_type)

            if key not in code_aggregates:
                code_aggregates[key] = {"pages": set()}

            code_aggregates[key]["pages"].add(page.page)

    # Insert aggregated codes
    codes_added = 0
    for (pattern, code_type), data in code_aggregates.items():
        # Format page numbers as comma-separated string
        pages_str = ",".join(str(p) for p in sorted(data["pages"]))

        # Get description from meta_categories
        description = get_category_description(pattern, code_type, meta_categories)

        cursor.execute("""
            INSERT OR IGNORE INTO document_codes
            (document_id, code_pattern, code_type, description, page_numbers)
            VALUES (?, ?, ?, ?, ?)
        """, (doc_id, pattern, code_type, description, pages_str))
        codes_added += 1

    conn.commit()
    conn.close()

    return codes_added


def save_document_topics(doc_id: str, pages_data: list, topics_dict: dict, topics_aliases: dict):
    """Save extracted topics to document_topics."""
    conn = get_db_connection()
    cursor = conn.cursor()

    topics_added = 0
    topics_skipped = 0
    seen_topics = set()
    skipped_names = set()

    for page in pages_data:
        for topic_info in page.topics:
            # Handle both TopicInfo objects and strings
            if hasattr(topic_info, 'name'):
                topic_name = topic_info.name
                anchor_start = getattr(topic_info, 'anchor_start', None)
                anchor_end = getattr(topic_info, 'anchor_end', None)
                topic_id_preset = getattr(topic_info, 'topic_id', None)
            else:
                topic_name = str(topic_info)
                anchor_start = None
                anchor_end = None
                topic_id_preset = None

            topic_name_lower = topic_name.lower().strip()

            # Skip duplicates within same document
            key = (doc_id, topic_name_lower)
            if key in seen_topics:
                continue
            seen_topics.add(key)

            # Get topic_id from dictionary
            topic_id = topic_id_preset

            # Try exact match
            if not topic_id:
                topic_id = topics_dict.get(topic_name_lower)

            # Try aliases
            if not topic_id:
                topic_id = topics_aliases.get(topic_name_lower)

            # Try partial match
            if not topic_id:
                for name, tid in topics_dict.items():
                    if topic_name_lower in name or name in topic_name_lower:
                        topic_id = tid
                        break

            # Skip topics not in dictionary
            if not topic_id:
                topics_skipped += 1
                skipped_names.add(topic_name)
                continue

            cursor.execute("""
                INSERT OR IGNORE INTO document_topics
                (document_id, topic_id, anchor_start, anchor_end, page, extracted_by)
                VALUES (?, ?, ?, ?, ?, 'gemini_flash')
            """, (
                doc_id,
                topic_id,
                anchor_start,
                anchor_end,
                page.page
            ))
            topics_added += 1

    conn.commit()
    conn.close()

    if skipped_names:
        print(f"    [Filter] Skipped {topics_skipped} non-dictionary topics: {list(skipped_names)[:5]}{'...' if len(skipped_names) > 5 else ''}")

    return topics_added


def load_topics_dict():
    """Load topics dictionary keyed by lowercase name, plus aliases."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id, name, aliases FROM topics_dictionary")

    topics_dict = {}
    aliases_dict = {}

    for row in cursor.fetchall():
        topic_id = row[0]
        name = row[1]
        aliases_json = row[2]

        # Add name
        topics_dict[name.lower()] = topic_id

        # Add aliases
        if aliases_json:
            try:
                aliases = json.loads(aliases_json)
                for alias in aliases:
                    aliases_dict[alias.lower()] = topic_id
            except:
                pass

    conn.close()
    return topics_dict, aliases_dict


async def process_document(doc: dict, topics: list, meta_categories: dict, topics_dict: dict, topics_aliases: dict, doc_index: int = 0, total_docs: int = 0):
    """Process a single document through multi-model pipeline."""
    import time
    start_time = time.time()

    print(f"\n{'='*60}", flush=True)
    print(f"[{doc_index}/{total_docs}] START: {doc['filename']}", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  Path: {doc['filepath']}", flush=True)
    print(f"  Pages: {doc['total_pages'] or 'unknown'}", flush=True)

    # Get PDF text
    print(f"  [1/5] Extracting text...", flush=True)
    chunks = get_pdf_text_chunks(doc['filepath'])
    if not chunks:
        print(f"  ❌ SKIP: no text extracted", flush=True)
        return {"codes": 0, "topics": 0, "error": "No text extracted"}

    print(f"  [1/5] ✅ Extracted {len(chunks)} pages", flush=True)

    # Run Gemini Flash only
    try:
        print(f"  [2/5] Running Gemini extraction (per-page, 30 parallel)...", flush=True)
        result = await run_extraction_pipeline(
            pdf_text_chunks=chunks,
            topics=topics,
            meta_categories=meta_categories,
            skip_critic=True,
            skip_fix=True,
            chunk_size=1,
            parallel_limit=30
        )

        # Parse result
        pages_data = parse_pipeline_result(result)
        print(f"  [2/5] ✅ Pipeline complete: {len(pages_data)} pages", flush=True)

        # Final document validation
        print(f"  [3/5] Validating...", flush=True)
        original_pages = {i + 1: text for i, text in enumerate(chunks)}
        final_pages = {p.page: p for p in pages_data}
        validation = validate_document(final_pages, original_pages)

        if not validation.valid:
            print(f"    ⚠️ Validation issues: OK={validation.content_ok}, Mismatch={validation.content_mismatch}", flush=True)
            if validation.missing_pages:
                print(f"    Missing pages: {validation.missing_pages[:10]}...", flush=True)

        # Filter invalid codes
        pages_data, filter_stats = filter_valid_codes(pages_data)
        print(f"  [3/5] ✅ Valid codes: {filter_stats['valid_codes']}/{filter_stats['total_codes']}", flush=True)

        # Save content files (JSON + TXT)
        print(f"  [4/5] Saving files...", flush=True)
        json_path, txt_path = save_content_files(
            doc['id'],
            doc['filename'],
            pages_data,
            doc['total_pages'] or len(chunks)
        )
        print(f"  [4/5] ✅ Saved: {json_path.name}, {txt_path.name}", flush=True)

        # Save to database
        print(f"  [5/5] Saving to DB...", flush=True)
        codes_added = save_document_codes(doc['id'], pages_data, meta_categories)
        topics_added = save_document_topics(doc['id'], pages_data, topics_dict, topics_aliases)

        elapsed = time.time() - start_time
        print(f"  [5/5] ✅ DB: {codes_added} codes, {topics_added} topics", flush=True)
        print(f"\n[{doc_index}/{total_docs}] ✅ DONE: {doc['filename']} ({elapsed:.1f}s)", flush=True)

        return {
            "codes": codes_added,
            "topics": topics_added,
            "valid_codes": filter_stats["valid_codes"],
            "invalid_codes": filter_stats["invalid_codes"],
            "json_path": str(json_path),
            "txt_path": str(txt_path),
            "elapsed": elapsed
        }

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n[{doc_index}/{total_docs}] ❌ ERROR: {doc['filename']} ({elapsed:.1f}s)", flush=True)
        print(f"    {e}", flush=True)
        import traceback
        traceback.print_exc()
        return {"codes": 0, "topics": 0, "error": str(e), "elapsed": elapsed}


async def run_migration():
    """Run the full migration."""
    print("=" * 60)
    print("Migration to Hierarchy-Based Extraction V2")
    print("=" * 60)
    print(f"Started: {datetime.now().isoformat()}")
    print(f"Config: Gemini 2.5 Flash only, skip_critic=True, skip_fix=True")

    # Step 1: Get documents
    print("\n1. Loading documents...")
    docs = get_all_documents()
    print(f"   Found {len(docs)} documents")

    if not docs:
        print("   No documents to process!")
        return

    # Step 2: Clear existing extractions
    print("\n2. Clearing existing extractions...")
    codes_cleared, topics_cleared = clear_document_extractions()

    # Step 3: Load topics and meta-categories
    print("\n3. Loading extraction config...")
    topics = load_topics_from_db()
    print(f"   Loaded {len(topics)} topics from dictionary")

    meta_categories = load_meta_categories_from_json()
    print(f"   Loaded meta-categories")

    topics_dict, topics_aliases = load_topics_dict()
    print(f"   Loaded {len(topics_dict)} topic names + {len(topics_aliases)} aliases")

    # Step 4: Process each document
    print("\n4. Processing documents...", flush=True)
    results = []

    for i, doc in enumerate(docs):
        result = await process_document(
            doc, topics, meta_categories, topics_dict, topics_aliases,
            doc_index=i+1, total_docs=len(docs)
        )
        result["filename"] = doc["filename"]
        results.append(result)

    # Summary
    print("\n" + "=" * 60)
    print("MIGRATION SUMMARY")
    print("=" * 60)

    total_codes = sum(r.get("codes", 0) for r in results)
    total_topics = sum(r.get("topics", 0) for r in results)
    total_valid = sum(r.get("valid_codes", 0) for r in results)
    total_invalid = sum(r.get("invalid_codes", 0) for r in results)
    errors = [r for r in results if "error" in r]

    print(f"Documents processed: {len(docs)}")
    print(f"Codes in DB: {total_codes}")
    print(f"Topics in DB: {total_topics}")
    print(f"Valid codes: {total_valid}")
    print(f"Invalid codes filtered: {total_invalid}")
    print(f"Errors: {len(errors)}")

    if errors:
        print("\nDocuments with errors:")
        for r in errors:
            print(f"  - {r['filename']}: {r['error']}")

    print(f"\nFinished: {datetime.now().isoformat()}")

    # Verify database
    print("\n" + "=" * 60)
    print("DATABASE VERIFICATION")
    print("=" * 60)
    verify_database()


def verify_database():
    """Verify database contents after migration."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check document_codes
    cursor.execute("SELECT COUNT(*) FROM document_codes")
    total_codes = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*) FROM document_codes
        WHERE code_type NOT IN ('ICD-10', 'CPT', 'HCPCS', 'NDC')
    """)
    garbage_codes = cursor.fetchone()[0]

    print(f"document_codes: {total_codes} total, {garbage_codes} garbage")

    # Show code distribution
    cursor.execute("""
        SELECT code_type, COUNT(*) as cnt
        FROM document_codes
        GROUP BY code_type
        ORDER BY cnt DESC
    """)
    print("  By type:")
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]}")

    # Check document_topics
    cursor.execute("SELECT COUNT(*) FROM document_topics")
    total_topics = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*) FROM document_topics dt
        WHERE NOT EXISTS (SELECT 1 FROM topics_dictionary td WHERE td.id = dt.topic_id)
    """)
    orphan_topics = cursor.fetchone()[0]

    print(f"\ndocument_topics: {total_topics} total, {orphan_topics} orphans")

    # Check files exist
    cursor.execute("SELECT file_hash, filename FROM documents")
    docs = cursor.fetchall()

    print(f"\nFiles check:")
    for doc in docs:
        file_hash, filename = doc
        json_exists = (DOCUMENTS_STORE_DIR / file_hash / "content.json").exists()
        txt_exists = (DOCUMENTS_STORE_DIR / file_hash / "content.txt").exists()
        status = "✅" if (json_exists and txt_exists) else "❌"
        print(f"  {status} {filename}: json={json_exists}, txt={txt_exists}")

    conn.close()

    if garbage_codes > 0 or orphan_topics > 0:
        print("\n⚠️  WARNING: Database has issues!")
    else:
        print("\n✅ Database verification passed!")


def main():
    """Entry point."""
    asyncio.run(run_migration())


if __name__ == "__main__":
    main()
