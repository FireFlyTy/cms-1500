#!/usr/bin/env python3
"""
Test the new validation pipeline on a single document.
Saves results to content.json if --save flag is provided.
"""

import sys
import json
import asyncio
from pathlib import Path
from datetime import datetime
from dataclasses import asdict

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import RAW_DOCUMENTS_DIR, DOCUMENTS_STORE_DIR
from src.db.connection import get_db_connection
from src.parsers.multi_model_pipeline import run_extraction_pipeline, load_topics_from_db
from src.parsers.document_parser import load_meta_categories_from_json, validate_document, PageData
import fitz


def get_document(doc_id: str):
    """Get document info from database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT file_hash, filename, source_path, total_pages
        FROM documents WHERE file_hash = ?
    """, (doc_id,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return {
            "id": row[0],
            "filename": row[1],
            "filepath": row[2],
            "total_pages": row[3]
        }
    return None


def get_pdf_chunks(filepath: str):
    """Extract text from PDF."""
    full_path = RAW_DOCUMENTS_DIR / filepath
    chunks = []
    doc = fitz.open(str(full_path))
    for i in range(len(doc)):
        chunks.append(doc[i].get_text())
    doc.close()
    return chunks


async def test_document(doc_id: str):
    """Test parsing and validation on single document."""
    print("=" * 60)
    print(f"Testing document: {doc_id[:16]}...")
    print("=" * 60)

    # Get document info
    doc = get_document(doc_id)
    if not doc:
        print(f"Document not found: {doc_id}")
        return

    print(f"Filename: {doc['filename']}")
    print(f"Total pages: {doc['total_pages']}")
    print()

    # Load PDF
    print("Loading PDF...")
    chunks = get_pdf_chunks(doc['filepath'])
    print(f"Extracted {len(chunks)} pages")

    # Load topics and meta-categories
    print("Loading config...")
    topics = load_topics_from_db()
    meta_categories = load_meta_categories_from_json()
    print(f"Topics: {len(topics)}, Meta-categories loaded")
    print()

    # Run pipeline (Gemini only, skip critic/fix for speed)
    print("Running extraction pipeline...")
    print("-" * 40)

    result = await run_extraction_pipeline(
        pdf_text_chunks=chunks,
        topics=topics,
        meta_categories=meta_categories,
        skip_critic=True,
        skip_fix=True,
        chunk_size=1,
        parallel_limit=30
    )

    print("-" * 40)
    print(f"Pipeline complete: {len(result.parsed_pages)} pages parsed")
    print()

    # Final validation
    print("Running final document validation...")
    original_pages = {i + 1: text for i, text in enumerate(chunks)}
    final_pages = {p.page: p for p in result.parsed_pages}

    validation = validate_document(final_pages, original_pages)

    print()
    print("=" * 60)
    print("VALIDATION RESULT")
    print("=" * 60)
    print(f"Valid: {validation.valid}")
    print(f"Content OK: {validation.content_ok}")
    print(f"Content Mismatch: {validation.content_mismatch}")
    print(f"Duplicates Found: {validation.duplicates_found}")
    print(f"Missing Pages: {len(validation.missing_pages)}")
    print(f"Extra Pages: {len(validation.extra_pages)}")

    if validation.missing_pages:
        print(f"  Missing: {validation.missing_pages[:20]}")
    if validation.extra_pages:
        print(f"  Extra: {validation.extra_pages[:20]}")

    print()
    if validation.issues:
        print(f"Issues ({len(validation.issues)} total):")
        for issue in validation.issues[:15]:
            print(f"  - {issue}")
        if len(validation.issues) > 15:
            print(f"  ... and {len(validation.issues) - 15} more")
    else:
        print("No issues found!")

    print()
    print("=" * 60)

    return result.parsed_pages, validation, doc


def save_results(doc_id: str, filename: str, pages_data: list, total_pages: int):
    """Save parsed results to content.json using DocumentData for proper summary."""
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

    # Convert to dict (includes properly built summary)
    content_json = doc_data.to_dict()

    json_path = doc_dir / "content.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(content_json, f, indent=2, ensure_ascii=False)

    print(f"Saved: {json_path}")

    # Also save content.txt with just the text content
    txt_path = doc_dir / "content.txt"
    with open(txt_path, 'w', encoding='utf-8') as f:
        for page in sorted(pages_data, key=lambda p: p.page):
            f.write(f"=== Page {page.page} ===\n")
            if page.content:
                f.write(page.content)
            elif page.skip_reason:
                f.write(f"[SKIPPED: {page.skip_reason}]")
            else:
                f.write("[NO CONTENT]")
            f.write("\n\n")

    print(f"Saved: {txt_path}")
    return json_path


if __name__ == "__main__":
    # Default: SGLT2 document (4 pages)
    doc_id = "cdec171862b3c13678e4a34f867e96f6f0e47b39f1d45f88b9c9a3f5b7e8d2a1"
    save = False

    # Parse args
    for arg in sys.argv[1:]:
        if arg == "--save":
            save = True
        else:
            doc_id = arg

    # Get full hash if partial provided
    if len(doc_id) < 64:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT file_hash FROM documents WHERE file_hash LIKE ?", (doc_id + "%",))
        row = cursor.fetchone()
        conn.close()
        if row:
            doc_id = row[0]

    pages_data, validation, doc = asyncio.run(test_document(doc_id))

    if save and pages_data:
        print()
        save_results(doc_id, doc["filename"], pages_data, doc["total_pages"])
