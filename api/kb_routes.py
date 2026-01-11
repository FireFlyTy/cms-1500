"""
Knowledge Base API - endpoints для управления документами и кодами.
"""

import os
import io
import re
import json
import hashlib
import asyncio
from typing import List, Optional, Dict
from datetime import datetime

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel
from pypdf import PdfReader, PdfWriter

from google import genai
from google.genai import types

from src.parsers.document_parser import (
    parse_chunk_response,
    merge_chunk_results,
    build_document_data,
    save_document_files,
    get_chunk_prompt,
    PageData,
    DocumentData
)
from src.db.connection import get_db_connection

# ============================================================
# CONFIG
# ============================================================

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_MODEL_NAME = os.getenv("GOOGLE_MODEL", "gemini-2.5-flash")

DOCUMENTS_DIR = os.getenv("DOCUMENTS_DIR", "data/processed/documents")
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "data/raw/documents")

CHUNK_SIZE = 15  # Pages per chunk
PARALLEL_LIMIT = 5  # Concurrent API calls

client = genai.Client(api_key=GOOGLE_API_KEY)

router = APIRouter(prefix="/api/kb", tags=["Knowledge Base"])


# ============================================================
# CODE NORMALIZATION
# ============================================================

def normalize_code_pattern(code: str) -> str:
    """
    Нормализует wildcard паттерны и диапазоны в кодах.

    Wildcards:
        E11.*  → E11.%
        E11.-  → E11.%
        E11.x  → E11.%

    Ranges:
        47531-47541 → 47531:47541
    """
    if not code:
        return code

    normalized = code.upper().strip()

    # Check for range pattern: CODE-CODE (but not wildcard like E11.-)
    range_match = re.match(r'^([A-Z0-9\.]+)-([A-Z0-9\.]+)$', normalized)
    if range_match:
        start, end = range_match.groups()
        # Make sure it's actually a range (not E11.- wildcard)
        if len(end) > 1 and not end.startswith('.'):
            return f"{start}:{end}"

    # Replace wildcard characters at the end
    if normalized.endswith('.*'):
        normalized = normalized[:-1] + '%'
    elif normalized.endswith('.-'):
        normalized = normalized[:-1] + '%'
    elif normalized.endswith('.X'):
        normalized = normalized[:-1] + '%'
    elif normalized.endswith('*'):
        normalized = normalized[:-1] + '%'

    return normalized


# ============================================================
# MODELS
# ============================================================

class DocumentMetadata(BaseModel):
    doc_type: Optional[str] = None
    doc_subtype: Optional[str] = None
    payer: Optional[str] = None
    categories: List[str] = []
    stages: List[str] = []
    notes: Optional[str] = None


class DocumentResponse(BaseModel):
    id: str
    filename: str
    filepath: str
    doc_type: Optional[str]
    doc_subtype: Optional[str]
    payer: Optional[str]
    total_pages: int
    content_pages: int
    parsed_at: Optional[str]
    analyzed_at: Optional[str]
    categories: List[str] = []
    codes: List[Dict] = []
    stages: List[str] = []


class CodeIndexItem(BaseModel):
    code: str
    type: str
    documents: List[Dict]


class ParseProgress(BaseModel):
    status: str
    message: str
    percent: int
    file_hash: Optional[str] = None


# ============================================================
# HELPERS
# ============================================================

def get_file_hash(file_bytes: bytes) -> str:
    return hashlib.sha256(file_bytes).hexdigest()


def get_doc_type_from_folder(folder: str) -> str:
    """
    Определяет doc_type по папке документа.

    Маппинг:
        guidelines/ → clinical_guideline
        policies/   → policy
        codebooks/  → codebook
        root/other  → unknown
    """
    if not folder:
        return 'unknown'

    folder_lower = folder.lower().strip('/')

    # Direct mapping
    mapping = {
        'guidelines': 'clinical_guideline',
        'guideline': 'clinical_guideline',
        'policies': 'policy',
        'policy': 'policy',
        'codebooks': 'codebook',
        'codebook': 'codebook',
        'reference': 'codebook',
    }

    # Check exact match first
    if folder_lower in mapping:
        return mapping[folder_lower]

    # Check if folder starts with known prefix
    for key, value in mapping.items():
        if folder_lower.startswith(key):
            return value

    return 'unknown'


async def process_pdf_chunk(
        chunk_bytes: bytes,
        chunk_index: int,
        start_page: int,
        pages_in_chunk: int,
        semaphore: asyncio.Semaphore,
        max_retries: int = 3
) -> List[PageData]:
    """Обрабатывает чанк PDF с метаданными"""

    async with semaphore:
        temp_path = f"/tmp/chunk_{chunk_index}_{int(datetime.now().timestamp())}.pdf"

        try:
            with open(temp_path, "wb") as f:
                f.write(chunk_bytes)

            for attempt in range(max_retries):
                try:
                    print(f"[CHUNK {chunk_index}] Processing pages {start_page}-{start_page + pages_in_chunk - 1}")

                    # Upload to Gemini
                    file_upload = client.files.upload(
                        file=temp_path,
                        config={'mime_type': 'application/pdf'}
                    )

                    # Wait for processing
                    wait_count = 0
                    while file_upload.state.name == "PROCESSING":
                        wait_count += 1
                        if wait_count > 60:
                            raise TimeoutError("Upload timeout")
                        await asyncio.sleep(1)
                        file_upload = client.files.get(name=file_upload.name)

                    if file_upload.state.name == "FAILED":
                        raise Exception("File processing FAILED")

                    # Generate with metadata prompt
                    prompt = get_chunk_prompt(pages_in_chunk, start_page)

                    response = await asyncio.wait_for(
                        client.aio.models.generate_content(
                            model=GOOGLE_MODEL_NAME,
                            contents=[types.Content(role="user", parts=[
                                types.Part.from_uri(file_uri=file_upload.uri, mime_type=file_upload.mime_type),
                                types.Part.from_text(text=prompt)
                            ])]
                        ),
                        timeout=120
                    )

                    # Parse response with metadata
                    pages = parse_chunk_response(response.text, start_page, pages_in_chunk)

                    print(f"[CHUNK {chunk_index}] ✓ Extracted {len([p for p in pages if p.content])} content pages")
                    return pages

                except Exception as e:
                    print(f"[CHUNK {chunk_index}] ⚠ Attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep((attempt + 1) * 10)
                    else:
                        # Return error page
                        return [PageData(
                            page=start_page,
                            page_type='empty',
                            skip_reason=f"Error: {str(e)[:100]}"
                        )]

            return []

        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)


async def parse_pdf_with_metadata(
        file_bytes: bytes,
        filename: str,
        file_hash: str,
        progress_callback=None
) -> DocumentData:
    """Парсит PDF и извлекает метаданные"""

    reader = PdfReader(io.BytesIO(file_bytes))
    total_pages = len(reader.pages)

    print(f"[PARSER] Starting: {filename} ({total_pages} pages)")

    semaphore = asyncio.Semaphore(PARALLEL_LIMIT)
    pages_completed = 0

    async def track_chunk(coro, chunk_size):
        nonlocal pages_completed
        result = await coro
        pages_completed += chunk_size
        if progress_callback:
            await progress_callback(
                status="parsing",
                pages_done=pages_completed,
                total_pages=total_pages
            )
        return result

    # Create chunk tasks
    tasks = []
    for i in range(0, total_pages, CHUNK_SIZE):
        writer = PdfWriter()
        chunk_pages = reader.pages[i:i + CHUNK_SIZE]
        for page in chunk_pages:
            writer.add_page(page)

        chunk_buffer = io.BytesIO()
        writer.write(chunk_buffer)

        task = process_pdf_chunk(
            chunk_buffer.getvalue(),
            i // CHUNK_SIZE,
            i + 1,  # 1-indexed
            len(chunk_pages),
            semaphore
        )
        tasks.append(track_chunk(task, len(chunk_pages)))

    # Execute all
    all_results = await asyncio.gather(*tasks)

    # Merge results
    all_pages = merge_chunk_results(all_results)

    # Build document
    doc = build_document_data(
        file_hash=file_hash,
        filename=filename,
        total_pages=total_pages,
        pages=all_pages
    )

    # Save files
    os.makedirs(DOCUMENTS_DIR, exist_ok=True)
    txt_path, json_path = save_document_files(doc, DOCUMENTS_DIR)

    print(f"[PARSER] ✓ Completed: {len(doc.summary['content_pages'])} content pages")

    return doc


def save_document_to_db(doc: DocumentData, filepath: str):
    """Сохраняет документ и метаданные в SQLite"""

    conn = get_db_connection()
    cursor = conn.cursor()

    # Determine doc_type from filepath (folder) - приоритет над page-based detection
    folder = os.path.dirname(filepath) if filepath else ''
    doc_type = get_doc_type_from_folder(folder)

    # Fallback to page-based detection if folder is unknown
    if doc_type == 'unknown':
        doc_type = doc.summary.get('doc_type', 'unknown')

    try:
        # Insert document
        cursor.execute("""
            INSERT OR REPLACE INTO documents 
            (file_hash, filename, source_path, doc_type, total_pages, parsed_at, content_path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            doc.file_hash,
            doc.filename,
            filepath,
            doc_type,
            doc.total_pages,
            doc.parsed_at,
            os.path.join(DOCUMENTS_DIR, doc.file_hash, 'content.json')
        ))

        # Insert codes (if table exists)
        try:
            # First, delete old codes for this document (for reparse case)
            cursor.execute("""
                DELETE FROM document_codes WHERE document_id = ?
            """, (doc.file_hash,))

            # Collect pages for each code
            code_pages = {}  # {(normalized_code, type): {'pages': set(), 'contexts': []}}

            for page in doc.pages:
                page_num = page.get('page', 0)
                for code_info in page.get('codes', []):
                    normalized_code = normalize_code_pattern(code_info.get('code', ''))
                    code_type = code_info.get('type', 'Unknown')
                    context = code_info.get('context', '')

                    key = (normalized_code, code_type)
                    if key not in code_pages:
                        code_pages[key] = {'pages': set(), 'contexts': []}

                    code_pages[key]['pages'].add(page_num)
                    if context and context not in code_pages[key]['contexts']:
                        code_pages[key]['contexts'].append(context)

            # Insert aggregated codes with page_numbers as JSON
            for (code, code_type), data in code_pages.items():
                pages_json = json.dumps(sorted(data['pages']))
                description = ', '.join(data['contexts'][:3])[:200]  # First 3 contexts

                cursor.execute("""
                    INSERT OR IGNORE INTO document_codes 
                    (document_id, code_pattern, code_type, description, page_numbers)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    doc.file_hash,
                    code,
                    code_type,
                    description,
                    pages_json
                ))
        except Exception as e:
            print(f"Warning: Could not insert codes: {e}")

        conn.commit()

    finally:
        conn.close()


def load_document_json(file_hash: str) -> Optional[Dict]:
    """Загружает JSON документа"""
    json_path = os.path.join(DOCUMENTS_DIR, file_hash, 'content.json')
    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


# ============================================================
# ENDPOINTS
# ============================================================

@router.get("/documents")
async def list_documents() -> List[DocumentResponse]:
    """Список всех документов"""

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT file_hash, filename, source_path, doc_type, doc_subtype, 
               NULL as payer, total_pages, parsed_at, analyzed_at, content_path
        FROM documents
        ORDER BY parsed_at DESC
    """)

    documents = []
    for row in cursor.fetchall():
        doc_id = row[0]

        # Get codes for this document (if table exists)
        codes = []
        try:
            cursor.execute("""
                SELECT DISTINCT code_pattern, code_type FROM document_codes
                WHERE document_id = ?
            """, (doc_id,))
            codes = [{'code': r[0], 'type': r[1]} for r in cursor.fetchall()]
        except:
            pass

        # Get categories (if table exists)
        categories = []
        try:
            cursor.execute("""
                SELECT c.name FROM categories c
                JOIN document_categories dc ON c.id = dc.category_id
                WHERE dc.document_id = ?
            """, (doc_id,))
            categories = [r[0] for r in cursor.fetchall()]
        except:
            pass

        # Get stages (if table exists)
        stages = []
        try:
            cursor.execute("""
                SELECT stage FROM document_stages
                WHERE document_id = ?
            """, (doc_id,))
            stages = [r[0] for r in cursor.fetchall()]
        except:
            pass

        # Load JSON for content_pages count
        doc_json = load_document_json(doc_id)
        content_pages = doc_json.get('summary', {}).get('content_page_count', 0) if doc_json else 0

        documents.append(DocumentResponse(
            id=doc_id,
            filename=row[1],
            filepath=row[2] or '',
            doc_type=row[3],
            doc_subtype=row[4],
            payer=row[5],
            total_pages=row[6] or 0,
            content_pages=content_pages,
            parsed_at=row[7],
            analyzed_at=row[8],
            categories=categories,
            codes=codes,
            stages=stages
        ))

    conn.close()
    return documents


@router.get("/documents/{doc_id}")
async def get_document(doc_id: str):
    """Получить документ с полными данными"""

    doc_json = load_document_json(doc_id)
    if not doc_json:
        raise HTTPException(status_code=404, detail="Document not found")

    return doc_json


@router.get("/documents/{doc_id}/text")
async def get_document_text(doc_id: str):
    """Получить текст документа"""

    txt_path = os.path.join(DOCUMENTS_DIR, doc_id, 'content.txt')
    if not os.path.exists(txt_path):
        raise HTTPException(status_code=404, detail="Text not found")

    with open(txt_path, 'r', encoding='utf-8') as f:
        content = f.read()

    return {"content": content}


@router.get("/documents/{doc_id}/pdf")
async def get_document_pdf(doc_id: str):
    """Получить PDF файл"""

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT source_path, filename FROM documents WHERE file_hash = ?", (doc_id,))
    row = cursor.fetchone()
    conn.close()

    if not row or not row[0]:
        raise HTTPException(status_code=404, detail="PDF not found")

    pdf_path = row[0]
    if not os.path.exists(pdf_path):
        pdf_path = os.path.join(UPLOAD_DIR, row[0])
    if not os.path.exists(pdf_path):
        raise HTTPException(status_code=404, detail="PDF file not found")

    filename = row[1] if row[1] else os.path.basename(pdf_path)

    return FileResponse(
        pdf_path,
        media_type="application/pdf",
        filename=filename,
        headers={
            "Accept-Ranges": "bytes",
            "Access-Control-Expose-Headers": "Content-Length, Accept-Ranges"
        }
    )


@router.post("/documents/{doc_id}/parse")
async def parse_document(doc_id: str, force: bool = False):
    """Парсит существующий документ из базы. force=True для перепарсинга."""

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT file_hash, filename, source_path FROM documents WHERE file_hash = ?", (doc_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Document not found")

    file_hash, filename, source_path = row[0], row[1], row[2]

    # Check if already parsed (skip if force=True)
    if not force:
        existing = load_document_json(file_hash)
        if existing:
            return {
                "status": "already_parsed",
                "file_hash": file_hash,
                "content_pages": existing.get('summary', {}).get('content_page_count', 0)
            }

    # Find PDF file
    pdf_path = source_path
    if not os.path.exists(pdf_path):
        pdf_path = os.path.join(UPLOAD_DIR, source_path)
    if not os.path.exists(pdf_path):
        raise HTTPException(status_code=404, detail=f"PDF file not found: {source_path}")

    # Read file
    with open(pdf_path, 'rb') as f:
        content = f.read()

    # Parse with metadata
    doc = await parse_pdf_with_metadata(content, filename, file_hash)

    # Update DB
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE documents 
        SET parsed_at = ?, content_path = ?, total_pages = ?
        WHERE file_hash = ?
    """, (
        doc.parsed_at,
        os.path.join(DOCUMENTS_DIR, file_hash, 'content.json'),
        doc.total_pages,
        file_hash
    ))
    conn.commit()
    conn.close()

    # Save codes
    save_document_to_db(doc, source_path)

    return {
        "status": "success",
        "file_hash": file_hash,
        "filename": filename,
        "total_pages": doc.total_pages,
        "content_pages": doc.summary['content_page_count'],
        "codes_found": len(doc.summary['all_codes'])
    }


@router.get("/documents/{doc_id}/parse-stream")
async def parse_document_stream(doc_id: str, force: bool = False):
    """Парсит документ с SSE стримингом прогресса"""

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT file_hash, filename, source_path, total_pages FROM documents WHERE file_hash = ?", (doc_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Document not found")

    file_hash, filename, source_path, total_pages = row[0], row[1], row[2], row[3] or 0

    # Check if already parsed (skip if force=True)
    if not force:
        existing = load_document_json(file_hash)
        if existing:
            async def already_parsed():
                yield f"data: {json.dumps({'status': 'already_parsed', 'file_hash': file_hash, 'percent': 100})}\n\n"

            return StreamingResponse(already_parsed(), media_type="text/event-stream")

    # Find PDF file
    pdf_path = source_path
    if not os.path.exists(pdf_path):
        pdf_path = os.path.join(UPLOAD_DIR, source_path)
    if not os.path.exists(pdf_path):
        raise HTTPException(status_code=404, detail=f"PDF file not found: {source_path}")

    # Read file
    with open(pdf_path, 'rb') as f:
        content = f.read()

    # Progress queue
    progress_queue = asyncio.Queue()

    async def progress_callback(status: str, pages_done: int, total_pages: int):
        await progress_queue.put({
            "status": status,
            "pages_done": pages_done,
            "total_pages": total_pages
        })

    async def parse_with_progress():
        # Send initial status
        yield f"data: {json.dumps({'status': 'starting', 'pages_done': 0, 'total_pages': total_pages})}\n\n"

        # Start parsing in background
        parse_task = asyncio.create_task(
            parse_pdf_with_metadata(content, filename, file_hash, progress_callback)
        )

        # Stream progress updates
        while not parse_task.done():
            try:
                progress = await asyncio.wait_for(progress_queue.get(), timeout=0.5)
                yield f"data: {json.dumps(progress)}\n\n"
            except asyncio.TimeoutError:
                continue

        # Get result
        try:
            doc = await parse_task

            # Update DB
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE documents 
                SET parsed_at = ?, content_path = ?, total_pages = ?
                WHERE file_hash = ?
            """, (
                doc.parsed_at,
                os.path.join(DOCUMENTS_DIR, file_hash, 'content.json'),
                doc.total_pages,
                file_hash
            ))
            conn.commit()
            conn.close()

            # Save codes
            save_document_to_db(doc, source_path)

            # Send completion
            yield f"data: {json.dumps({'status': 'complete', 'pages_done': doc.total_pages, 'total_pages': doc.total_pages, 'content_pages': doc.summary['content_page_count'], 'codes_found': len(doc.summary['all_codes'])})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'status': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(parse_with_progress(), media_type="text/event-stream")


@router.post("/documents/parse-all")
async def parse_all_documents():
    """Парсит все непарсенные документы"""

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT file_hash, filename, source_path FROM documents 
        WHERE parsed_at IS NULL OR content_path IS NULL
    """)
    unparsed = cursor.fetchall()
    conn.close()

    results = []
    for row in unparsed:
        file_hash, filename, source_path = row[0], row[1], row[2]

        try:
            # Find PDF file
            pdf_path = source_path
            if not os.path.exists(pdf_path):
                pdf_path = os.path.join(UPLOAD_DIR, source_path)

            if not os.path.exists(pdf_path):
                results.append({
                    "file_hash": file_hash,
                    "filename": filename,
                    "status": "error",
                    "error": "PDF not found"
                })
                continue

            # Read file
            with open(pdf_path, 'rb') as f:
                content = f.read()

            # Parse
            doc = await parse_pdf_with_metadata(content, filename, file_hash)

            # Update DB
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE documents 
                SET parsed_at = ?, content_path = ?, total_pages = ?
                WHERE file_hash = ?
            """, (
                doc.parsed_at,
                os.path.join(DOCUMENTS_DIR, file_hash, 'content.json'),
                doc.total_pages,
                file_hash
            ))
            conn.commit()
            conn.close()

            # Save codes
            save_document_to_db(doc, source_path)

            results.append({
                "file_hash": file_hash,
                "filename": filename,
                "status": "success",
                "content_pages": doc.summary['content_page_count'],
                "codes_found": len(doc.summary['all_codes'])
            })

        except Exception as e:
            results.append({
                "file_hash": file_hash,
                "filename": filename,
                "status": "error",
                "error": str(e)
            })

    return {
        "total": len(unparsed),
        "parsed": len([r for r in results if r['status'] == 'success']),
        "errors": len([r for r in results if r['status'] == 'error']),
        "results": results
    }


@router.post("/documents/upload")
async def upload_document(
        file: UploadFile = File(...),
        folder: str = Form("guidelines")
):
    """Загрузить и распарсить документ"""

    content = await file.read()
    file_hash = get_file_hash(content)

    # Check if already exists
    existing = load_document_json(file_hash)
    if existing:
        return {
            "status": "exists",
            "file_hash": file_hash,
            "message": "Document already parsed"
        }

    # Save original PDF
    upload_path = os.path.join(UPLOAD_DIR, folder)
    os.makedirs(upload_path, exist_ok=True)
    pdf_path = os.path.join(upload_path, file.filename)
    with open(pdf_path, 'wb') as f:
        f.write(content)

    # Parse with metadata
    doc = await parse_pdf_with_metadata(
        content,
        file.filename,
        file_hash
    )

    # Save to DB
    save_document_to_db(doc, os.path.join(folder, file.filename))

    return {
        "status": "success",
        "file_hash": file_hash,
        "filename": file.filename,
        "total_pages": doc.total_pages,
        "content_pages": doc.summary['content_page_count'],
        "codes_found": len(doc.summary['all_codes'])
    }


@router.patch("/documents/{doc_id}/metadata")
async def update_document_metadata(doc_id: str, metadata: DocumentMetadata):
    """Обновить метаданные документа"""

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Update document
        cursor.execute("""
            UPDATE documents 
            SET doc_type = COALESCE(?, doc_type),
                doc_subtype = COALESCE(?, doc_subtype),
                analyzed_at = ?
            WHERE file_hash = ?
        """, (
            metadata.doc_type,
            metadata.doc_subtype,
            datetime.utcnow().isoformat() + 'Z',
            doc_id
        ))

        # Update categories (if table exists)
        try:
            cursor.execute("DELETE FROM document_categories WHERE document_id = ?", (doc_id,))
            for cat_name in metadata.categories:
                cursor.execute("SELECT id FROM categories WHERE name = ?", (cat_name,))
                row = cursor.fetchone()
                if row:
                    cursor.execute("""
                        INSERT INTO document_categories (document_id, category_id)
                        VALUES (?, ?)
                    """, (doc_id, row[0]))
        except:
            pass

        # Update stages (if table exists)
        try:
            cursor.execute("DELETE FROM document_stages WHERE document_id = ?", (doc_id,))
            for stage in metadata.stages:
                cursor.execute("""
                    INSERT INTO document_stages (document_id, stage)
                    VALUES (?, ?)
                """, (doc_id, stage))
        except:
            pass

        conn.commit()

    finally:
        conn.close()

    return {"status": "updated"}


@router.get("/codes")
async def list_codes() -> List[CodeIndexItem]:
    """Индекс всех кодов - быстрая версия без загрузки страниц"""

    conn = get_db_connection()
    cursor = conn.cursor()

    # Get all codes with document count in one query
    try:
        cursor.execute("""
            SELECT 
                dc.code_pattern, 
                dc.code_type,
                COUNT(DISTINCT dc.document_id) as doc_count
            FROM document_codes dc
            GROUP BY dc.code_pattern, dc.code_type
            ORDER BY dc.code_type, dc.code_pattern
        """)
    except:
        conn.close()
        return []

    # Use dict to merge normalized duplicates (E11.* + E11.% → E11.%)
    codes_map = {}

    for row in cursor.fetchall():
        code, code_type, doc_count = row[0], row[1], row[2]

        # Normalize the code
        normalized = normalize_code_pattern(code)
        key = (normalized, code_type or 'Unknown')

        if key in codes_map:
            # Merge counts
            codes_map[key]['count'] += doc_count
        else:
            codes_map[key] = {
                'code': normalized,
                'type': code_type or 'Unknown',
                'count': doc_count
            }

    conn.close()

    # Convert to list
    codes = []
    for key, data in sorted(codes_map.items(), key=lambda x: (x[0][1], x[0][0])):
        codes.append(CodeIndexItem(
            code=data['code'],
            type=data['type'],
            documents=[{'count': data['count']}]
        ))

    return codes


@router.get("/codes/{code}")
async def get_code_details(code: str):
    """Детали по конкретному коду"""

    # Normalize the requested code
    normalized_code = normalize_code_pattern(code)

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Search for both original and normalized code patterns, include page_numbers
        cursor.execute("""
            SELECT DISTINCT d.file_hash, d.filename, d.doc_type, dc.code_type, dc.description, dc.page_numbers
            FROM documents d
            JOIN document_codes dc ON d.file_hash = dc.document_id
            WHERE dc.code_pattern = ? OR dc.code_pattern = ?
        """, (code, normalized_code))
    except:
        conn.close()
        raise HTTPException(status_code=404, detail="Code not found")

    rows = cursor.fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="Code not found")

    # Group by document - use dict to deduplicate
    documents_map = {}
    code_type = None

    for row in rows:
        doc_id, filename, doc_type, c_type, desc, page_numbers_json = row
        code_type = c_type

        # Skip if already processed
        if doc_id in documents_map:
            continue

        pages_info = []

        # Try to use page_numbers from database first
        if page_numbers_json:
            try:
                page_nums = json.loads(page_numbers_json)
                # Get content preview from JSON only for display
                doc_json = load_document_json(doc_id)
                page_content_map = {}
                if doc_json:
                    for page in doc_json.get('pages', []):
                        page_content_map[page.get('page')] = {
                            'content': page.get('content', '')[:200] if page.get('content') else None,
                            'topics': page.get('topics', [])
                        }

                for page_num in page_nums:
                    page_data = page_content_map.get(page_num, {})
                    pages_info.append({
                        'page': page_num,
                        'context': desc,
                        'topics': page_data.get('topics', []),
                        'content_preview': page_data.get('content')
                    })
            except json.JSONDecodeError:
                pass

        # Fallback: get pages from JSON if page_numbers not in DB
        if not pages_info:
            doc_json = load_document_json(doc_id)
            seen_pages = set()

            if doc_json:
                for page in doc_json.get('pages', []):
                    page_num = page.get('page')
                    if page_num in seen_pages:
                        continue

                    for page_code in page.get('codes', []):
                        page_code_normalized = normalize_code_pattern(page_code.get('code', ''))
                        if page_code_normalized == normalized_code or page_code.get('code') == code:
                            seen_pages.add(page_num)
                            pages_info.append({
                                'page': page_num,
                                'context': page_code.get('context'),
                                'topics': page.get('topics', []),
                                'content_preview': page.get('content', '')[:200] if page.get('content') else None
                            })
                            break

        # Add document if it has pages
        if pages_info:
            documents_map[doc_id] = {
                'id': doc_id,
                'filename': filename,
                'doc_type': doc_type,
                'pages': pages_info
            }

    return {
        'code': normalized_code,
        'type': code_type,
        'documents': list(documents_map.values())
    }


@router.get("/categories")
async def list_categories():
    """Список всех категорий"""

    conn = get_db_connection()
    cursor = conn.cursor()

    categories = {
        'medical': [],
        'service': []
    }

    try:
        cursor.execute("""
            SELECT id, category_type, name, description FROM categories
            ORDER BY category_type, name
        """)

        for row in cursor.fetchall():
            cat_type = row[1]
            if cat_type in categories:
                categories[cat_type].append({
                    'id': row[0],
                    'name': row[2],
                    'description': row[3]
                })
    except:
        pass

    conn.close()
    return categories


@router.get("/stats")
async def get_stats():
    """Статистика Knowledge Base"""

    conn = get_db_connection()
    cursor = conn.cursor()

    # Documents count
    try:
        cursor.execute("SELECT COUNT(*) FROM documents")
        docs_count = cursor.fetchone()[0]
    except:
        docs_count = 0

    # Codes count
    try:
        cursor.execute("SELECT COUNT(DISTINCT code_pattern) FROM document_codes")
        codes_count = cursor.fetchone()[0]
    except:
        codes_count = 0

    # Reference data
    ref_stats = []
    for table in ['hcpcs', 'ncci_ptp', 'ncci_mue_pra', 'ncci_mue_dme', 'icd10']:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            ref_stats.append({'table': table, 'records': count})
        except:
            pass

    conn.close()

    return {
        'documents': docs_count,
        'codes_indexed': codes_count,
        'reference_data': ref_stats
    }


@router.post("/migrate-codes")
async def migrate_codes():
    """
    Миграция:
    1. Добавляет колонку page_numbers если её нет
    2. Нормализует wildcards (E11.* → E11.%)
    3. Заполняет page_numbers из JSON файлов
    4. Удаляет дубликаты
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    results = {
        'column_added': False,
        'patterns_normalized': 0,
        'rows_updated': 0,
        'duplicates_removed': 0,
        'pages_populated': 0,
        'total_unique_codes': 0
    }

    # 1. Add page_numbers column if not exists
    try:
        cursor.execute("ALTER TABLE document_codes ADD COLUMN page_numbers TEXT")
        conn.commit()
        results['column_added'] = True
    except Exception as e:
        # Column already exists
        pass

    # 2. Normalize wildcards
    cursor.execute("""
        SELECT DISTINCT code_pattern 
        FROM document_codes 
        WHERE code_pattern LIKE '%.*' 
           OR code_pattern LIKE '%.-'
           OR code_pattern LIKE '%.x'
           OR code_pattern LIKE '%.X'
    """)

    codes_to_fix = cursor.fetchall()
    updates = []

    for (old_code,) in codes_to_fix:
        new_code = normalize_code_pattern(old_code)
        if new_code != old_code:
            updates.append((old_code, new_code))

    for old_code, new_code in updates:
        cursor.execute("""
            UPDATE document_codes 
            SET code_pattern = ? 
            WHERE code_pattern = ?
        """, (new_code, old_code))
        results['rows_updated'] += cursor.rowcount

    results['patterns_normalized'] = len(updates)

    # 3. Remove duplicates (keep first occurrence)
    cursor.execute("""
        DELETE FROM document_codes 
        WHERE rowid NOT IN (
            SELECT MIN(rowid) 
            FROM document_codes 
            GROUP BY document_id, code_pattern
        )
    """)
    results['duplicates_removed'] = cursor.rowcount

    conn.commit()

    # 4. Populate page_numbers from JSON files (for records where it's NULL)
    cursor.execute("""
        SELECT DISTINCT document_id FROM document_codes 
        WHERE page_numbers IS NULL
    """)
    docs_to_update = [row[0] for row in cursor.fetchall()]

    for doc_id in docs_to_update:
        doc_json = load_document_json(doc_id)
        if not doc_json:
            continue

        # Collect pages for each code
        code_pages = {}
        for page in doc_json.get('pages', []):
            page_num = page.get('page', 0)
            for code_info in page.get('codes', []):
                code = normalize_code_pattern(code_info.get('code', ''))
                if code not in code_pages:
                    code_pages[code] = set()
                code_pages[code].add(page_num)

        # Update records
        for code, pages in code_pages.items():
            pages_json = json.dumps(sorted(pages))
            cursor.execute("""
                UPDATE document_codes 
                SET page_numbers = ?
                WHERE document_id = ? AND code_pattern = ?
            """, (pages_json, doc_id, code))
            results['pages_populated'] += cursor.rowcount

    conn.commit()

    # Get final count
    cursor.execute("SELECT COUNT(DISTINCT code_pattern) FROM document_codes")
    results['total_unique_codes'] = cursor.fetchone()[0]

    conn.close()

    return results


@router.get("/scan")
async def scan_existing_files():
    """Сканирует существующие PDF файлы и добавляет их в базу (без парсинга)"""

    found_files = []

    # Рекурсивно сканируем все PDF файлы
    for root, dirs, files in os.walk(UPLOAD_DIR):
        for filename in files:
            if not filename.lower().endswith('.pdf'):
                continue

            filepath = os.path.join(root, filename)
            relative_path = os.path.relpath(filepath, UPLOAD_DIR)
            folder = os.path.dirname(relative_path) or 'root'

            # Determine doc_type from folder
            doc_type = get_doc_type_from_folder(folder)

            # Calculate hash
            with open(filepath, 'rb') as f:
                file_hash = hashlib.sha256(f.read()).hexdigest()

            # Get page count
            try:
                reader = PdfReader(filepath)
                total_pages = len(reader.pages)
            except:
                total_pages = 0

            found_files.append({
                'file_hash': file_hash,
                'filename': filename,
                'filepath': filepath,  # Полный путь для чтения файла
                'relative_path': relative_path,  # Относительный путь для UI
                'folder': folder,
                'doc_type': doc_type,
                'total_pages': total_pages
            })

    # Insert into database
    conn = get_db_connection()
    cursor = conn.cursor()

    added = 0
    updated = 0
    for f in found_files:
        try:
            # Проверяем существует ли запись
            cursor.execute("SELECT source_path, doc_type FROM documents WHERE file_hash = ?", (f['file_hash'],))
            existing = cursor.fetchone()

            if existing:
                # Обновляем путь и doc_type если изменились
                cursor.execute("""
                    UPDATE documents 
                    SET source_path = ?, filename = ?, doc_type = ?
                    WHERE file_hash = ?
                """, (f['relative_path'], f['filename'], f['doc_type'], f['file_hash']))
                updated += 1
            else:
                # Добавляем новую запись с doc_type
                cursor.execute("""
                    INSERT INTO documents 
                    (file_hash, filename, source_path, doc_type, total_pages)
                    VALUES (?, ?, ?, ?, ?)
                """, (f['file_hash'], f['filename'], f['relative_path'], f['doc_type'], f['total_pages']))
                added += 1
        except Exception as e:
            print(f"Error processing {f['filename']}: {e}")

    conn.commit()
    conn.close()

    return {
        'scanned': len(found_files),
        'added': added,
        'updated': updated,
        'files': found_files
    }