"""
Knowledge Base API - endpoints для управления документами и кодами.
"""

import os
import io
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
            doc.summary.get('doc_type'),
            doc.total_pages,
            doc.parsed_at,
            os.path.join(DOCUMENTS_DIR, doc.file_hash, 'content.json')
        ))

        # Insert codes (if table exists)
        try:
            for code_info in doc.summary.get('all_codes', []):
                cursor.execute("""
                    INSERT OR IGNORE INTO document_codes 
                    (document_id, code_pattern, code_type, description)
                    VALUES (?, ?, ?, ?)
                """, (
                    doc.file_hash,
                    code_info['code'],
                    code_info['type'],
                    ', '.join(code_info.get('contexts', []))[:200]
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
    """Индекс всех кодов"""

    conn = get_db_connection()
    cursor = conn.cursor()

    # Get all unique codes
    try:
        cursor.execute("""
            SELECT DISTINCT code_pattern, code_type FROM document_codes
            ORDER BY code_type, code_pattern
        """)
    except:
        conn.close()
        return []

    codes = []
    for row in cursor.fetchall():
        code, code_type = row[0], row[1]

        # Get documents for this code
        cursor.execute("""
            SELECT d.file_hash, d.filename, d.doc_type, dc.description
            FROM documents d
            JOIN document_codes dc ON d.file_hash = dc.document_id
            WHERE dc.code_pattern = ?
        """, (code,))

        documents = []
        for doc_row in cursor.fetchall():
            # Get pages from JSON
            doc_json = load_document_json(doc_row[0])
            pages = []
            if doc_json:
                for page in doc_json.get('pages', []):
                    for page_code in page.get('codes', []):
                        if page_code.get('code') == code:
                            pages.append(page['page'])

            documents.append({
                'id': doc_row[0],
                'filename': doc_row[1],
                'doc_type': doc_row[2],
                'pages': sorted(set(pages)),
                'context': doc_row[3]
            })

        codes.append(CodeIndexItem(
            code=code,
            type=code_type or 'Unknown',
            documents=documents
        ))

    conn.close()
    return codes


@router.get("/codes/{code}")
async def get_code_details(code: str):
    """Детали по конкретному коду"""

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT d.file_hash, d.filename, d.doc_type, dc.code_type, dc.description
            FROM documents d
            JOIN document_codes dc ON d.file_hash = dc.document_id
            WHERE dc.code_pattern = ?
        """, (code,))
    except:
        conn.close()
        raise HTTPException(status_code=404, detail="Code not found")

    documents = []
    code_type = None

    for row in cursor.fetchall():
        doc_id, filename, doc_type, c_type, desc = row
        code_type = c_type

        # Get detailed page info from JSON
        doc_json = load_document_json(doc_id)
        pages_info = []

        if doc_json:
            for page in doc_json.get('pages', []):
                for page_code in page.get('codes', []):
                    if page_code.get('code') == code:
                        pages_info.append({
                            'page': page['page'],
                            'context': page_code.get('context'),
                            'topics': page.get('topics', []),
                            'content_preview': page.get('content', '')[:200] if page.get('content') else None
                        })

        documents.append({
            'id': doc_id,
            'filename': filename,
            'doc_type': doc_type,
            'pages': pages_info
        })

    conn.close()

    if not documents:
        raise HTTPException(status_code=404, detail="Code not found")

    return {
        'code': code,
        'type': code_type,
        'documents': documents
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
            cursor.execute("SELECT source_path FROM documents WHERE file_hash = ?", (f['file_hash'],))
            existing = cursor.fetchone()

            if existing:
                # Обновляем путь если изменился
                if existing[0] != f['relative_path']:
                    cursor.execute("""
                        UPDATE documents 
                        SET source_path = ?, filename = ?
                        WHERE file_hash = ?
                    """, (f['relative_path'], f['filename'], f['file_hash']))
                    updated += 1
            else:
                # Добавляем новую запись
                cursor.execute("""
                    INSERT INTO documents 
                    (file_hash, filename, source_path, total_pages)
                    VALUES (?, ?, ?, ?)
                """, (f['file_hash'], f['filename'], f['relative_path'], f['total_pages']))
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