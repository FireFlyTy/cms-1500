"""
Context Builder for Multi-Document Rule Generation Pipeline

Собирает и форматирует source documents из Knowledge Base для промптов.

Output format для промптов:
    === SOURCE: icd10_codebook.pdf [doc_id: abc12345] ===
    ## Page 45
    content...
    
    ## Page 46
    content...
    
    === SOURCE: cms_guidelines.pdf [doc_id: def67890] ===
    ## Page 95
    content...

doc_id = первые 8 символов file_hash (SHA256)
"""

import os
import json
import hashlib
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

# Paths - will be configured from main app
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOCUMENTS_DIR = os.path.join(BASE_DIR, "data", "processed", "documents")


@dataclass
class SourceDocument:
    """Информация о source документе."""
    doc_id: str           # First 8 chars of file_hash
    file_hash: str        # Full SHA256 hash
    filename: str         # Original filename
    document_id: str      # DB document ID
    pages: List[int]      # Page numbers included
    

@dataclass
class SourcesContext:
    """Результат сборки контекста."""
    sources_text: str                    # Форматированный текст для промпта
    source_documents: List[SourceDocument]  # Метаданные документов
    total_pages: int                     # Общее количество страниц
    code_description: str = ""           # Описание кода из document_codes
    

def get_doc_id(file_hash: str) -> str:
    """
    Генерирует doc_id из file_hash.
    doc_id = первые 8 символов SHA256 хэша.
    """
    return file_hash[:8]


def build_sources_context(
    code: str,
    document_ids: Optional[List[str]] = None,
    db_connection=None,
    expand_pages: int = 0
) -> SourcesContext:
    """
    Собирает source documents для кода из Knowledge Base.
    
    Args:
        code: ICD-10 код (e.g., "E11.9")
        document_ids: Опционально - конкретные document IDs. 
                      Если None, берёт все документы где встречается код.
        db_connection: SQLite connection (если None, создаёт новый)
        expand_pages: Сколько страниц добавить до/после найденных (для контекста)
        
    Returns:
        SourcesContext с форматированным текстом и метаданными
    """
    # Import here to avoid circular imports
    from src.db.connection import get_db_connection
    
    conn = db_connection or get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get description for the code
        cursor.execute("""
            SELECT description FROM document_codes 
            WHERE code_pattern = ? AND description IS NOT NULL AND description != ''
            LIMIT 1
        """, (code,))
        desc_row = cursor.fetchone()
        code_description = desc_row[0] if desc_row else ""
        
        # Get document IDs, pages, and file info for this code
        # NOTE: document_codes.document_id = documents.file_hash (NOT documents.id)
        if document_ids:
            placeholders = ','.join('?' * len(document_ids))
            cursor.execute(f"""
                SELECT DISTINCT 
                    dc.document_id, 
                    d.file_hash,
                    d.filename
                FROM document_codes dc
                JOIN documents d ON dc.document_id = d.file_hash
                WHERE dc.code_pattern = ? AND dc.document_id IN ({placeholders})
            """, [code] + document_ids)
        else:
            cursor.execute("""
                SELECT DISTINCT 
                    dc.document_id, 
                    d.file_hash,
                    d.filename
                FROM document_codes dc
                JOIN documents d ON dc.document_id = d.file_hash
                WHERE dc.code_pattern = ?
            """, (code,))
        
        rows = cursor.fetchall()
        
        if not rows:
            return SourcesContext(
                sources_text="",
                source_documents=[],
                total_pages=0,
                code_description=code_description
            )
        
        # Collect unique documents
        # Row format: (document_id, file_hash, filename)
        docs_data: Dict[str, Dict] = {}
        for document_id, file_hash, filename in rows:
            if file_hash not in docs_data:
                docs_data[file_hash] = {
                    'file_hash': file_hash,
                    'filename': filename,
                    'document_id': document_id
                }
        
        # Load content and find pages with the code
        source_documents = []
        formatted_parts = []
        total_pages = 0
        
        for file_hash, data in docs_data.items():
            filename = data['filename']
            
            # Load content.json
            content_path = os.path.join(DOCUMENTS_DIR, file_hash, 'content.json')
            if not os.path.exists(content_path):
                continue
            
            with open(content_path, 'r', encoding='utf-8') as f:
                doc_content = json.load(f)
            
            # Find pages where this code appears
            pages_with_code = set()
            for page_data in doc_content.get('pages', []):
                page_codes = page_data.get('codes', [])
                for code_info in page_codes:
                    if code_info.get('code') == code:
                        pages_with_code.add(page_data['page'])
                        break
            
            # If no specific pages found, check summary
            if not pages_with_code:
                # Fallback: include all pages (document matched but no page-level info)
                pages_with_code = {p['page'] for p in doc_content.get('pages', []) if p.get('content')}
            
            # Expand pages if requested
            if expand_pages > 0:
                expanded = set()
                for page in pages_with_code:
                    for p in range(page - expand_pages, page + expand_pages + 1):
                        if p > 0:
                            expanded.add(p)
                pages_with_code = expanded
            
            # Generate doc_id from file_hash
            doc_id = get_doc_id(file_hash)
            
            # Build document header
            doc_header = f"=== SOURCE: {filename} [doc_id: {doc_id}] ==="
            page_contents = []
            actual_pages = []
            
            # Extract pages content
            pages_map = {p['page']: p['content'] for p in doc_content.get('pages', [])}
            
            for page_num in sorted(pages_with_code):
                if page_num in pages_map and pages_map[page_num]:
                    page_contents.append(f"## Page {page_num}\n{pages_map[page_num]}")
                    actual_pages.append(page_num)
            
            if page_contents:
                formatted_parts.append(doc_header + "\n\n" + "\n\n".join(page_contents))
                
                source_documents.append(SourceDocument(
                    doc_id=doc_id,
                    file_hash=file_hash,
                    filename=filename,
                    document_id=data['document_id'],
                    pages=actual_pages
                ))
                total_pages += len(actual_pages)
        
        sources_text = "\n\n".join(formatted_parts)
        
        return SourcesContext(
            sources_text=sources_text,
            source_documents=source_documents,
            total_pages=total_pages,
            code_description=code_description
        )
        
    finally:
        if db_connection is None:
            conn.close()


def format_sources_for_prompt(sources_context: SourcesContext) -> str:
    """
    Возвращает форматированный текст sources для вставки в промпт.
    Алиас для sources_context.sources_text.
    """
    return sources_context.sources_text


def build_sources_from_raw_pages(
    pages_data: List[Dict],
    filename: str = "document.pdf",
    file_hash: Optional[str] = None
) -> SourcesContext:
    """
    Создаёт SourcesContext из raw page data (для тестов или single-doc случаев).
    
    Args:
        pages_data: List of {"page": N, "content": "..."} 
        filename: Имя файла
        file_hash: SHA256 хэш (если None, генерируется из content)
        
    Returns:
        SourcesContext
    """
    if not pages_data:
        return SourcesContext(
            sources_text="",
            source_documents=[],
            total_pages=0
        )
    
    # Generate hash if not provided
    if file_hash is None:
        content_str = json.dumps(pages_data, sort_keys=True)
        file_hash = hashlib.sha256(content_str.encode()).hexdigest()
    
    doc_id = get_doc_id(file_hash)
    
    # Format content
    doc_header = f"=== SOURCE: {filename} [doc_id: {doc_id}] ==="
    page_contents = []
    actual_pages = []
    
    for page_data in sorted(pages_data, key=lambda x: x.get('page', 0)):
        page_num = page_data.get('page')
        content = page_data.get('content', '')
        if page_num and content:
            page_contents.append(f"## Page {page_num}\n{content}")
            actual_pages.append(page_num)
    
    sources_text = doc_header + "\n\n" + "\n\n".join(page_contents)
    
    return SourcesContext(
        sources_text=sources_text,
        source_documents=[
            SourceDocument(
                doc_id=doc_id,
                file_hash=file_hash,
                filename=filename,
                document_id="raw",
                pages=actual_pages
            )
        ],
        total_pages=len(actual_pages)
    )


def merge_sources_contexts(*contexts: SourcesContext) -> SourcesContext:
    """
    Объединяет несколько SourcesContext в один.
    Полезно когда нужно добавить документы из разных источников.
    """
    all_texts = []
    all_docs = []
    total = 0
    
    for ctx in contexts:
        if ctx.sources_text:
            all_texts.append(ctx.sources_text)
        all_docs.extend(ctx.source_documents)
        total += ctx.total_pages
    
    return SourcesContext(
        sources_text="\n\n".join(all_texts),
        source_documents=all_docs,
        total_pages=total
    )


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def get_available_documents_for_code(code: str, db_connection=None) -> List[Dict]:
    """
    Получает список всех документов, содержащих данный код.
    
    Returns:
        List of {"document_id": str, "filename": str, "doc_id": str, "file_hash": str}
    """
    from src.db.connection import get_db_connection
    
    conn = db_connection or get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT DISTINCT
                dc.document_id,
                d.filename,
                d.file_hash
            FROM document_codes dc
            JOIN documents d ON dc.document_id = d.file_hash
            WHERE dc.code_pattern = ?
        """, (code,))
        
        rows = cursor.fetchall()
        
        result = []
        for document_id, filename, file_hash in rows:
            # Load content.json to get pages with this code
            content_path = os.path.join(DOCUMENTS_DIR, file_hash, 'content.json')
            pages = []
            
            if os.path.exists(content_path):
                with open(content_path, 'r', encoding='utf-8') as f:
                    doc_content = json.load(f)
                
                for page_data in doc_content.get('pages', []):
                    for code_info in page_data.get('codes', []):
                        if code_info.get('code') == code:
                            pages.append(page_data['page'])
                            break
            
            result.append({
                'document_id': document_id,
                'filename': filename,
                'doc_id': get_doc_id(file_hash),
                'file_hash': file_hash,
                'pages': sorted(pages)
            })
        
        return result
        
        return result
        
    finally:
        if db_connection is None:
            conn.close()


def validate_doc_ids_exist(doc_ids: List[str], sources_context: SourcesContext) -> List[str]:
    """
    Проверяет что все doc_ids присутствуют в контексте.
    
    Returns:
        List of missing doc_ids
    """
    available = {doc.doc_id for doc in sources_context.source_documents}
    return [did for did in doc_ids if did not in available]
