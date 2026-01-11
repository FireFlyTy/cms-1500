"""
Rule Generation API Routes

Endpoints для генерации правил валидации кодов.
"""

import os
import json
import asyncio
from typing import List, Dict, Optional
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.utils.code_categories import get_code_category, group_codes_by_category
from src.db.connection import get_db_connection

# NEW: Import generators
from src.generators import (
    build_sources_context,
    RuleGenerator,
)

router = APIRouter(prefix="/api/rules", tags=["rules"])

# Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RULES_DIR = os.path.join(BASE_DIR, "data", "rules")
DOCUMENTS_DIR = os.path.join(BASE_DIR, "data", "processed", "documents")

os.makedirs(RULES_DIR, exist_ok=True)


# ============================================================
# MODELS
# ============================================================

class GenerateRuleRequest(BaseModel):
    code: str = ""
    code_type: str = "ICD-10"
    document_ids: Optional[List[str]] = None  # If None, use all relevant docs
    parallel_validators: bool = True  # Run Mentor || RedTeam in parallel
    thinking_budget: int = 10000


class BatchGenerateRequest(BaseModel):
    codes: List[str]
    code_type: str = "ICD-10"


# ============================================================
# HELPERS
# ============================================================

def get_all_codes_from_db() -> List[Dict]:
    """Получает все коды из базы данных с информацией о документах."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            dc.code_pattern,
            dc.code_type,
            dc.document_id,
            d.filename
        FROM document_codes dc
        JOIN documents d ON dc.document_id = d.file_hash
        WHERE d.parsed_at IS NOT NULL
        ORDER BY dc.code_pattern
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    # Aggregate by code
    codes_map = {}
    for row in rows:
        code = row[0]
        if code not in codes_map:
            codes_map[code] = {
                'code': code,
                'type': row[1],
                'documents': [],
                'total_pages': 0
            }
        
        codes_map[code]['documents'].append({
            'id': row[2],
            'filename': row[3]
        })
    
    return list(codes_map.values())


def get_rule_status(code: str) -> Dict:
    """Проверяет статус правила для кода."""
    # Check new versioned structure first
    code_dir = code.replace(".", "_").replace("/", "_")
    versioned_path = os.path.join(RULES_DIR, code_dir)
    
    if os.path.exists(versioned_path):
        # Find latest version
        versions = [d for d in os.listdir(versioned_path) if d.startswith('v')]
        if versions:
            latest = sorted(versions, key=lambda x: int(x[1:]))[-1]
            rule_path = os.path.join(versioned_path, latest, "rule.json")
            if os.path.exists(rule_path):
                with open(rule_path, 'r') as f:
                    rule = json.load(f)
                return {
                    'has_rule': True,
                    'version': rule.get('version', 1),
                    'created_at': rule.get('created_at'),
                    'validation_status': rule.get('validation_status', 'unknown'),
                    'path': rule_path
                }
    
    # Fallback to old flat structure
    rule_path = os.path.join(RULES_DIR, f"{code.replace('.', '_')}.json")
    
    if os.path.exists(rule_path):
        with open(rule_path, 'r') as f:
            rule = json.load(f)
        return {
            'has_rule': True,
            'version': rule.get('version', '1.0'),
            'created_at': rule.get('created_at'),
            'updated_at': rule.get('updated_at')
        }
    
    return {'has_rule': False}


def get_guideline_text_for_code(code: str, document_ids: Optional[List[str]] = None) -> str:
    """
    Собирает текст гайдлайнов для кода из всех релевантных документов.
    DEPRECATED: Use build_sources_context() instead for multi-doc support.
    """
    sources_ctx = build_sources_context(code, document_ids)
    return sources_ctx.sources_text


# ============================================================
# ENDPOINTS
# ============================================================

@router.get("/categories")
async def get_categories():
    """
    Получает список категорий с количеством кодов и статусом покрытия.
    """
    all_codes = get_all_codes_from_db()
    grouped = group_codes_by_category(all_codes)
    
    categories = []
    for category_name, codes in grouped.items():
        # Count rules
        codes_with_rules = sum(1 for c in codes if get_rule_status(c['code'])['has_rule'])
        
        # Get icon from first code
        icon = codes[0]['category_info']['icon'] if codes else '📄'
        
        categories.append({
            'name': category_name,
            'icon': icon,
            'total_codes': len(codes),
            'codes_with_rules': codes_with_rules,
            'coverage_percent': round(codes_with_rules / len(codes) * 100) if codes else 0
        })
    
    # Sort by name
    categories.sort(key=lambda x: x['name'])
    
    return {
        'categories': categories,
        'total_codes': len(all_codes),
        'total_with_rules': sum(c['codes_with_rules'] for c in categories)
    }


@router.get("/categories/{category_name}/codes")
async def get_codes_by_category(category_name: str):
    """
    Получает коды в категории с информацией о документах и статусе правил.
    """
    all_codes = get_all_codes_from_db()
    grouped = group_codes_by_category(all_codes)
    
    if category_name not in grouped:
        raise HTTPException(status_code=404, detail=f"Category '{category_name}' not found")
    
    codes = grouped[category_name]
    
    # Enrich with rule status
    result = []
    for code_info in codes:
        rule_status = get_rule_status(code_info['code'])
        result.append({
            **code_info,
            'rule_status': rule_status
        })
    
    # Sort by code
    result.sort(key=lambda x: x['code'])
    
    return {
        'category': category_name,
        'codes': result,
        'total': len(result),
        'with_rules': sum(1 for c in result if c['rule_status']['has_rule'])
    }


@router.get("/codes/{code}")
async def get_code_details(code: str):
    """
    Получает детальную информацию о коде.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT
            dc.code_pattern,
            dc.code_type,
            dc.description,
            dc.document_id,
            d.filename,
            d.file_hash
        FROM document_codes dc
        JOIN documents d ON dc.document_id = d.file_hash
        WHERE dc.code_pattern = ?
        ORDER BY d.filename
    """, (code,))
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        raise HTTPException(status_code=404, detail=f"Code '{code}' not found")
    
    # Group by document and load page info from content.json
    documents = {}
    contexts = []
    code_type = rows[0][1]
    
    for row in rows:
        file_hash = row[5]
        if file_hash not in documents:
            documents[file_hash] = {
                'id': row[3],
                'filename': row[4],
                'file_hash': file_hash,
                'doc_id': file_hash[:8] if file_hash else None,
                'pages': []
            }
            
            # Load content.json to get pages with this code
            content_path = os.path.join(DOCUMENTS_DIR, file_hash, 'content.json')
            if os.path.exists(content_path):
                with open(content_path, 'r', encoding='utf-8') as f:
                    doc_content = json.load(f)
                
                for page_data in doc_content.get('pages', []):
                    for code_info in page_data.get('codes', []):
                        if code_info.get('code') == code:
                            documents[file_hash]['pages'].append(page_data['page'])
                            if code_info.get('context'):
                                contexts.append({
                                    'page': page_data['page'],
                                    'context': code_info['context'],
                                    'document': row[4]
                                })
                            break
    
    category_info = get_code_category(code)
    rule_status = get_rule_status(code)
    
    return {
        'code': code,
        'type': code_type,
        'category': category_info,
        'documents': list(documents.values()),
        'contexts': contexts[:10],
        'rule_status': rule_status,
        'total_pages': sum(len(d['pages']) for d in documents.values())
    }


@router.get("/codes/{code}/sources")
async def get_code_sources(code: str, document_ids: Optional[str] = None):
    """
    Получает форматированные sources для кода (с doc_id headers).
    """
    doc_ids = document_ids.split(',') if document_ids else None
    sources_ctx = build_sources_context(code, doc_ids)
    
    if not sources_ctx.sources_text:
        raise HTTPException(status_code=404, detail=f"No sources found for code '{code}'")
    
    return {
        'code': code,
        'sources_text': sources_ctx.sources_text,
        'documents': [
            {
                'doc_id': doc.doc_id,
                'filename': doc.filename,
                'pages': doc.pages
            }
            for doc in sources_ctx.source_documents
        ],
        'total_pages': sources_ctx.total_pages
    }


@router.get("/codes/{code}/guideline")
async def get_code_guideline(code: str, document_ids: Optional[str] = None):
    """
    Получает текст гайдлайна для кода.
    DEPRECATED: Use /codes/{code}/sources instead.
    """
    doc_ids = document_ids.split(',') if document_ids else None
    sources_ctx = build_sources_context(code, doc_ids)
    
    if not sources_ctx.sources_text:
        raise HTTPException(status_code=404, detail=f"No guideline text found for code '{code}'")
    
    return {
        'code': code,
        'guideline': sources_ctx.sources_text,
        'length': len(sources_ctx.sources_text),
        'documents': [doc.doc_id for doc in sources_ctx.source_documents]
    }


@router.get("/codes/{code}/rule")
async def get_code_rule(code: str):
    """
    Получает существующее правило для кода.
    """
    # Check versioned structure first
    code_dir = code.replace(".", "_").replace("/", "_")
    versioned_path = os.path.join(RULES_DIR, code_dir)
    
    if os.path.exists(versioned_path):
        versions = [d for d in os.listdir(versioned_path) if d.startswith('v')]
        if versions:
            latest = sorted(versions, key=lambda x: int(x[1:]))[-1]
            rule_path = os.path.join(versioned_path, latest, "rule.json")
            if os.path.exists(rule_path):
                with open(rule_path, 'r') as f:
                    return json.load(f)
    
    # Fallback to flat structure
    rule_path = os.path.join(RULES_DIR, f"{code.replace('.', '_')}.json")
    
    if not os.path.exists(rule_path):
        raise HTTPException(status_code=404, detail=f"No rule found for code '{code}'")
    
    with open(rule_path, 'r') as f:
        return json.load(f)


@router.get("/codes/{code}/generation-log")
async def get_generation_log(code: str, version: Optional[int] = None):
    """
    Получает лог генерации для кода.
    """
    code_dir = code.replace(".", "_").replace("/", "_")
    versioned_path = os.path.join(RULES_DIR, code_dir)
    
    if not os.path.exists(versioned_path):
        raise HTTPException(status_code=404, detail=f"No generation log found for code '{code}'")
    
    versions = [d for d in os.listdir(versioned_path) if d.startswith('v')]
    if not versions:
        raise HTTPException(status_code=404, detail=f"No versions found for code '{code}'")
    
    if version:
        target_version = f"v{version}"
    else:
        target_version = sorted(versions, key=lambda x: int(x[1:]))[-1]
    
    log_path = os.path.join(versioned_path, target_version, "generation_log.json")
    
    if not os.path.exists(log_path):
        raise HTTPException(status_code=404, detail=f"Generation log not found for {code} {target_version}")
    
    with open(log_path, 'r') as f:
        return json.load(f)


# ============================================================
# GENERATION ENDPOINT
# ============================================================

@router.post("/generate/{code}")
async def generate_rule_endpoint(code: str, request: GenerateRuleRequest):
    """
    Генерирует правило для кода с SSE стримингом прогресса.
    
    Pipeline: Draft → Validation (Mentor + RedTeam) → Arbitration → Final
    
    SSE Event Format:
    {
        "step": "draft|mentor|redteam|arbitration|finalization|pipeline",
        "type": "status|thought|content|verification|done|error",
        "content": "...",
        "full_text": "...",  // on done
        "duration_ms": 15000  // on done
    }
    """
    # Check sources exist
    sources_ctx = build_sources_context(code, request.document_ids)
    
    if not sources_ctx.sources_text:
        raise HTTPException(
            status_code=400, 
            detail=f"No source documents found for code '{code}'. Upload relevant documents first."
        )
    
    async def event_stream():
        generator = RuleGenerator(thinking_budget=request.thinking_budget)
        
        async for event in generator.stream_pipeline(
            code=code,
            document_ids=request.document_ids,
            code_type=request.code_type,
            parallel_validators=request.parallel_validators
        ):
            yield f"data: {event}\n\n"
    
    return StreamingResponse(
        event_stream(), 
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.delete("/codes/{code}/rule")
async def delete_rule(code: str):
    """
    Удаляет правило для кода.
    """
    deleted = False
    
    # Delete versioned structure
    code_dir = code.replace(".", "_").replace("/", "_")
    versioned_path = os.path.join(RULES_DIR, code_dir)
    
    if os.path.exists(versioned_path):
        import shutil
        shutil.rmtree(versioned_path)
        deleted = True
    
    # Delete flat structure
    rule_path = os.path.join(RULES_DIR, f"{code.replace('.', '_')}.json")
    
    if os.path.exists(rule_path):
        os.remove(rule_path)
        deleted = True
    
    if not deleted:
        raise HTTPException(status_code=404, detail=f"No rule found for code '{code}'")
    
    return {'status': 'deleted', 'code': code}
