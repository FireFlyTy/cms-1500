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

from src.utils.code_categories import get_code_category, group_codes_by_category, get_all_categories
from src.db.connection import get_db_connection

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
    code: str
    code_type: str = "ICD-10"
    document_ids: Optional[List[str]] = None  # If None, use all relevant docs


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
            dc.code_pattern as code,
            dc.code_type,
            dc.document_id,
            d.filename
        FROM document_codes dc
        JOIN documents d ON dc.document_id = d.file_hash
        WHERE d.parsed_at IS NOT NULL
        GROUP BY dc.code_pattern, dc.code_type, dc.document_id
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
                'type': row[1] or 'ICD-10',
                'documents': [],
                'total_pages': 0
            }

        codes_map[code]['documents'].append({
            'id': row[2],
            'filename': row[3]
        })
        codes_map[code]['total_pages'] += 1

    return list(codes_map.values())


def get_rule_status(code: str) -> Dict:
    """Проверяет статус правила для кода."""
    rule_path = os.path.join(RULES_DIR, f"{code.replace('.', '_')}.json")

    if os.path.exists(rule_path):
        with open(rule_path, 'r') as f:
            rule = json.load(f)
        return {
            'has_rule': True,
            'is_mock': rule.get('is_mock', False),
            'version': rule.get('version', '1.0'),
            'created_at': rule.get('created_at'),
            'updated_at': rule.get('updated_at')
        }

    return {'has_rule': False, 'is_mock': False}


def get_guideline_text_for_code(code: str, document_ids: Optional[List[str]] = None) -> str:
    """
    Собирает текст гайдлайнов для кода из всех релевантных документов.
    Ищет страницы где упоминается код в content.json.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get documents that have this code (document_id = file_hash)
    if document_ids:
        placeholders = ','.join('?' * len(document_ids))
        cursor.execute(f"""
            SELECT DISTINCT dc.document_id
            FROM document_codes dc
            JOIN documents d ON dc.document_id = d.file_hash
            WHERE dc.code_pattern = ? AND dc.document_id IN ({placeholders})
        """, [code] + document_ids)
    else:
        cursor.execute("""
            SELECT DISTINCT dc.document_id
            FROM document_codes dc
            JOIN documents d ON dc.document_id = d.file_hash
            WHERE dc.code_pattern = ?
        """, (code,))

    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return ""

    # Load content from each document and find pages with this code
    guideline_parts = []

    for (file_hash,) in rows:
        content_path = os.path.join(DOCUMENTS_DIR, file_hash, 'content.json')

        if not os.path.exists(content_path):
            continue

        with open(content_path, 'r') as f:
            doc_data = json.load(f)

        # Find pages where this code appears
        for page_data in doc_data.get('pages', []):
            page_codes = [c.get('code') for c in page_data.get('codes', [])]
            if code in page_codes and page_data.get('content'):
                guideline_parts.append(f"## Page {page_data['page']}\n{page_data['content']}")

    return "\n\n".join(guideline_parts)


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

        # Get color from first code's category_info
        color = codes[0]['category_info'].get('color', '#6B7280') if codes else '#6B7280'

        categories.append({
            'name': category_name,
            'color': color,
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
    Группирует по типу: diagnoses (ICD-10) и procedures (CPT/HCPCS).
    """
    all_codes = get_all_codes_from_db()
    grouped = group_codes_by_category(all_codes)

    if category_name not in grouped:
        raise HTTPException(status_code=404, detail=f"Category '{category_name}' not found")

    codes = grouped[category_name]

    # Separate into diagnoses and procedures
    diagnoses = []
    procedures = []

    for code_info in codes:
        rule_status = get_rule_status(code_info['code'])
        enriched = {
            **code_info,
            'rule_status': rule_status
        }

        code_type = code_info.get('type', '').upper()
        if code_type in ('ICD-10', 'ICD10', 'ICD'):
            diagnoses.append(enriched)
        else:  # CPT, HCPCS, etc.
            procedures.append(enriched)

    # Sort by code
    diagnoses.sort(key=lambda x: x['code'])
    procedures.sort(key=lambda x: x['code'])

    return {
        'category': category_name,
        'diagnoses': diagnoses,
        'procedures': procedures,
        'total': len(diagnoses) + len(procedures),
        'total_diagnoses': len(diagnoses),
        'total_procedures': len(procedures),
        'with_rules': sum(1 for c in diagnoses + procedures if c['rule_status']['has_rule'])
    }


@router.get("/codes/{code}")
async def get_code_details(code: str):
    """
    Получает детальную информацию о коде.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get documents that have this code (document_id = file_hash)
    cursor.execute("""
        SELECT 
            dc.code_pattern,
            dc.code_type,
            dc.document_id as file_hash,
            d.filename
        FROM document_codes dc
        JOIN documents d ON dc.document_id = d.file_hash
        WHERE dc.code_pattern = ? AND d.parsed_at IS NOT NULL
        ORDER BY d.filename
    """, (code,))

    rows = cursor.fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Code '{code}' not found")

    # Aggregate documents and find pages with this code from content.json
    documents = {}
    contexts = []
    code_type = rows[0][1] or 'ICD-10'

    for row in rows:
        file_hash = row[2]
        filename = row[3]

        if file_hash not in documents:
            documents[file_hash] = {
                'id': file_hash,
                'filename': filename,
                'file_hash': file_hash,
                'pages': []
            }

        # Load content.json to find pages
        content_path = os.path.join(DOCUMENTS_DIR, file_hash, 'content.json')
        if os.path.exists(content_path):
            with open(content_path, 'r') as f:
                doc_data = json.load(f)

            for page_data in doc_data.get('pages', []):
                for page_code in page_data.get('codes', []):
                    if page_code.get('code') == code:
                        page_num = page_data.get('page')
                        if page_num not in documents[file_hash]['pages']:
                            documents[file_hash]['pages'].append(page_num)
                        # Add context
                        if page_code.get('context'):
                            contexts.append({
                                'page': page_num,
                                'context': page_code.get('context'),
                                'document': filename
                            })

    category_info = get_code_category(code)
    rule_status = get_rule_status(code)

    return {
        'code': code,
        'type': code_type,
        'category': category_info,
        'documents': list(documents.values()),
        'contexts': contexts[:10],  # Limit contexts
        'rule_status': rule_status,
        'total_pages': sum(len(d['pages']) for d in documents.values())
    }


@router.get("/codes/{code}/guideline")
async def get_code_guideline(code: str, document_ids: Optional[str] = None):
    """
    Получает текст гайдлайна для кода.
    """
    doc_ids = document_ids.split(',') if document_ids else None
    guideline_text = get_guideline_text_for_code(code, doc_ids)

    if not guideline_text:
        raise HTTPException(status_code=404, detail=f"No guideline text found for code '{code}'")

    return {
        'code': code,
        'guideline': guideline_text,
        'length': len(guideline_text)
    }


@router.get("/codes/{code}/rule")
async def get_code_rule(code: str):
    """
    Получает существующее правило для кода.
    """
    rule_path = os.path.join(RULES_DIR, f"{code.replace('.', '_')}.json")

    if not os.path.exists(rule_path):
        raise HTTPException(status_code=404, detail=f"No rule found for code '{code}'")

    with open(rule_path, 'r') as f:
        rule = json.load(f)

    return rule


@router.post("/generate/{code}")
async def generate_rule_stream(code: str, request: GenerateRuleRequest):
    """
    Генерирует правило для кода с SSE стримингом прогресса.

    Pipeline: Draft → Validation (Mentor + RedTeam) → Arbitration → Final
    """
    # Get guideline text
    guideline_text = get_guideline_text_for_code(code, request.document_ids)

    if not guideline_text:
        raise HTTPException(status_code=400, detail=f"No guideline text found for code '{code}'")

    async def generate_stream():
        import json
        from datetime import datetime

        # Step 1: Draft
        yield f"data: {json.dumps({'step': 'draft', 'status': 'starting', 'message': 'Generating draft...'})}\n\n"

        # TODO: Implement actual generation with core_ai
        await asyncio.sleep(1)  # Placeholder

        yield f"data: {json.dumps({'step': 'draft', 'status': 'complete', 'message': 'Draft complete'})}\n\n"

        # Step 2: Validation
        yield f"data: {json.dumps({'step': 'validation', 'status': 'starting', 'message': 'Running validators...'})}\n\n"

        await asyncio.sleep(1)

        yield f"data: {json.dumps({'step': 'validation', 'status': 'complete', 'message': 'Validation complete'})}\n\n"

        # Step 3: Arbitration
        yield f"data: {json.dumps({'step': 'arbitration', 'status': 'starting', 'message': 'Arbitrating corrections...'})}\n\n"

        await asyncio.sleep(1)

        yield f"data: {json.dumps({'step': 'arbitration', 'status': 'complete', 'message': 'Arbitration complete'})}\n\n"

        # Step 4: Finalization
        yield f"data: {json.dumps({'step': 'final', 'status': 'starting', 'message': 'Finalizing rule...'})}\n\n"

        await asyncio.sleep(1)

        # Save rule with is_mock flag
        rule = {
            'code': code,
            'code_type': request.code_type,
            'version': '1.0',
            'is_mock': True,  # Mark as mock - no real content yet
            'created_at': datetime.utcnow().isoformat() + 'Z',
            'draft': '# Mock draft - real content pending',
            'final': '# Mock rule - real content pending',
            'citations': []
        }

        rule_path = os.path.join(RULES_DIR, f"{code.replace('.', '_')}.json")
        with open(rule_path, 'w') as f:
            json.dump(rule, f, indent=2)

        yield f"data: {json.dumps({'step': 'final', 'status': 'complete', 'message': 'Rule saved'})}\n\n"
        yield f"data: {json.dumps({'step': 'done', 'status': 'complete', 'rule': rule})}\n\n"

    return StreamingResponse(generate_stream(), media_type="text/event-stream")


@router.delete("/codes/{code}/rule")
async def delete_rule(code: str):
    """
    Удаляет правило для кода.
    """
    rule_path = os.path.join(RULES_DIR, f"{code.replace('.', '_')}.json")

    if not os.path.exists(rule_path):
        raise HTTPException(status_code=404, detail=f"No rule found for code '{code}'")

    os.remove(rule_path)

    return {'status': 'deleted', 'code': code}


@router.delete("/mock")
async def clear_mock_rules():
    """
    Удаляет все mock-правила (is_mock=True).
    Используется для перезапуска генерации.
    """
    deleted = []
    kept = []

    if not os.path.exists(RULES_DIR):
        return {'deleted': 0, 'kept': 0, 'codes': []}

    for filename in os.listdir(RULES_DIR):
        if not filename.endswith('.json'):
            continue

        rule_path = os.path.join(RULES_DIR, filename)
        try:
            with open(rule_path, 'r') as f:
                rule = json.load(f)

            if rule.get('is_mock', False):
                os.remove(rule_path)
                deleted.append(rule.get('code', filename))
            else:
                kept.append(rule.get('code', filename))
        except Exception as e:
            print(f"Error processing {filename}: {e}")

    return {
        'deleted': len(deleted),
        'kept': len(kept),
        'deleted_codes': deleted
    }


@router.get("/stats")
async def get_rules_stats():
    """
    Статистика по правилам.
    """
    total = 0
    mock_count = 0
    real_count = 0

    if os.path.exists(RULES_DIR):
        for filename in os.listdir(RULES_DIR):
            if not filename.endswith('.json'):
                continue

            total += 1
            rule_path = os.path.join(RULES_DIR, filename)
            try:
                with open(rule_path, 'r') as f:
                    rule = json.load(f)
                if rule.get('is_mock', False):
                    mock_count += 1
                else:
                    real_count += 1
            except:
                pass

    return {
        'total': total,
        'mock': mock_count,
        'real': real_count
    }