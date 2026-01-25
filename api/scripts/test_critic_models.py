#!/usr/bin/env python3
"""Test GPT-5.1 vs GPT-4.1 as critic."""

import asyncio
import sys
sys.path.insert(0, '.')

from pypdf import PdfReader
from src.parsers.document_parser import (
    load_meta_categories_from_json,
    format_meta_categories_for_prompt,
    format_topics_for_prompt
)
from src.parsers.multi_model_pipeline import (
    load_topics_from_db,
    stage1_draft_extraction,
    CRITIC_PROMPT
)
from src.generators.core_ai import call_openai_model
import time
import json


async def test_critic_comparison():
    reader = PdfReader('data/raw/documents/codebooks/icd_10_cm_october_2025_guidelines_0.pdf')
    page_texts = [page.extract_text() or '' for page in reader.pages[15:30]]  # Pages 16-30

    topics = load_topics_from_db()
    meta_cats = load_meta_categories_from_json()

    # Get Gemini draft first
    print('=== GEMINI DRAFT ===')
    start = time.time()
    draft = await stage1_draft_extraction(page_texts, topics, meta_cats, start_page=16)
    draft_time = int((time.time() - start) * 1000)
    print(f'Draft time: {draft_time}ms')

    # Format for critic
    source_text = '\n\n'.join(page_texts)[:15000]
    meta_cats_str = format_meta_categories_for_prompt(meta_cats)
    topics_str = format_topics_for_prompt(topics)

    critic_prompt = CRITIC_PROMPT.format(
        source_text=source_text,
        draft_extraction=draft,
        meta_categories=meta_cats_str,
        topics_dictionary=topics_str
    )

    # Test GPT-5.1 critic
    print('\n=== GPT-5.1 CRITIC ===')
    start = time.time()
    gpt51_response = await call_openai_model(critic_prompt, model='gpt-5.1', reasoning_effort='medium')
    gpt51_time = int((time.time() - start) * 1000)

    # Parse issues
    try:
        if '```json' in gpt51_response:
            json_str = gpt51_response.split('```json')[1].split('```')[0]
        elif '```' in gpt51_response:
            json_str = gpt51_response.split('```')[1].split('```')[0]
        else:
            json_str = gpt51_response
        gpt51_issues = json.loads(json_str.strip())
    except Exception as e:
        print(f'Parse error: {e}')
        gpt51_issues = []

    print(f'Time: {gpt51_time}ms')
    print(f'Issues found: {len(gpt51_issues)}')
    if gpt51_issues:
        print(f'Issue types: {set(i.get("issue_type") for i in gpt51_issues)}')
        print(f'Sample issue: {gpt51_issues[0]}')

    # Test GPT-4.1 critic
    print('\n=== GPT-4.1 CRITIC ===')
    start = time.time()
    gpt41_response = await call_openai_model(critic_prompt, model='gpt-4.1')
    gpt41_time = int((time.time() - start) * 1000)

    try:
        if '```json' in gpt41_response:
            json_str = gpt41_response.split('```json')[1].split('```')[0]
        elif '```' in gpt41_response:
            json_str = gpt41_response.split('```')[1].split('```')[0]
        else:
            json_str = gpt41_response
        gpt41_issues = json.loads(json_str.strip())
    except Exception as e:
        print(f'Parse error: {e}')
        gpt41_issues = []

    print(f'Time: {gpt41_time}ms')
    print(f'Issues found: {len(gpt41_issues)}')
    if gpt41_issues:
        print(f'Issue types: {set(i.get("issue_type") for i in gpt41_issues)}')
        print(f'Sample issue: {gpt41_issues[0]}')

    # Comparison
    print('\n=== COMPARISON ===')
    print(f'GPT-5.1: {gpt51_time}ms, {len(gpt51_issues)} issues')
    print(f'GPT-4.1: {gpt41_time}ms, {len(gpt41_issues)} issues')
    if gpt41_time > 0:
        print(f'Speed difference: GPT-4.1 is {gpt51_time/gpt41_time:.1f}x faster')

    # Estimate full pipeline
    print('\n=== FULL PIPELINE ESTIMATE (121 pages, 9 chunks) ===')
    gemini_per_chunk = draft_time
    fix_time = 15000  # ~15s for GPT-4.1 fix

    # Current: Gemini + GPT-5.1 + GPT-4.1 fix
    current_per_chunk = gemini_per_chunk + gpt51_time + fix_time
    current_total = (current_per_chunk * 9) / 3  # parallel=3

    # Alternative: Gemini + GPT-4.1 critic + GPT-4.1 fix
    alt_per_chunk = gemini_per_chunk + gpt41_time + fix_time
    alt_total = (alt_per_chunk * 9) / 3

    print(f'Current (GPT-5.1 critic): ~{int(current_total/1000/60)}m {int(current_total/1000)%60}s')
    print(f'With GPT-4.1 critic: ~{int(alt_total/1000/60)}m {int(alt_total/1000)%60}s')
    print(f'Savings: ~{int((current_total - alt_total)/1000)}s ({int((1 - alt_total/current_total)*100)}% faster)')


if __name__ == '__main__':
    asyncio.run(test_critic_comparison())
