#!/usr/bin/env python3
"""
Rebuild summary in content.json files from existing page data.

This is a quick fix script - no API calls, just recalculates summary
from already extracted page-level data.
"""

import sys
import json
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import DOCUMENTS_STORE_DIR


def rebuild_summary(content_json: dict) -> dict:
    """Rebuild summary from pages data."""
    codes_dict = {}
    topics_dict = {}
    medications_dict = {}
    content_pages = []
    skipped_pages = []

    for page in content_json.get("pages", []):
        page_num = page.get("page", 0)
        page_content = page.get("content")

        # Track content/skipped pages
        if page_content:
            content_pages.append(page_num)
        else:
            skipped_pages.append(page_num)

        # Aggregate codes
        for code_entry in page.get("codes", []):
            if isinstance(code_entry, dict):
                code = code_entry.get("code")
                code_type = code_entry.get("type")
                anchor_start = code_entry.get("anchor_start")
                anchor_end = code_entry.get("anchor_end")
            else:
                continue

            if not code:
                continue

            if code not in codes_dict:
                codes_dict[code] = {
                    "code": code,
                    "type": code_type,
                    "pages": [],
                    "anchors": []
                }
            if page_num not in codes_dict[code]["pages"]:
                codes_dict[code]["pages"].append(page_num)
            if anchor_start and anchor_end:
                codes_dict[code]["anchors"].append({
                    "page": page_num,
                    "start": anchor_start,
                    "end": anchor_end
                })

        # Aggregate topics
        for topic_entry in page.get("topics", []):
            if isinstance(topic_entry, dict):
                name = topic_entry.get("name")
                anchor_start = topic_entry.get("anchor_start")
                anchor_end = topic_entry.get("anchor_end")
            elif isinstance(topic_entry, str):
                name = topic_entry
                anchor_start = None
                anchor_end = None
            else:
                continue

            if not name:
                continue

            if name not in topics_dict:
                topics_dict[name] = {
                    "name": name,
                    "pages": [],
                    "anchors": []
                }
            if page_num not in topics_dict[name]["pages"]:
                topics_dict[name]["pages"].append(page_num)
            if anchor_start and anchor_end:
                topics_dict[name]["anchors"].append({
                    "page": page_num,
                    "start": anchor_start,
                    "end": anchor_end
                })

        # Aggregate medications
        for med_entry in page.get("medications", []):
            if isinstance(med_entry, dict):
                name = med_entry.get("name")
                anchor = med_entry.get("anchor")
                anchor_start = med_entry.get("anchor_start")
                anchor_end = med_entry.get("anchor_end")
            elif isinstance(med_entry, str):
                name = med_entry
                anchor = None
                anchor_start = None
                anchor_end = None
            else:
                continue

            if not name:
                continue

            if name not in medications_dict:
                medications_dict[name] = {
                    "name": name,
                    "pages": [],
                    "anchors": []
                }
            if page_num not in medications_dict[name]["pages"]:
                medications_dict[name]["pages"].append(page_num)
            if anchor_start and anchor_end:
                medications_dict[name]["anchors"].append({
                    "page": page_num,
                    "start": anchor_start,
                    "end": anchor_end
                })
            elif anchor:
                medications_dict[name]["anchors"].append({
                    "page": page_num,
                    "text": anchor
                })

    # Determine doc_type from page content
    doc_type = "unknown"
    # Could add logic here based on content analysis

    # Return in format expected by frontend
    return {
        "doc_type": doc_type,
        "all_codes": list(codes_dict.values()),
        "topics": sorted(topics_dict.values(), key=lambda x: x.get("name", "")),
        "medications": sorted(medications_dict.values(), key=lambda x: x.get("name", "")),
        "content_pages": content_pages,
        "skipped_pages": skipped_pages,
        "content_page_count": len(content_pages)
    }


def main():
    print("=" * 60)
    print("Rebuilding summary in content.json files")
    print("=" * 60)

    # Find all content.json files
    doc_dirs = [d for d in DOCUMENTS_STORE_DIR.iterdir() if d.is_dir()]

    print(f"Found {len(doc_dirs)} document directories")

    updated = 0
    errors = 0

    for doc_dir in doc_dirs:
        json_path = doc_dir / "content.json"
        if not json_path.exists():
            print(f"  Skip {doc_dir.name}: no content.json")
            continue

        try:
            # Load existing
            with open(json_path, 'r', encoding='utf-8') as f:
                content = json.load(f)

            filename = content.get("filename", doc_dir.name)
            old_summary = content.get("summary", {})

            # Count old (handle both dict and list formats)
            old_codes_data = old_summary.get("all_codes") or old_summary.get("codes", {})
            old_topics_data = old_summary.get("topics", {})
            old_meds_data = old_summary.get("medications", {})
            old_codes = len(old_codes_data) if isinstance(old_codes_data, (list, dict)) else 0
            old_topics = len(old_topics_data) if isinstance(old_topics_data, (list, dict)) else 0
            old_meds = len(old_meds_data) if isinstance(old_meds_data, (list, dict)) else 0

            # Rebuild
            new_summary = rebuild_summary(content)
            content["summary"] = new_summary

            # Count new
            new_codes = len(new_summary.get("all_codes", []))
            new_topics = len(new_summary.get("topics", []))
            new_meds = len(new_summary.get("medications", []))

            # Save
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(content, f, indent=2, ensure_ascii=False)

            print(f"  {filename}")
            print(f"    codes: {old_codes} -> {new_codes}")
            print(f"    topics: {old_topics} -> {new_topics}")
            print(f"    medications: {old_meds} -> {new_meds}")

            updated += 1

        except Exception as e:
            print(f"  ERROR {doc_dir.name}: {e}")
            errors += 1

    print()
    print("=" * 60)
    print(f"Updated: {updated}")
    print(f"Errors: {errors}")
    print("=" * 60)


if __name__ == "__main__":
    main()
