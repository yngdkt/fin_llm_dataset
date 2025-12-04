#!/usr/bin/env python3
"""
Interactive review script for books_review.jsonl records.

Usage:
    python scripts/processors/review_master.py

Commands during review:
    y / はい / yes  - Apply suggested changes
    n / いいえ / no - Skip this record (keep original)
    d / 除外 / del  - Remove record from master
    v / 確認       - Verify if resource exists (web search)
    q / quit       - Quit review session
    ? / help       - Show help
"""

import json
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

# Paths
BASE_DIR = Path(__file__).parent.parent.parent
MASTER_FILE = BASE_DIR / "data" / "master" / "books.jsonl"
REVIEW_FILE = BASE_DIR / "data" / "master" / "books_review.jsonl"
PROGRESS_FILE = BASE_DIR / "data" / "master" / ".review_progress.json"


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Load JSONL file."""
    records = []
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return records


def save_jsonl(records: List[Dict[str, Any]], path: Path) -> None:
    """Save records to JSONL file."""
    with open(path, 'w', encoding='utf-8') as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')


def load_progress() -> Dict[str, Any]:
    """Load review progress."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"reviewed_ids": [], "last_index": 0}


def save_progress(progress: Dict[str, Any]) -> None:
    """Save review progress."""
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, ensure_ascii=False, indent=2, fp=f)


def format_review(review: Dict[str, Any], index: int, total: int) -> str:
    """Format review record for display."""
    lines = []
    lines.append("")
    lines.append("=" * 60)
    lines.append(f"Review #{index + 1}/{total}")
    lines.append("=" * 60)

    # Basic info
    original = review.get('original_data', {})
    lines.append(f"Title: {review.get('title', 'N/A')}")
    lines.append(f"Work ID: {review.get('work_id', 'N/A')}")
    lines.append(f"Authors: {', '.join(original.get('authors', [])) or 'なし'}")
    lines.append(f"Publisher: {original.get('publisher', 'N/A')}")
    lines.append(f"Language: {original.get('language', 'N/A')}")

    # Review reasons
    reasons = review.get('reasons', [])
    lines.append(f"\nReview Reasons: {', '.join(reasons)}")

    # Details based on reason
    details = review.get('details', {})
    suggested = review.get('suggested_changes', {})

    if 'SEGMENT_CHANGE' in reasons:
        lines.append(f"\nCurrent Segment: {original.get('segment', 'N/A')} / {original.get('subsegment', 'N/A')}")
        lines.append(f"Suggested Segment: {suggested.get('segment', 'N/A')} / {suggested.get('subsegment', 'N/A')}")
        lines.append(f"Confidence: {suggested.get('confidence', 0):.2f}")
        if details.get('reasoning'):
            lines.append(f"Reasoning: {details['reasoning']}")

    if 'TITLE_MISMATCH' in reasons:
        lines.append(f"\nOriginal Title: {details.get('original_title', 'N/A')}")
        lines.append(f"API Title: {details.get('api_title', 'N/A')}")
        lines.append(f"Similarity: {details.get('similarity', 0):.2f}")

    if 'AUTHOR_MISMATCH' in reasons:
        lines.append(f"\nOriginal Authors: {details.get('original_authors', [])}")
        lines.append(f"API Authors: {details.get('api_authors', [])}")

    if 'NO_ISBN_FOUND' in reasons or 'MISSING_CRITICAL_DATA' in reasons:
        lines.append(f"\nMissing Fields: {details.get('missing_fields', ['isbn'])}")

    # Notes
    notes = []
    if not original.get('authors'):
        notes.append("著者なし")
    if original.get('publisher') == 'Unknown':
        notes.append("出版社不明")
    if 'NO_ISBN_FOUND' in reasons:
        notes.append("ISBNなし")

    if notes:
        lines.append(f"\nNotes: {', '.join(notes)}")

    lines.append("")
    lines.append("-" * 60)
    lines.append("Commands: [y]es/はい  [n]o/スキップ  [d]el/除外  [v]確認  [q]uit  [?]help")
    lines.append("-" * 60)

    return '\n'.join(lines)


def apply_segment_change(master_records: List[Dict[str, Any]],
                         work_id: str,
                         segment: str,
                         subsegment: str) -> bool:
    """Apply segment change to master record."""
    for r in master_records:
        if r.get('work_id') == work_id:
            r['segment'] = segment
            r['subsegment'] = subsegment
            return True
    return False


def remove_from_master(master_records: List[Dict[str, Any]], work_id: str) -> int:
    """Remove record from master. Returns number removed."""
    original_count = len(master_records)
    master_records[:] = [r for r in master_records if r.get('work_id') != work_id]
    return original_count - len(master_records)


def show_help():
    """Show help message."""
    print("""
Commands:
  y, yes, はい     - Apply suggested changes (segment/subsegment update)
  n, no, いいえ    - Skip this record (keep original, mark as reviewed)
  d, del, 除外     - Remove this record from master database
  v, verify, 確認  - Search web to verify if resource exists
  q, quit          - Save progress and quit review session
  ?, help          - Show this help message

Notes:
  - Progress is automatically saved
  - Removed records are permanently deleted from master
  - Skipped records won't appear in future reviews
""")


def main():
    print("=" * 60)
    print("Interactive Book Review Tool")
    print("=" * 60)

    # Load data
    print(f"\nLoading master: {MASTER_FILE}")
    master_records = load_jsonl(MASTER_FILE)
    print(f"  Total records: {len(master_records)}")

    print(f"\nLoading reviews: {REVIEW_FILE}")
    review_records = load_jsonl(REVIEW_FILE)
    print(f"  Total reviews: {len(review_records)}")

    # Load progress
    progress = load_progress()
    reviewed_ids = set(progress.get('reviewed_ids', []))
    print(f"  Already reviewed: {len(reviewed_ids)}")

    # Filter out already reviewed
    pending_reviews = [r for r in review_records if r.get('work_id') not in reviewed_ids]
    print(f"  Pending reviews: {len(pending_reviews)}")

    if not pending_reviews:
        print("\nAll reviews completed!")
        return

    # Stats
    stats = {
        'approved': 0,
        'skipped': 0,
        'removed': 0,
    }

    # Review loop
    try:
        for i, review in enumerate(pending_reviews):
            work_id = review.get('work_id', '')

            # Display review
            print(format_review(review, i, len(pending_reviews)))

            while True:
                try:
                    cmd = input("\n> ").strip().lower()
                except EOFError:
                    cmd = 'q'

                if cmd in ('y', 'yes', 'はい'):
                    # Apply changes
                    suggested = review.get('suggested_changes', {})
                    if suggested.get('segment') and suggested.get('subsegment'):
                        if apply_segment_change(
                            master_records,
                            work_id,
                            suggested['segment'],
                            suggested['subsegment']
                        ):
                            print(f"✓ Applied: {suggested['segment']} / {suggested['subsegment']}")
                            stats['approved'] += 1
                        else:
                            print(f"✗ Record not found in master: {work_id}")
                    else:
                        print("✓ Marked as reviewed (no changes to apply)")
                        stats['skipped'] += 1

                    reviewed_ids.add(work_id)
                    break

                elif cmd in ('n', 'no', 'いいえ', 'skip', 'スキップ'):
                    print("→ Skipped (keeping original)")
                    reviewed_ids.add(work_id)
                    stats['skipped'] += 1
                    break

                elif cmd in ('d', 'del', 'delete', '除外', 'remove'):
                    removed = remove_from_master(master_records, work_id)
                    if removed > 0:
                        print(f"✓ Removed from master (was {len(master_records) + removed} → {len(master_records)})")
                        stats['removed'] += 1
                    else:
                        print(f"✗ Record not found in master: {work_id}")
                    reviewed_ids.add(work_id)
                    break

                elif cmd in ('v', 'verify', '確認'):
                    title = review.get('title', '')
                    print(f"\n検索URL: https://www.google.com/search?q={title.replace(' ', '+')}")
                    print("ブラウザで確認してから、y/n/d を入力してください")
                    continue

                elif cmd in ('q', 'quit', '終了'):
                    raise KeyboardInterrupt

                elif cmd in ('?', 'help', 'ヘルプ'):
                    show_help()
                    continue

                else:
                    print("Unknown command. Type '?' for help.")
                    continue

            # Save progress after each review
            progress['reviewed_ids'] = list(reviewed_ids)
            progress['last_index'] = i + 1
            save_progress(progress)

    except KeyboardInterrupt:
        print("\n\nReview interrupted.")

    finally:
        # Save master
        print(f"\nSaving master to {MASTER_FILE}...")
        save_jsonl(master_records, MASTER_FILE)

        # Save progress
        progress['reviewed_ids'] = list(reviewed_ids)
        save_progress(progress)

        # Summary
        print("\n" + "=" * 60)
        print("Review Session Summary")
        print("=" * 60)
        print(f"  Approved (changes applied): {stats['approved']}")
        print(f"  Skipped (kept original): {stats['skipped']}")
        print(f"  Removed from master: {stats['removed']}")
        print(f"  Master records: {len(master_records)}")
        print(f"  Remaining reviews: {len(pending_reviews) - sum(stats.values())}")


if __name__ == '__main__':
    main()
