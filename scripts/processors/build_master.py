"""
Build and maintain the master JSONL file.

The master file (data/master/books.jsonl) is the single source of truth for all book records.
It consolidates data from multiple sources and handles deduplication.

Workflow:
1. Load existing master (if any)
2. Build dedup index from master
3. Process new/updated sources
4. Merge with deduplication
5. Write updated master

Usage:
    # Initial build from all v2 files
    python scripts/processors/build_master.py --init

    # Add new data from a source
    python scripts/processors/build_master.py --add data/raw/wiley/new_books.jsonl

    # Rebuild from all processed v2 files
    python scripts/processors/build_master.py --rebuild

    # Show statistics
    python scripts/processors/build_master.py --stats
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

# Add parent to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.processors.book_matcher import BookIndex, MatchResult
from scripts.processors.migrate_v1_to_v2 import convert_record_v1_to_v2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Default paths
DEFAULT_MASTER_DIR = Path("data/master")
DEFAULT_MASTER_FILE = DEFAULT_MASTER_DIR / "books.jsonl"
DEFAULT_BACKUP_DIR = DEFAULT_MASTER_DIR / "backups"


def is_v2_record(record: Dict[str, Any]) -> bool:
    """Check if a record is v2 format."""
    return 'editions' in record and isinstance(record.get('editions'), list)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Load records from a JSONL file."""
    records = []
    if not path.exists():
        return records

    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON: {e}")
    return records


def save_jsonl(records: List[Dict[str, Any]], path: Path) -> None:
    """Save records to a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')


def backup_master(master_path: Path, backup_dir: Path) -> Optional[Path]:
    """Create a timestamped backup of the master file."""
    if not master_path.exists():
        return None

    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"books_{timestamp}.jsonl"
    shutil.copy2(master_path, backup_path)
    logger.info(f"Backup created: {backup_path}")

    # Keep only last 10 backups
    backups = sorted(backup_dir.glob("books_*.jsonl"), reverse=True)
    for old_backup in backups[10:]:
        old_backup.unlink()
        logger.info(f"Removed old backup: {old_backup}")

    return backup_path


def merge_records(
    existing: Dict[str, Any],
    new: Dict[str, Any],
    match_result: MatchResult
) -> Dict[str, Any]:
    """
    Merge a new record into an existing one.

    Strategy:
    - Keep existing work_id
    - Merge editions (add new editions, update existing)
    - Merge data_sources
    - Update timestamps
    """
    merged = existing.copy()

    # Merge data_sources
    existing_sources = set(merged.get('data_sources', []))
    new_sources = set(new.get('data_sources', []))
    merged['data_sources'] = list(existing_sources | new_sources)

    # Merge editions
    existing_editions = {e['edition_number']: e for e in merged.get('editions', [])}
    for new_edition in new.get('editions', []):
        edition_num = new_edition['edition_number']
        if edition_num in existing_editions:
            # Merge formats for existing edition
            existing_formats = {f['format_type']: f for f in existing_editions[edition_num].get('formats', [])}
            for new_format in new_edition.get('formats', []):
                fmt_type = new_format['format_type']
                if fmt_type not in existing_formats:
                    existing_editions[edition_num].setdefault('formats', []).append(new_format)
                # Could also update price/availability here
        else:
            # Add new edition
            existing_editions[edition_num] = new_edition

    # Sort editions by number descending and update is_latest
    sorted_editions = sorted(existing_editions.values(), key=lambda e: e['edition_number'], reverse=True)
    for i, edition in enumerate(sorted_editions):
        edition['is_latest'] = (i == 0)
    merged['editions'] = sorted_editions

    # Update timestamp
    merged['updated_at'] = datetime.utcnow().isoformat() + 'Z'

    return merged


def add_to_master(
    master_records: List[Dict[str, Any]],
    new_records: List[Dict[str, Any]],
    index: BookIndex,
    convert_v1: bool = True
) -> Tuple[int, int, int]:
    """
    Add new records to master with deduplication.

    Returns:
        Tuple of (added, merged, skipped) counts
    """
    added = 0
    merged = 0
    skipped = 0

    # Build work_id to index mapping for fast lookup
    work_id_to_idx: Dict[str, int] = {
        r.get('work_id', ''): i for i, r in enumerate(master_records)
    }

    for record in new_records:
        # Convert v1 to v2 if needed
        if not is_v2_record(record):
            if convert_v1:
                record = convert_record_v1_to_v2(record)
            else:
                skipped += 1
                continue

        # Check for existing match
        match = index.find_match(record)

        if match:
            matched_book, match_result = match
            matched_work_id = matched_book.get('work_id', '')

            if matched_work_id in work_id_to_idx:
                idx = work_id_to_idx[matched_work_id]
                master_records[idx] = merge_records(master_records[idx], record, match_result)

                if match_result.confidence >= 0.90:
                    merged += 1
                    logger.debug(f"Merged: {record.get('title', '')[:50]}... ({match_result.confidence:.0%})")
                else:
                    merged += 1
                    logger.info(f"Merged (review): {record.get('title', '')[:50]}... with {matched_book.get('title', '')[:50]}... ({match_result.confidence:.0%})")
            else:
                # Matched book not in master_records yet (shouldn't happen normally)
                master_records.append(record)
                index.add(record)
                work_id_to_idx[record.get('work_id', '')] = len(master_records) - 1
                added += 1
                logger.debug(f"Added (match not found): {record.get('title', '')[:50]}...")
        else:
            # No match - add as new
            master_records.append(record)
            index.add(record)
            work_id_to_idx[record.get('work_id', '')] = len(master_records) - 1
            added += 1
            logger.debug(f"Added: {record.get('title', '')[:50]}...")

    return added, merged, skipped


def init_master(
    source_dirs: List[Path],
    master_path: Path,
    pattern: str = "*_v2.jsonl"
) -> Dict[str, int]:
    """
    Initialize master from processed v2 files.
    """
    logger.info(f"Initializing master from {source_dirs}")

    all_records = []
    for source_dir in source_dirs:
        if source_dir.is_file():
            files = [source_dir]
        else:
            files = list(source_dir.glob(pattern))

        for file_path in files:
            logger.info(f"Loading {file_path}")
            records = load_jsonl(file_path)
            all_records.extend(records)

    logger.info(f"Loaded {len(all_records)} records total")

    # Build index and deduplicate
    index = BookIndex()
    master_records: List[Dict[str, Any]] = []

    added, merged, skipped = add_to_master(master_records, all_records, index)

    # Save master
    save_jsonl(master_records, master_path)
    logger.info(f"Master saved to {master_path}")

    return {
        'total_input': len(all_records),
        'added': added,
        'merged': merged,
        'skipped': skipped,
        'final_count': len(master_records),
    }


def add_source(
    source_path: Path,
    master_path: Path,
    convert_v1: bool = True
) -> Dict[str, int]:
    """
    Add records from a source file to the master.
    """
    # Backup existing master
    backup_master(master_path, DEFAULT_BACKUP_DIR)

    # Load existing master
    master_records = load_jsonl(master_path)
    logger.info(f"Loaded {len(master_records)} existing records")

    # Build index from master
    index = BookIndex(master_records)

    # Load new records
    new_records = load_jsonl(source_path)
    logger.info(f"Processing {len(new_records)} new records from {source_path}")

    # Add with deduplication
    added, merged, skipped = add_to_master(master_records, new_records, index, convert_v1)

    # Save updated master
    save_jsonl(master_records, master_path)
    logger.info(f"Master updated: {master_path}")

    return {
        'source': str(source_path),
        'input_count': len(new_records),
        'added': added,
        'merged': merged,
        'skipped': skipped,
        'final_count': len(master_records),
    }


def show_stats(master_path: Path) -> None:
    """Show statistics about the master file."""
    records = load_jsonl(master_path)

    if not records:
        print("Master file is empty or does not exist.")
        return

    # Basic counts
    print(f"\n{'=' * 60}")
    print(f"Master File Statistics: {master_path}")
    print(f"{'=' * 60}")
    print(f"Total records: {len(records):,}")

    # By language
    lang_counts: Dict[str, int] = {}
    for r in records:
        lang = r.get('language', 'unknown')
        lang_counts[lang] = lang_counts.get(lang, 0) + 1
    print(f"\nBy Language:")
    for lang, count in sorted(lang_counts.items(), key=lambda x: -x[1]):
        print(f"  {lang}: {count:,}")

    # By segment
    segment_counts: Dict[str, int] = {}
    for r in records:
        segment = r.get('segment', 'unknown')
        segment_counts[segment] = segment_counts.get(segment, 0) + 1
    print(f"\nBy Segment:")
    for segment, count in sorted(segment_counts.items(), key=lambda x: -x[1]):
        print(f"  {segment}: {count:,}")

    # By perspective
    perspective_counts: Dict[str, int] = {}
    for r in records:
        perspective = r.get('perspective', 'unknown')
        perspective_counts[perspective] = perspective_counts.get(perspective, 0) + 1
    print(f"\nBy Perspective:")
    for perspective, count in sorted(perspective_counts.items(), key=lambda x: -x[1]):
        print(f"  {perspective}: {count:,}")

    # By publisher (top 10)
    publisher_counts: Dict[str, int] = {}
    for r in records:
        publisher = r.get('publisher', 'unknown')
        publisher_counts[publisher] = publisher_counts.get(publisher, 0) + 1
    print(f"\nTop Publishers:")
    for publisher, count in sorted(publisher_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"  {publisher}: {count:,}")

    # Data sources
    source_counts: Dict[str, int] = {}
    for r in records:
        for source in r.get('data_sources', []):
            source_counts[source] = source_counts.get(source, 0) + 1
    if source_counts:
        print(f"\nData Sources:")
        for source, count in sorted(source_counts.items(), key=lambda x: -x[1]):
            print(f"  {source}: {count:,}")

    # Edition statistics
    total_editions = sum(len(r.get('editions', [])) for r in records)
    total_formats = sum(
        len(e.get('formats', []))
        for r in records
        for e in r.get('editions', [])
    )
    print(f"\nEditions & Formats:")
    print(f"  Total editions: {total_editions:,}")
    print(f"  Total formats: {total_formats:,}")
    print(f"  Avg editions per work: {total_editions / len(records):.2f}")


def main():
    parser = argparse.ArgumentParser(
        description='Build and maintain the master JSONL file'
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--init',
        action='store_true',
        help='Initialize master from processed v2 files'
    )
    group.add_argument(
        '--add',
        type=Path,
        help='Add records from a source file'
    )
    group.add_argument(
        '--rebuild',
        action='store_true',
        help='Rebuild master from all sources'
    )
    group.add_argument(
        '--stats',
        action='store_true',
        help='Show master file statistics'
    )

    parser.add_argument(
        '--master',
        type=Path,
        default=DEFAULT_MASTER_FILE,
        help=f'Master file path (default: {DEFAULT_MASTER_FILE})'
    )
    parser.add_argument(
        '--source-dir',
        type=Path,
        default=Path('data/processed'),
        help='Source directory for init/rebuild (default: data/processed)'
    )
    parser.add_argument(
        '--pattern',
        default='*_v2.jsonl',
        help='Glob pattern for source files (default: *_v2.jsonl)'
    )
    parser.add_argument(
        '--no-convert-v1',
        action='store_true',
        help='Skip v1 records instead of converting'
    )

    args = parser.parse_args()

    if args.init or args.rebuild:
        if args.rebuild and args.master.exists():
            backup_master(args.master, DEFAULT_BACKUP_DIR)

        stats = init_master(
            [args.source_dir],
            args.master,
            args.pattern
        )

        print(f"\n{'=' * 60}")
        print("Build Complete")
        print(f"{'=' * 60}")
        for key, value in stats.items():
            print(f"  {key}: {value:,}" if isinstance(value, int) else f"  {key}: {value}")

    elif args.add:
        stats = add_source(
            args.add,
            args.master,
            convert_v1=not args.no_convert_v1
        )

        print(f"\n{'=' * 60}")
        print("Add Complete")
        print(f"{'=' * 60}")
        for key, value in stats.items():
            print(f"  {key}: {value:,}" if isinstance(value, int) else f"  {key}: {value}")

    elif args.stats:
        show_stats(args.master)


if __name__ == '__main__':
    main()
