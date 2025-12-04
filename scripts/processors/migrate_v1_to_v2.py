"""Migration script to convert v1 book records to v2 schema format."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def normalize_for_id(text: str) -> str:
    """Normalize text for ID generation."""
    text = text.lower()
    # Remove edition info
    text = re.sub(r',?\s*\d+(st|nd|rd|th)\s+edition', '', text, flags=re.IGNORECASE)
    text = re.sub(r',?\s*第\d+版', '', text)
    # Remove special characters
    text = re.sub(r'[^\w\s]', '', text)
    # Normalize whitespace
    text = re.sub(r'\s+', '_', text.strip())
    return text


def generate_work_id(publisher: str, title: str, first_author: Optional[str] = None) -> str:
    """Generate a stable work_id from publisher, title, and author."""
    normalized = normalize_for_id(title)
    if first_author:
        normalized += "_" + normalize_for_id(first_author)

    hash_str = hashlib.md5(normalized.encode()).hexdigest()[:12]
    publisher_slug = re.sub(r'[^a-z0-9]', '', publisher.lower())

    return f"{publisher_slug}_{hash_str}"


def parse_edition_from_title(title: str) -> tuple[str, int, Optional[str]]:
    """
    Extract edition info from title.
    Returns: (clean_title, edition_number, edition_label)
    """
    patterns = [
        (r',?\s*(\d+)(st|nd|rd|th)\s+Edition', r'\1'),
        (r',?\s*(\d+)(st|nd|rd|th)\s+ed\.?', r'\1'),
        (r',?\s*第(\d+)版', r'\1'),
        (r',?\s*Edition\s+(\d+)', r'\1'),
    ]

    for pattern, _ in patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            edition_num = int(match.group(1))
            edition_label = match.group(0).strip().lstrip(',').strip()
            clean_title = re.sub(pattern, '', title, flags=re.IGNORECASE).strip()
            return clean_title, edition_num, edition_label

    return title, 1, None


def parse_isbn(isbn_str: Optional[str]) -> Optional[str]:
    """Clean and validate ISBN string."""
    if not isbn_str:
        return None

    # Remove hyphens and spaces
    isbn = re.sub(r'[-\s]', '', isbn_str)

    # Check if valid ISBN-10 or ISBN-13
    if re.match(r'^(97[89])?\d{9}[\dX]$', isbn):
        return isbn

    return None


def convert_format(v1_format: Optional[str]) -> str:
    """Convert v1 format to v2 format_type."""
    format_map = {
        'print': 'paperback',
        'ebook': 'ebook',
        'hybrid': 'bundle',
        'unknown': 'other',
        None: 'other',
    }
    return format_map.get(v1_format, 'other')


def convert_record_v1_to_v2(v1_record: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a v1 record to v2 format."""

    # Parse edition info from title
    title = v1_record.get('title', '')
    clean_title, edition_number, edition_label = parse_edition_from_title(title)

    # Generate work_id
    publisher = v1_record.get('publisher', 'unknown')
    authors = v1_record.get('authors', [])
    first_author = authors[0] if authors else None
    work_id = generate_work_id(publisher, clean_title, first_author)

    # Build format info
    format_info: Dict[str, Any] = {
        'format_type': convert_format(v1_record.get('format'))
    }

    isbn = parse_isbn(v1_record.get('isbn_or_issn'))
    if isbn:
        format_info['isbn'] = isbn

    if v1_record.get('source_url'):
        format_info['url'] = v1_record['source_url']
        format_info['url_status'] = 'unchecked'

    # Build edition info
    publication_year = v1_record.get('publication_year', 2020)
    edition_info: Dict[str, Any] = {
        'edition_number': edition_number,
        'publication_year': publication_year,
        'is_latest': True,  # Assume single edition records are the latest
        'formats': [format_info]
    }

    if edition_label:
        edition_info['edition_label'] = edition_label

    if v1_record.get('pages'):
        edition_info['pages'] = v1_record['pages']

    # Build v2 record
    v2_record: Dict[str, Any] = {
        'work_id': work_id,
        'title': clean_title,
        'language': v1_record.get('language', 'en'),
        'publisher': publisher,
        'segment': v1_record.get('segment', 'Other'),
        'subsegment': v1_record.get('subsegment', 'General'),
        'perspective': v1_record.get('perspective', 'practice'),
        'editions': [edition_info],
        'dataset_status': v1_record.get('dataset_status', 'draft'),
    }

    # Optional fields
    if v1_record.get('subtitle'):
        v2_record['subtitle'] = v1_record['subtitle']

    if authors:
        v2_record['authors'] = authors

    if v1_record.get('series'):
        v2_record['series'] = v1_record['series']

    if v1_record.get('topics'):
        v2_record['topics'] = v1_record['topics']

    if v1_record.get('instrument_or_asset_class'):
        v2_record['instrument_or_asset_class'] = v1_record['instrument_or_asset_class']

    if v1_record.get('jurisdiction'):
        v2_record['jurisdiction'] = v1_record['jurisdiction']

    if v1_record.get('audience_level'):
        v2_record['audience_level'] = v1_record['audience_level']

    if v1_record.get('official_status'):
        v2_record['official_status'] = v1_record['official_status']

    if v1_record.get('related_regulation'):
        v2_record['related_regulation'] = v1_record['related_regulation']

    if v1_record.get('qualification_target'):
        v2_record['qualification_target'] = v1_record['qualification_target']

    if v1_record.get('notes'):
        v2_record['notes'] = v1_record['notes']

    if v1_record.get('recommended_for'):
        v2_record['recommended_for'] = v1_record['recommended_for']

    # Data source handling
    data_sources = []
    if v1_record.get('data_source'):
        data_sources.append(v1_record['data_source'])
    if data_sources:
        v2_record['data_sources'] = data_sources

    # Timestamps
    now = datetime.utcnow().isoformat() + 'Z'
    v2_record['created_at'] = now
    v2_record['updated_at'] = now

    if v1_record.get('last_reviewed_at'):
        v2_record['last_reviewed_at'] = v1_record['last_reviewed_at']

    return v2_record


def merge_editions(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge records with the same work_id into a single record with multiple editions.
    """
    works: Dict[str, Dict[str, Any]] = {}

    for record in records:
        work_id = record['work_id']

        if work_id not in works:
            works[work_id] = record
        else:
            # Merge editions
            existing = works[work_id]
            new_edition = record['editions'][0]

            # Check if this edition already exists
            edition_exists = False
            for existing_edition in existing['editions']:
                if existing_edition['edition_number'] == new_edition['edition_number']:
                    # Merge formats
                    existing_formats = {f['format_type'] for f in existing_edition['formats']}
                    for fmt in new_edition['formats']:
                        if fmt['format_type'] not in existing_formats:
                            existing_edition['formats'].append(fmt)
                    edition_exists = True
                    break

            if not edition_exists:
                existing['editions'].append(new_edition)

            # Merge data_sources
            if 'data_sources' in record:
                if 'data_sources' not in existing:
                    existing['data_sources'] = []
                for src in record['data_sources']:
                    if src not in existing['data_sources']:
                        existing['data_sources'].append(src)

    # Sort editions by edition_number descending
    for work_id, record in works.items():
        record['editions'].sort(key=lambda e: e['edition_number'], reverse=True)

        # Set is_latest flag
        for i, edition in enumerate(record['editions']):
            edition['is_latest'] = (i == 0)

    return list(works.values())


def migrate_file(input_path: Path, output_path: Path, merge: bool = True) -> tuple[int, int]:
    """
    Migrate a single JSONL file from v1 to v2.
    Returns: (input_count, output_count)
    """
    v2_records = []
    input_count = 0

    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                v1_record = json.loads(line)
                v2_record = convert_record_v1_to_v2(v1_record)
                v2_records.append(v2_record)
                input_count += 1
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON: {e}")
            except Exception as e:
                logger.warning(f"Failed to convert record: {e}")

    if merge:
        v2_records = merge_editions(v2_records)

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        for record in v2_records:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')

    return input_count, len(v2_records)


def migrate_directory(
    input_dir: Path,
    output_dir: Path,
    pattern: str = '*.jsonl',
    merge: bool = True
) -> Dict[str, tuple[int, int]]:
    """
    Migrate all JSONL files in a directory.
    Returns: dict of filename -> (input_count, output_count)
    """
    results = {}

    for input_path in input_dir.glob(pattern):
        output_filename = input_path.stem + '_v2.jsonl'
        output_path = output_dir / output_filename

        logger.info(f"Migrating {input_path.name} -> {output_filename}")

        try:
            input_count, output_count = migrate_file(input_path, output_path, merge)
            results[input_path.name] = (input_count, output_count)
            logger.info(f"  {input_count} records -> {output_count} works")
        except Exception as e:
            logger.error(f"  Failed: {e}")
            results[input_path.name] = (0, 0)

    return results


def main():
    parser = argparse.ArgumentParser(description='Migrate v1 book records to v2 schema')
    parser.add_argument('input', type=Path, help='Input file or directory')
    parser.add_argument('output', type=Path, help='Output file or directory')
    parser.add_argument('--no-merge', action='store_true',
                        help='Do not merge records with same work_id')
    parser.add_argument('--pattern', default='*.jsonl',
                        help='File pattern for directory mode (default: *.jsonl)')

    args = parser.parse_args()

    merge = not args.no_merge

    if args.input.is_file():
        logger.info(f"Migrating single file: {args.input}")
        input_count, output_count = migrate_file(args.input, args.output, merge)
        logger.info(f"Migrated {input_count} records to {output_count} works")
        logger.info(f"Output written to: {args.output}")

    elif args.input.is_dir():
        logger.info(f"Migrating directory: {args.input}")
        results = migrate_directory(args.input, args.output, args.pattern, merge)

        total_input = sum(r[0] for r in results.values())
        total_output = sum(r[1] for r in results.values())

        logger.info(f"\nSummary:")
        logger.info(f"  Files processed: {len(results)}")
        logger.info(f"  Total v1 records: {total_input}")
        logger.info(f"  Total v2 works: {total_output}")
        logger.info(f"  Output directory: {args.output}")

    else:
        logger.error(f"Input path does not exist: {args.input}")
        sys.exit(1)


if __name__ == '__main__':
    main()
