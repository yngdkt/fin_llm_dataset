"""
Build SQLite database from JSONL files for efficient querying and analysis.

The JSONL files remain the source of truth (for Git-friendliness and schema flexibility).
SQLite is used as a derived view for:
- Complex queries (segment/subsegment/perspective analysis)
- Coverage reports
- Ad-hoc analysis with SQL
- Integration with tools like DuckDB, Metabase, etc.

Usage:
    python scripts/processors/build_db.py [--input data/] [--output data/books.db]
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Iterator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Schema version for tracking migrations
SCHEMA_VERSION = 2


def create_schema(conn: sqlite3.Connection) -> None:
    """Create database schema for v2 book records."""
    conn.executescript("""
        -- Schema metadata
        CREATE TABLE IF NOT EXISTS schema_info (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        -- Works (top-level book records)
        CREATE TABLE IF NOT EXISTS works (
            work_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            subtitle TEXT,
            language TEXT NOT NULL CHECK(language IN ('ja', 'en', 'other')),
            publisher TEXT NOT NULL,
            series TEXT,
            segment TEXT NOT NULL,
            subsegment TEXT NOT NULL,
            perspective TEXT NOT NULL CHECK(perspective IN (
                'law_regulation', 'theory', 'practice', 'case', 'trend', 'qualification'
            )),
            instrument_or_asset_class TEXT,
            jurisdiction TEXT,
            audience_level TEXT CHECK(audience_level IN (
                'introductory', 'intermediate', 'advanced', 'mixed', NULL
            )),
            official_status TEXT CHECK(official_status IN ('public', 'private', NULL)),
            related_regulation TEXT,
            qualification_target TEXT,
            notes TEXT,
            recommended_for TEXT,
            importance_score REAL,
            dataset_status TEXT NOT NULL DEFAULT 'draft' CHECK(dataset_status IN (
                'draft', 'validated', 'rejected'
            )),
            created_at TEXT,
            updated_at TEXT,
            last_reviewed_at TEXT,
            -- Store original JSON for reference
            raw_json TEXT
        );

        -- Authors (many-to-many with works)
        CREATE TABLE IF NOT EXISTS authors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_id TEXT NOT NULL REFERENCES works(work_id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            position INTEGER NOT NULL,  -- order in author list
            UNIQUE(work_id, position)
        );

        -- Editors (many-to-many with works)
        CREATE TABLE IF NOT EXISTS editors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_id TEXT NOT NULL REFERENCES works(work_id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            position INTEGER NOT NULL,
            UNIQUE(work_id, position)
        );

        -- Topics/keywords (many-to-many with works)
        CREATE TABLE IF NOT EXISTS topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_id TEXT NOT NULL REFERENCES works(work_id) ON DELETE CASCADE,
            topic TEXT NOT NULL,
            UNIQUE(work_id, topic)
        );

        -- Data sources (many-to-many with works)
        CREATE TABLE IF NOT EXISTS data_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_id TEXT NOT NULL REFERENCES works(work_id) ON DELETE CASCADE,
            source TEXT NOT NULL,
            UNIQUE(work_id, source)
        );

        -- Editions
        CREATE TABLE IF NOT EXISTS editions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_id TEXT NOT NULL REFERENCES works(work_id) ON DELETE CASCADE,
            edition_number INTEGER NOT NULL,
            edition_label TEXT,
            publication_year INTEGER NOT NULL,
            is_latest INTEGER NOT NULL DEFAULT 0,  -- boolean
            pages INTEGER,
            description TEXT,
            UNIQUE(work_id, edition_number)
        );

        -- Table of contents (per edition)
        CREATE TABLE IF NOT EXISTS table_of_contents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
            chapter_number INTEGER NOT NULL,
            chapter_title TEXT NOT NULL,
            UNIQUE(edition_id, chapter_number)
        );

        -- Formats (per edition)
        CREATE TABLE IF NOT EXISTS formats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            edition_id INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE,
            format_type TEXT NOT NULL CHECK(format_type IN (
                'hardcover', 'paperback', 'ebook', 'pdf', 'kindle',
                'audiobook', 'online_access', 'bundle', 'print_replica', 'other'
            )),
            isbn TEXT,
            asin TEXT,
            price_amount REAL,
            price_currency TEXT,
            price_type TEXT CHECK(price_type IN ('list', 'sale', 'rental', NULL)),
            url TEXT,
            url_status TEXT DEFAULT 'unchecked' CHECK(url_status IN (
                'valid', 'invalid', 'redirect', 'unchecked'
            )),
            url_verified_at TEXT,
            availability TEXT DEFAULT 'unknown' CHECK(availability IN (
                'available', 'out_of_stock', 'preorder', 'discontinued', 'unknown'
            )),
            UNIQUE(edition_id, format_type, isbn)
        );

        -- Indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_works_segment ON works(segment);
        CREATE INDEX IF NOT EXISTS idx_works_subsegment ON works(subsegment);
        CREATE INDEX IF NOT EXISTS idx_works_perspective ON works(perspective);
        CREATE INDEX IF NOT EXISTS idx_works_language ON works(language);
        CREATE INDEX IF NOT EXISTS idx_works_publisher ON works(publisher);
        CREATE INDEX IF NOT EXISTS idx_works_status ON works(dataset_status);
        CREATE INDEX IF NOT EXISTS idx_editions_year ON editions(publication_year);
        CREATE INDEX IF NOT EXISTS idx_editions_latest ON editions(is_latest);
        CREATE INDEX IF NOT EXISTS idx_formats_isbn ON formats(isbn);
        CREATE INDEX IF NOT EXISTS idx_authors_name ON authors(name);

        -- Useful views
        CREATE VIEW IF NOT EXISTS v_books_flat AS
        SELECT
            w.work_id,
            w.title,
            w.subtitle,
            GROUP_CONCAT(DISTINCT a.name) as authors,
            w.language,
            w.publisher,
            w.segment,
            w.subsegment,
            w.perspective,
            w.jurisdiction,
            w.audience_level,
            w.dataset_status,
            e.edition_number,
            e.publication_year,
            e.is_latest,
            e.pages,
            GROUP_CONCAT(DISTINCT f.isbn) as isbns,
            GROUP_CONCAT(DISTINCT f.format_type) as formats
        FROM works w
        LEFT JOIN authors a ON w.work_id = a.work_id
        LEFT JOIN editions e ON w.work_id = e.work_id AND e.is_latest = 1
        LEFT JOIN formats f ON e.id = f.edition_id
        GROUP BY w.work_id;

        -- Coverage analysis view
        CREATE VIEW IF NOT EXISTS v_coverage AS
        SELECT
            segment,
            subsegment,
            perspective,
            language,
            COUNT(DISTINCT work_id) as book_count,
            COUNT(DISTINCT CASE WHEN dataset_status = 'validated' THEN work_id END) as validated_count
        FROM works
        GROUP BY segment, subsegment, perspective, language;

        -- Recent publications view
        CREATE VIEW IF NOT EXISTS v_recent AS
        SELECT
            w.*,
            e.publication_year,
            e.edition_number
        FROM works w
        JOIN editions e ON w.work_id = e.work_id AND e.is_latest = 1
        WHERE e.publication_year >= 2020
        ORDER BY e.publication_year DESC;
    """)

    # Set schema version
    conn.execute(
        "INSERT OR REPLACE INTO schema_info (key, value) VALUES (?, ?)",
        ('version', str(SCHEMA_VERSION))
    )
    conn.execute(
        "INSERT OR REPLACE INTO schema_info (key, value) VALUES (?, ?)",
        ('updated_at', datetime.utcnow().isoformat())
    )
    conn.commit()


def insert_work(conn: sqlite3.Connection, record: Dict[str, Any]) -> None:
    """Insert a single v2 book record into the database."""
    work_id = record.get('work_id')
    if not work_id:
        return

    # Insert main work record
    conn.execute("""
        INSERT OR REPLACE INTO works (
            work_id, title, subtitle, language, publisher, series,
            segment, subsegment, perspective, instrument_or_asset_class,
            jurisdiction, audience_level, official_status, related_regulation,
            qualification_target, notes, recommended_for, importance_score,
            dataset_status, created_at, updated_at, last_reviewed_at, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        work_id,
        record.get('title'),
        record.get('subtitle'),
        record.get('language'),
        record.get('publisher'),
        record.get('series'),
        record.get('segment'),
        record.get('subsegment'),
        record.get('perspective'),
        record.get('instrument_or_asset_class'),
        record.get('jurisdiction'),
        record.get('audience_level'),
        record.get('official_status'),
        record.get('related_regulation'),
        record.get('qualification_target'),
        record.get('notes'),
        record.get('recommended_for'),
        record.get('importance_score'),
        record.get('dataset_status', 'draft'),
        record.get('created_at'),
        record.get('updated_at'),
        record.get('last_reviewed_at'),
        json.dumps(record, ensure_ascii=False),
    ))

    # Delete existing related records (for updates)
    conn.execute("DELETE FROM authors WHERE work_id = ?", (work_id,))
    conn.execute("DELETE FROM editors WHERE work_id = ?", (work_id,))
    conn.execute("DELETE FROM topics WHERE work_id = ?", (work_id,))
    conn.execute("DELETE FROM data_sources WHERE work_id = ?", (work_id,))

    # Insert authors
    for i, author in enumerate(record.get('authors', [])):
        conn.execute(
            "INSERT INTO authors (work_id, name, position) VALUES (?, ?, ?)",
            (work_id, author, i)
        )

    # Insert editors
    for i, editor in enumerate(record.get('editors', [])):
        conn.execute(
            "INSERT INTO editors (work_id, name, position) VALUES (?, ?, ?)",
            (work_id, editor, i)
        )

    # Insert topics
    for topic in record.get('topics', []):
        conn.execute(
            "INSERT OR IGNORE INTO topics (work_id, topic) VALUES (?, ?)",
            (work_id, topic)
        )

    # Insert data sources
    for source in record.get('data_sources', []):
        conn.execute(
            "INSERT OR IGNORE INTO data_sources (work_id, source) VALUES (?, ?)",
            (work_id, source)
        )

    # Delete existing editions (cascade deletes formats and toc)
    conn.execute("DELETE FROM editions WHERE work_id = ?", (work_id,))

    # Insert editions and formats
    for edition in record.get('editions', []):
        cursor = conn.execute("""
            INSERT INTO editions (
                work_id, edition_number, edition_label, publication_year,
                is_latest, pages, description
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            work_id,
            edition.get('edition_number'),
            edition.get('edition_label'),
            edition.get('publication_year'),
            1 if edition.get('is_latest') else 0,
            edition.get('pages'),
            edition.get('description'),
        ))
        edition_id = cursor.lastrowid

        # Insert table of contents
        for i, chapter in enumerate(edition.get('table_of_contents', [])):
            conn.execute(
                "INSERT INTO table_of_contents (edition_id, chapter_number, chapter_title) VALUES (?, ?, ?)",
                (edition_id, i + 1, chapter)
            )

        # Insert formats
        for fmt in edition.get('formats', []):
            price = fmt.get('price', {})
            conn.execute("""
                INSERT INTO formats (
                    edition_id, format_type, isbn, asin,
                    price_amount, price_currency, price_type,
                    url, url_status, url_verified_at, availability
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                edition_id,
                fmt.get('format_type'),
                fmt.get('isbn'),
                fmt.get('asin'),
                price.get('amount') if price else None,
                price.get('currency') if price else None,
                price.get('price_type') if price else None,
                fmt.get('url'),
                fmt.get('url_status', 'unchecked'),
                fmt.get('url_verified_at'),
                fmt.get('availability', 'unknown'),
            ))


def load_jsonl_files(input_path: Path, pattern: str = "*.jsonl") -> Iterator[Dict[str, Any]]:
    """Load records from JSONL files."""
    if input_path.is_file():
        files = [input_path]
    else:
        files = list(input_path.glob(pattern))

    for file_path in files:
        logger.info(f"Loading {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON in {file_path}: {e}")


def is_v2_record(record: Dict[str, Any]) -> bool:
    """Check if a record is v2 format (has editions array)."""
    return 'editions' in record and isinstance(record.get('editions'), list)


def build_database(
    input_path: Path,
    output_path: Path,
    pattern: str = "*.jsonl"
) -> Dict[str, int]:
    """
    Build SQLite database from JSONL files.

    Args:
        input_path: Input file or directory
        output_path: Output SQLite database file
        pattern: Glob pattern for finding JSONL files

    Returns:
        Statistics dict with counts
    """
    # Remove existing database
    if output_path.exists():
        output_path.unlink()

    conn = sqlite3.Connection(output_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")

    try:
        create_schema(conn)

        stats = {
            'total_records': 0,
            'v2_records': 0,
            'v1_skipped': 0,
            'errors': 0,
        }

        for record in load_jsonl_files(input_path, pattern):
            stats['total_records'] += 1

            if not is_v2_record(record):
                stats['v1_skipped'] += 1
                continue

            try:
                insert_work(conn, record)
                stats['v2_records'] += 1
            except Exception as e:
                logger.warning(f"Failed to insert record {record.get('work_id')}: {e}")
                stats['errors'] += 1

            # Commit every 1000 records
            if stats['v2_records'] % 1000 == 0:
                conn.commit()
                logger.info(f"Processed {stats['v2_records']} records...")

        conn.commit()

        # Optimize database
        conn.execute("ANALYZE")
        conn.execute("VACUUM")

        return stats

    finally:
        conn.close()


def print_summary(conn: sqlite3.Connection) -> None:
    """Print database summary statistics."""
    print("\n" + "=" * 60)
    print("Database Summary")
    print("=" * 60)

    # Basic counts
    for table in ['works', 'editions', 'formats', 'authors']:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"{table}: {count:,}")

    # Coverage by segment
    print("\nBy Segment:")
    rows = conn.execute("""
        SELECT segment, COUNT(*) as count
        FROM works
        GROUP BY segment
        ORDER BY count DESC
    """).fetchall()
    for segment, count in rows:
        print(f"  {segment}: {count:,}")

    # Coverage by perspective
    print("\nBy Perspective:")
    rows = conn.execute("""
        SELECT perspective, COUNT(*) as count
        FROM works
        GROUP BY perspective
        ORDER BY count DESC
    """).fetchall()
    for perspective, count in rows:
        print(f"  {perspective}: {count:,}")

    # By language
    print("\nBy Language:")
    rows = conn.execute("""
        SELECT language, COUNT(*) as count
        FROM works
        GROUP BY language
        ORDER BY count DESC
    """).fetchall()
    for lang, count in rows:
        print(f"  {lang}: {count:,}")


def main():
    parser = argparse.ArgumentParser(
        description='Build SQLite database from JSONL book records'
    )
    parser.add_argument(
        '--input', '-i',
        type=Path,
        default=Path('data/processed'),
        help='Input directory or file (default: data/processed)'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=Path('data/books.db'),
        help='Output SQLite database (default: data/books.db)'
    )
    parser.add_argument(
        '--pattern', '-p',
        default='*_v2.jsonl',
        help='Glob pattern for JSONL files (default: *_v2.jsonl)'
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Print summary after building'
    )

    args = parser.parse_args()

    logger.info(f"Building database from {args.input} -> {args.output}")

    stats = build_database(args.input, args.output, args.pattern)

    print("\n" + "=" * 60)
    print("Build Complete")
    print("=" * 60)
    print(f"Total records processed: {stats['total_records']:,}")
    print(f"V2 records imported: {stats['v2_records']:,}")
    print(f"V1 records skipped: {stats['v1_skipped']:,}")
    print(f"Errors: {stats['errors']:,}")
    print(f"Database: {args.output}")

    if args.summary:
        conn = sqlite3.connect(args.output)
        print_summary(conn)
        conn.close()


if __name__ == '__main__':
    main()
