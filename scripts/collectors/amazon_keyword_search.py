#!/usr/bin/env python3
"""
Amazon keyword search script for financial books.

Searches Amazon.co.jp (for Japanese keywords) and Amazon.com (for English keywords)
based on segment_topics.jsonl, filters out beginner/introductory books,
and adds top 10 results per keyword to master.

Usage:
    # Dry-run (show what would be done)
    python scripts/collectors/amazon_keyword_search.py --dry-run

    # Process all keywords
    python scripts/collectors/amazon_keyword_search.py

    # Process specific segment
    python scripts/collectors/amazon_keyword_search.py --segment-num 1

    # Resume from last state
    python scripts/collectors/amazon_keyword_search.py --resume

    # Reset progress and start fresh
    python scripts/collectors/amazon_keyword_search.py --reset
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Set, Tuple
from enum import Enum

try:
    import requests
    from bs4 import BeautifulSoup
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

# Import book matching utilities
try:
    from scripts.processors.book_matcher import BookIndex, BookMatcher, TitleNormalizer
    MATCHER_AVAILABLE = True
except ImportError:
    MATCHER_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent.parent
MASTER_FILE = BASE_DIR / "data" / "master" / "books.jsonl"
TOPICS_FILE = BASE_DIR / "data" / "config" / "segment_topics.jsonl"
STATE_FILE = BASE_DIR / "data" / "master" / ".amazon_search_state.json"
SEARCH_LOG_FILE = BASE_DIR / "data" / "master" / "amazon_search_log.jsonl"

# Patterns to identify beginner/introductory books (to exclude)
BEGINNER_PATTERNS_JA = [
    r'入門',
    r'はじめて',
    r'初めて',
    r'ゼロから',
    r'基礎から',
    r'わかる',
    r'やさしい',
    r'かんたん',
    r'簡単',
    r'初心者',
    r'ビギナー',
    r'超入門',
    r'よくわかる',
    r'すぐわかる',
    r'図解',
    r'マンガ',
    r'まんが',
    r'漫画',
    r'イラスト',
    r'1時間で',
    r'1日で',
    r'一日で',
    r'週末で',
    r'すぐできる',
    r'これだけ',
    r'いちばんやさしい',
    r'世界一やさしい',
]

BEGINNER_PATTERNS_EN = [
    r'(?i)for\s+dummies',
    r'(?i)for\s+beginners',
    r'(?i)beginner',
    r'(?i)introduction\s+to',
    r'(?i)intro\s+to',
    r'(?i)getting\s+started',
    r'(?i)step\s+by\s+step',
    r'(?i)made\s+simple',
    r'(?i)made\s+easy',
    r'(?i)simplified',
    r'(?i)basics',
    r'(?i)fundamentals',
    r'(?i)primer',
    r'(?i)crash\s+course',
    r'(?i)quick\s+start',
    r'(?i)in\s+24\s+hours',
    r'(?i)in\s+a\s+week',
    r'(?i)illustrated',
    r'(?i)visual\s+guide',
    r'(?i)complete\s+idiot',
]


class KeywordStatus(Enum):
    """Status of keyword processing."""
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class SearchState:
    """State of the search process."""
    started_at: str = ""
    last_updated: str = ""
    keywords_processed: Dict[str, str] = field(default_factory=dict)  # keyword -> status
    total_books_added: int = 0
    total_keywords: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SearchState':
        return cls(
            started_at=data.get('started_at', ''),
            last_updated=data.get('last_updated', ''),
            keywords_processed=data.get('keywords_processed', {}),
            total_books_added=data.get('total_books_added', 0),
            total_keywords=data.get('total_keywords', 0)
        )


@dataclass
class BookResult:
    """A book result from Amazon search."""
    title: str
    authors: List[str]
    asin: str
    url: str
    price: Optional[float] = None
    currency: str = "JPY"
    publication_year: Optional[int] = None
    publisher: Optional[str] = None
    language: str = "ja"
    description: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None

    def to_master_record(
        self,
        segment: str,
        subsegment: str,
        topic: str,
        keyword: str
    ) -> Dict[str, Any]:
        """Convert to master record format."""
        # Generate work_id from ASIN
        work_id = f"amz_{self.asin}"

        record = {
            "work_id": work_id,
            "title": self.title,
            "language": self.language,
            "segment": segment,
            "subsegment": subsegment,
            "perspective": "practice",  # Default to practice
            "editions": [
                {
                    "edition_number": 1,
                    "is_latest": True,
                    "formats": [
                        {
                            "format_type": "other",
                            "asin": self.asin,
                            "url": self.url,
                            "url_status": "unchecked"
                        }
                    ]
                }
            ],
            "dataset_status": "draft",
            "authors": self.authors,
            "topics": [keyword],
            "created_at": datetime.now().isoformat() + "Z",
            "updated_at": datetime.now().isoformat() + "Z",
            "data_sources": ["amazon_search"]
        }

        # Add optional fields
        if self.publisher:
            record["publisher"] = self.publisher

        if self.price:
            record["editions"][0]["formats"][0]["price"] = {
                "amount": self.price,
                "currency": self.currency,
                "price_type": "list"
            }

        if self.publication_year:
            record["editions"][0]["publication_year"] = self.publication_year

        if self.description:
            record["editions"][0]["description"] = self.description

        return record


class AmazonSearcher:
    """Search Amazon for books."""

    def __init__(self, delay: float = 3.0):
        self.delay = delay
        self.last_request = 0.0
        self.session = requests.Session() if REQUESTS_AVAILABLE else None

        if self.session:
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            })

    def _wait(self):
        """Rate limiting with jitter."""
        elapsed = time.time() - self.last_request
        wait_time = self.delay + random.uniform(0.5, 2.0)  # Add jitter
        if elapsed < wait_time:
            time.sleep(wait_time - elapsed)
        self.last_request = time.time()

    def search_amazon_jp(
        self,
        keyword: str,
        max_results: int = 20
    ) -> List[BookResult]:
        """Search Amazon.co.jp for Japanese keyword."""
        if not self.session:
            logger.warning("requests not available")
            return []

        self._wait()

        # Search URL for books category
        url = "https://www.amazon.co.jp/s"
        params = {
            'k': keyword,
            'i': 'stripbooks',  # Books category
            'rh': 'n:465392',   # Books node
            's': 'relevanceblender',
        }

        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()

            return self._parse_amazon_jp_results(response.text, max_results)

        except Exception as e:
            logger.warning(f"Amazon.co.jp search failed for '{keyword}': {e}")
            return []

    def search_amazon_com(
        self,
        keyword: str,
        max_results: int = 20
    ) -> List[BookResult]:
        """Search Amazon.com for English keyword."""
        if not self.session:
            logger.warning("requests not available")
            return []

        self._wait()

        # Update headers for US site
        self.session.headers.update({
            'Accept-Language': 'en-US,en;q=0.9',
        })

        url = "https://www.amazon.com/s"
        params = {
            'k': keyword,
            'i': 'stripbooks',
            'rh': 'n:283155',  # Books node
            's': 'relevanceblender',
        }

        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()

            return self._parse_amazon_com_results(response.text, max_results)

        except Exception as e:
            logger.warning(f"Amazon.com search failed for '{keyword}': {e}")
            return []

    def _parse_amazon_jp_results(
        self,
        html: str,
        max_results: int
    ) -> List[BookResult]:
        """Parse Amazon.co.jp search results."""
        results = []
        soup = BeautifulSoup(html, 'html.parser')

        # Find book items
        items = soup.select('[data-component-type="s-search-result"]')

        for item in items[:max_results * 2]:  # Get more to filter
            try:
                result = self._extract_book_jp(item)
                if result:
                    results.append(result)
                    if len(results) >= max_results:
                        break
            except Exception as e:
                logger.debug(f"Failed to parse item: {e}")
                continue

        return results

    def _parse_amazon_com_results(
        self,
        html: str,
        max_results: int
    ) -> List[BookResult]:
        """Parse Amazon.com search results."""
        results = []
        soup = BeautifulSoup(html, 'html.parser')

        items = soup.select('[data-component-type="s-search-result"]')

        for item in items[:max_results * 2]:
            try:
                result = self._extract_book_com(item)
                if result:
                    results.append(result)
                    if len(results) >= max_results:
                        break
            except Exception as e:
                logger.debug(f"Failed to parse item: {e}")
                continue

        return results

    def _extract_book_jp(self, item) -> Optional[BookResult]:
        """Extract book info from Amazon.co.jp item."""
        # Get ASIN
        asin = item.get('data-asin', '')
        if not asin:
            return None

        # Get title
        title_elem = item.select_one('h2 a span')
        if not title_elem:
            return None
        title = title_elem.get_text(strip=True)

        # Get URL
        link_elem = item.select_one('h2 a')
        url = f"https://www.amazon.co.jp/dp/{asin}" if link_elem else ""

        # Get authors
        author_elem = item.select_one('.a-row.a-size-base.a-color-secondary')
        authors = []
        if author_elem:
            author_text = author_elem.get_text(strip=True)
            # Clean up author text (remove "著", etc.)
            author_text = re.sub(r'[\|｜].*$', '', author_text)
            if author_text:
                authors = [a.strip() for a in re.split(r'[,、]', author_text) if a.strip()]

        # Get price
        price = None
        price_elem = item.select_one('.a-price .a-offscreen')
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            price_match = re.search(r'[\d,]+', price_text.replace(',', ''))
            if price_match:
                price = float(price_match.group().replace(',', ''))

        return BookResult(
            title=title,
            authors=authors,
            asin=asin,
            url=url,
            price=price,
            currency="JPY",
            language="ja"
        )

    def _extract_book_com(self, item) -> Optional[BookResult]:
        """Extract book info from Amazon.com item."""
        asin = item.get('data-asin', '')
        if not asin:
            return None

        title_elem = item.select_one('h2 a span')
        if not title_elem:
            return None
        title = title_elem.get_text(strip=True)

        url = f"https://www.amazon.com/dp/{asin}"

        author_elem = item.select_one('.a-row.a-size-base.a-color-secondary')
        authors = []
        if author_elem:
            author_text = author_elem.get_text(strip=True)
            author_text = re.sub(r'\|.*$', '', author_text)
            if author_text and 'by' not in author_text.lower():
                authors = [a.strip() for a in re.split(r'[,]', author_text) if a.strip()]

        price = None
        price_elem = item.select_one('.a-price .a-offscreen')
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            price_match = re.search(r'[\d.]+', price_text.replace(',', ''))
            if price_match:
                price = float(price_match.group())

        return BookResult(
            title=title,
            authors=authors,
            asin=asin,
            url=url,
            price=price,
            currency="USD",
            language="en"
        )


def is_beginner_book(title: str, language: str = "ja") -> bool:
    """Check if a book is for beginners/introductory."""
    patterns = BEGINNER_PATTERNS_JA if language == "ja" else BEGINNER_PATTERNS_EN

    for pattern in patterns:
        if re.search(pattern, title):
            return True
    return False


def filter_non_beginner_books(
    books: List[BookResult],
    language: str = "ja",
    max_count: int = 10
) -> List[BookResult]:
    """Filter out beginner books and return top N."""
    filtered = []

    for book in books:
        if not is_beginner_book(book.title, language):
            filtered.append(book)
            if len(filtered) >= max_count:
                break

    return filtered


def load_topics(path: Path) -> List[Dict[str, Any]]:
    """Load topics from JSONL file."""
    topics = []
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        topics.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return topics


def load_master(path: Path) -> List[Dict[str, Any]]:
    """Load master records."""
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


def save_master(records: List[Dict[str, Any]], path: Path) -> None:
    """Save master records."""
    with open(path, 'w', encoding='utf-8') as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')


def load_state(path: Path) -> SearchState:
    """Load search state."""
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return SearchState.from_dict(data)
    return SearchState(started_at=datetime.now().isoformat())


def save_state(state: SearchState, path: Path) -> None:
    """Save search state."""
    state.last_updated = datetime.now().isoformat()
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(state.to_dict(), f, ensure_ascii=False, indent=2)


def append_search_log(
    keyword: str,
    language: str,
    segment: str,
    subsegment: str,
    books_found: int,
    books_added: int,
    path: Path
) -> None:
    """Append entry to search log."""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "keyword": keyword,
        "language": language,
        "segment": segment,
        "subsegment": subsegment,
        "books_found": books_found,
        "books_added": books_added
    }
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(entry, ensure_ascii=False) + '\n')


def get_existing_asins(master_records: List[Dict[str, Any]]) -> Set[str]:
    """Get set of existing ASINs from master."""
    asins = set()
    for record in master_records:
        for edition in record.get('editions', []):
            for fmt in edition.get('formats', []):
                if fmt.get('asin'):
                    asins.add(fmt['asin'])
                # Also check URL for ASIN
                url = fmt.get('url', '')
                asin_match = re.search(r'/dp/([A-Z0-9]{10})', url)
                if asin_match:
                    asins.add(asin_match.group(1))
    return asins


class DuplicateChecker:
    """
    Check for duplicate books using multiple strategies:
    1. ASIN exact match
    2. ISBN exact match
    3. Title similarity (using BookIndex)
    """

    def __init__(self, master_records: List[Dict[str, Any]]):
        self.asins: Set[str] = set()
        self.isbns: Set[str] = set()
        self.book_index: Optional[Any] = None

        # Build ASIN/ISBN sets
        for record in master_records:
            for edition in record.get('editions', []):
                for fmt in edition.get('formats', []):
                    # Collect ASINs
                    if fmt.get('asin'):
                        self.asins.add(fmt['asin'])
                    # Extract ASIN from URL
                    url = fmt.get('url', '')
                    asin_match = re.search(r'/dp/([A-Z0-9]{10})', url)
                    if asin_match:
                        self.asins.add(asin_match.group(1))
                    # Collect ISBNs
                    if fmt.get('isbn'):
                        isbn = re.sub(r'[-\s]', '', fmt['isbn'])
                        self.isbns.add(isbn)

        # Build BookIndex for fuzzy matching
        if MATCHER_AVAILABLE and master_records:
            logger.info("Building book index for fuzzy matching...")
            self.book_index = BookIndex(master_records)
            logger.info(f"  Index stats: {self.book_index.stats()}")

    def is_duplicate(
        self,
        book: BookResult,
        fuzzy_threshold: float = 0.85
    ) -> Tuple[bool, str, Optional[float]]:
        """
        Check if a book is a duplicate.

        Returns:
            Tuple of (is_duplicate, reason, confidence)
        """
        # Check 1: ASIN exact match
        if book.asin in self.asins:
            return (True, 'asin_exact', 1.0)

        # Check 2: ISBN match (for books where ASIN = ISBN-10)
        # Amazon book ASINs are often ISBN-10
        if book.asin in self.isbns:
            return (True, 'isbn_exact', 1.0)

        # Check 3: Fuzzy title matching
        if self.book_index:
            # Convert BookResult to dict for matching
            book_dict = {
                'title': book.title,
                'authors': book.authors,
                'editions': [{
                    'formats': [{
                        'asin': book.asin
                    }]
                }]
            }

            match_result = self.book_index.find_match(book_dict, fuzzy_threshold)
            if match_result:
                matched_book, result = match_result
                return (True, result.match_type, result.confidence)

        return (False, '', None)

    def add_book(self, book: BookResult) -> None:
        """Add a new book to the checker (for tracking newly added books)."""
        self.asins.add(book.asin)

        # Also add to book index if available
        if self.book_index:
            book_dict = {
                'title': book.title,
                'authors': book.authors,
                'work_id': f'amz_{book.asin}',
                'editions': [{
                    'formats': [{
                        'asin': book.asin
                    }]
                }]
            }
            self.book_index.add(book_dict)


def generate_keyword_id(keyword: str, language: str) -> str:
    """Generate unique ID for a keyword."""
    key = f"{language}:{keyword}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


def main():
    parser = argparse.ArgumentParser(
        description='Search Amazon for financial books by keyword'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    parser.add_argument(
        '--segment-num',
        type=int,
        help='Only process specific segment number'
    )
    parser.add_argument(
        '--subsegment-num',
        type=str,
        help='Only process specific subsegment (e.g., "1-1")'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from last saved state'
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Reset progress and start fresh'
    )
    parser.add_argument(
        '--max-per-keyword',
        type=int,
        default=10,
        help='Maximum books to add per keyword (default: 10)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=3.0,
        help='Delay between requests in seconds (default: 3.0)'
    )
    parser.add_argument(
        '--master',
        type=Path,
        default=MASTER_FILE,
        help=f'Master file path (default: {MASTER_FILE})'
    )
    parser.add_argument(
        '--topics',
        type=Path,
        default=TOPICS_FILE,
        help=f'Topics file path (default: {TOPICS_FILE})'
    )
    parser.add_argument(
        '--ja-only',
        action='store_true',
        help='Only process Japanese keywords'
    )
    parser.add_argument(
        '--en-only',
        action='store_true',
        help='Only process English keywords'
    )
    parser.add_argument(
        '--max-keywords',
        type=int,
        help='Maximum number of keywords to process'
    )

    args = parser.parse_args()

    if not REQUESTS_AVAILABLE:
        logger.error("requests and beautifulsoup4 are required. Install with: pip install requests beautifulsoup4")
        sys.exit(1)

    # Load topics
    logger.info(f"Loading topics from {args.topics}")
    topics = load_topics(args.topics)
    logger.info(f"  Loaded {len(topics)} topic records")

    # Load or reset state
    if args.reset:
        state = SearchState(started_at=datetime.now().isoformat())
        logger.info("Reset progress - starting fresh")
    elif args.resume:
        state = load_state(STATE_FILE)
        logger.info(f"Resuming from saved state")
        logger.info(f"  Keywords processed: {len(state.keywords_processed)}")
        logger.info(f"  Books added so far: {state.total_books_added}")
    else:
        state = SearchState(started_at=datetime.now().isoformat())

    # Load master
    logger.info(f"Loading master from {args.master}")
    master_records = load_master(args.master)
    logger.info(f"  Loaded {len(master_records)} records")

    # Initialize duplicate checker (uses ASIN, ISBN, and fuzzy title matching)
    logger.info("Initializing duplicate checker...")
    dup_checker = DuplicateChecker(master_records)
    logger.info(f"  Existing ASINs: {len(dup_checker.asins)}")
    logger.info(f"  Existing ISBNs: {len(dup_checker.isbns)}")
    if not MATCHER_AVAILABLE:
        logger.warning("  book_matcher not available - fuzzy matching disabled")

    # Build keyword list
    keywords_to_process = []

    for topic in topics:
        segment_num = topic.get('segment_num')
        subsegment_num = topic.get('subsegment_num')
        segment = topic.get('segment', '')
        subsegment = topic.get('subsegment', '')

        # Filter by segment/subsegment if specified
        if args.segment_num and segment_num != args.segment_num:
            continue
        if args.subsegment_num and subsegment_num != args.subsegment_num:
            continue

        # Japanese keywords
        if not args.en_only:
            for keyword in topic.get('keywords_ja', []):
                keyword_id = generate_keyword_id(keyword, 'ja')
                if keyword_id not in state.keywords_processed or state.keywords_processed[keyword_id] != 'completed':
                    keywords_to_process.append({
                        'keyword': keyword,
                        'keyword_id': keyword_id,
                        'language': 'ja',
                        'segment': segment,
                        'subsegment': subsegment,
                        'topic': topic.get('topic', '')
                    })

        # English keywords
        if not args.ja_only:
            for keyword in topic.get('keywords_en', []):
                keyword_id = generate_keyword_id(keyword, 'en')
                if keyword_id not in state.keywords_processed or state.keywords_processed[keyword_id] != 'completed':
                    keywords_to_process.append({
                        'keyword': keyword,
                        'keyword_id': keyword_id,
                        'language': 'en',
                        'segment': segment,
                        'subsegment': subsegment,
                        'topic': topic.get('topic', '')
                    })

    state.total_keywords = len(keywords_to_process) + len([
        k for k, v in state.keywords_processed.items() if v == 'completed'
    ])

    logger.info(f"\nKeywords to process: {len(keywords_to_process)}")

    if args.max_keywords:
        keywords_to_process = keywords_to_process[:args.max_keywords]
        logger.info(f"  Limited to {args.max_keywords} keywords")

    if not keywords_to_process:
        logger.info("No keywords to process")
        return

    # Initialize searcher
    searcher = AmazonSearcher(delay=args.delay)

    # Statistics
    stats = {
        'keywords_processed': 0,
        'books_found': 0,
        'books_added': 0,
        'books_filtered': 0,
        'books_duplicate': 0,
        'errors': 0
    }

    new_records = []

    try:
        for i, kw_info in enumerate(keywords_to_process):
            keyword = kw_info['keyword']
            keyword_id = kw_info['keyword_id']
            language = kw_info['language']
            segment = kw_info['segment']
            subsegment = kw_info['subsegment']
            topic = kw_info['topic']

            logger.info(f"\n[{i+1}/{len(keywords_to_process)}] Searching: '{keyword}' ({language})")
            logger.info(f"  Segment: {segment} / {subsegment}")

            try:
                # Search Amazon
                if language == 'ja':
                    results = searcher.search_amazon_jp(keyword, max_results=30)
                else:
                    results = searcher.search_amazon_com(keyword, max_results=30)

                logger.info(f"  Found: {len(results)} books")
                stats['books_found'] += len(results)

                # Filter out beginner books
                filtered = filter_non_beginner_books(
                    results,
                    language=language,
                    max_count=args.max_per_keyword * 2  # Get extra for duplicates
                )

                filtered_count = len(results) - len(filtered)
                stats['books_filtered'] += filtered_count
                logger.info(f"  After filtering: {len(filtered)} books (filtered {filtered_count} beginner books)")

                # Add to master (avoiding duplicates)
                added_count = 0
                for book in filtered:
                    # Check for duplicates using multi-tier strategy
                    is_dup, dup_reason, dup_score = dup_checker.is_duplicate(book)
                    if is_dup:
                        stats['books_duplicate'] += 1
                        # Track duplicate type for detailed stats
                        if 'dup_by_type' not in stats:
                            stats['dup_by_type'] = {'asin': 0, 'isbn': 0, 'fuzzy': 0}
                        if 'asin' in dup_reason:
                            stats['dup_by_type']['asin'] += 1
                        elif 'isbn' in dup_reason:
                            stats['dup_by_type']['isbn'] += 1
                        elif 'fuzzy' in dup_reason or 'title' in dup_reason:
                            stats['dup_by_type']['fuzzy'] += 1
                            logger.debug(f"    Fuzzy match: {book.title} (score: {dup_score:.2f})")
                        continue

                    if added_count >= args.max_per_keyword:
                        break

                    if not args.dry_run:
                        record = book.to_master_record(
                            segment=segment,
                            subsegment=subsegment,
                            topic=topic,
                            keyword=keyword
                        )
                        new_records.append(record)
                        # Add to duplicate checker for tracking new additions
                        dup_checker.add_book(book)

                    added_count += 1
                    stats['books_added'] += 1

                logger.info(f"  Added: {added_count} new books")

                # Update state
                state.keywords_processed[keyword_id] = 'completed'
                state.total_books_added += added_count
                stats['keywords_processed'] += 1

                # Log search
                if not args.dry_run:
                    append_search_log(
                        keyword=keyword,
                        language=language,
                        segment=segment,
                        subsegment=subsegment,
                        books_found=len(results),
                        books_added=added_count,
                        path=SEARCH_LOG_FILE
                    )

            except Exception as e:
                logger.error(f"  Error: {e}")
                state.keywords_processed[keyword_id] = 'failed'
                stats['errors'] += 1

            # Save state periodically
            if (i + 1) % 10 == 0 and not args.dry_run:
                save_state(state, STATE_FILE)
                logger.info("  (State saved)")

    except KeyboardInterrupt:
        logger.info("\n\nInterrupted by user")

    # Final save
    if not args.dry_run:
        # Append new records to master
        if new_records:
            master_records.extend(new_records)
            save_master(master_records, args.master)
            logger.info(f"\nSaved master ({len(master_records)} total records)")

        # Save final state
        save_state(state, STATE_FILE)
        logger.info(f"Saved state to {STATE_FILE}")

    # Summary
    print(f"\n{'=' * 60}")
    print("Amazon Keyword Search Summary")
    print(f"{'=' * 60}")
    print(f"  Keywords processed: {stats['keywords_processed']}")
    print(f"  Books found: {stats['books_found']}")
    print(f"  Books filtered (beginner): {stats['books_filtered']}")
    print(f"  Books duplicate: {stats['books_duplicate']}")
    # Show duplicate breakdown if available
    if 'dup_by_type' in stats:
        dup_types = stats['dup_by_type']
        print(f"    - ASIN exact match: {dup_types.get('asin', 0)}")
        print(f"    - ISBN exact match: {dup_types.get('isbn', 0)}")
        print(f"    - Fuzzy title match: {dup_types.get('fuzzy', 0)}")
    print(f"  Books added: {stats['books_added']}")
    print(f"  Errors: {stats['errors']}")

    if args.dry_run:
        print("\n[DRY-RUN] No changes were made")
    else:
        print(f"\n  Master records: {len(master_records)}")

    # Show progress
    completed = len([k for k, v in state.keywords_processed.items() if v == 'completed'])
    total = state.total_keywords
    if total > 0:
        progress = completed / total * 100
        print(f"\n  Overall progress: {completed}/{total} ({progress:.1f}%)")


if __name__ == '__main__':
    main()
