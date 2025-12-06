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

# Ensure project root is in path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

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

# Import unified content filter
try:
    from scripts.filters import (
        is_beginner_book,
        is_irrelevant_content,
        should_exclude_book,
        filter_relevant_books,
    )
    FILTER_AVAILABLE = True
except ImportError:
    FILTER_AVAILABLE = False

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


class AmazonSearchError(Exception):
    """Custom exception for Amazon search errors."""
    def __init__(self, message: str, is_retryable: bool = False):
        super().__init__(message)
        self.is_retryable = is_retryable


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
            "data_sources": [
                {
                    "source": "amazon_search",
                    "keyword": keyword,
                    "searched_at": datetime.now().isoformat() + "Z"
                }
            ]
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

    # User agent rotation list
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ]

    def __init__(self, delay: float = 8.0, fresh_session: bool = False):
        self.delay = delay
        self.last_request = 0.0
        self.consecutive_errors = 0
        self.fresh_session = fresh_session
        self.request_count = 0
        self._create_session()

    def _create_session(self):
        """Create a new session with random user agent."""
        if not REQUESTS_AVAILABLE:
            self.session = None
            return

        self.session = requests.Session()
        user_agent = random.choice(self.USER_AGENTS)
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        logger.debug(f"Created new session with UA: {user_agent[:50]}...")

    def _maybe_refresh_session(self):
        """Refresh session if fresh_session mode is enabled or after errors."""
        if self.fresh_session or self.consecutive_errors > 0:
            logger.info("  Refreshing session...")
            self._create_session()
            self.consecutive_errors = 0

    def _wait(self, extra_delay: float = 0.0):
        """Rate limiting with jitter and backoff for errors."""
        elapsed = time.time() - self.last_request
        # Base delay + jitter (8-12 seconds) + extra delay for backoff
        wait_time = self.delay + random.uniform(2.0, 4.0) + extra_delay
        if elapsed < wait_time:
            time.sleep(wait_time - elapsed)
        self.last_request = time.time()

    def _backoff_wait(self):
        """Additional wait after error with exponential backoff."""
        backoff = min(30.0, 5.0 * (2 ** self.consecutive_errors))
        logger.info(f"  Backing off for {backoff:.0f} seconds...")
        time.sleep(backoff)

    def search_amazon_jp(
        self,
        keyword: str,
        max_results: int = 20
    ) -> List[BookResult]:
        """Search Amazon.co.jp for Japanese keyword."""
        if not REQUESTS_AVAILABLE:
            logger.warning("requests not available")
            return []

        # Refresh session if needed (fresh_session mode or after errors)
        self._maybe_refresh_session()
        self._wait()

        # Update headers for JP site
        self.session.headers.update({
            'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
        })

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

        except requests.exceptions.HTTPError as e:
            # Re-raise HTTP errors (503, 429, etc.) for proper handling
            logger.warning(f"Amazon.co.jp HTTP error for '{keyword}': {e}")
            raise AmazonSearchError(f"HTTP error: {e}", is_retryable=True)
        except requests.exceptions.RequestException as e:
            logger.warning(f"Amazon.co.jp request failed for '{keyword}': {e}")
            raise AmazonSearchError(f"Request error: {e}", is_retryable=True)
        except Exception as e:
            logger.warning(f"Amazon.co.jp search failed for '{keyword}': {e}")
            raise AmazonSearchError(f"Search error: {e}", is_retryable=False)

    def search_amazon_com(
        self,
        keyword: str,
        max_results: int = 20
    ) -> List[BookResult]:
        """Search Amazon.com for English keyword."""
        if not REQUESTS_AVAILABLE:
            logger.warning("requests not available")
            return []

        # Refresh session if needed (fresh_session mode or after errors)
        self._maybe_refresh_session()
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

        except requests.exceptions.HTTPError as e:
            # Re-raise HTTP errors (503, 429, etc.) for proper handling
            logger.warning(f"Amazon.com HTTP error for '{keyword}': {e}")
            raise AmazonSearchError(f"HTTP error: {e}", is_retryable=True)
        except requests.exceptions.RequestException as e:
            logger.warning(f"Amazon.com request failed for '{keyword}': {e}")
            raise AmazonSearchError(f"Request error: {e}", is_retryable=True)
        except Exception as e:
            logger.warning(f"Amazon.com search failed for '{keyword}': {e}")
            raise AmazonSearchError(f"Search error: {e}", is_retryable=False)

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

        # Get title (try multiple selectors for robustness)
        title_elem = (
            item.select_one('h2 span') or
            item.select_one('h2 a span') or
            item.select_one('.a-size-medium.a-text-normal') or
            item.select_one('h2')
        )
        if not title_elem:
            return None
        title = title_elem.get_text(strip=True)
        if not title:
            return None

        # Skip sponsored items by checking title prefix
        if title.startswith('スポンサー'):
            return None

        # Get URL
        url = f"https://www.amazon.co.jp/dp/{asin}"

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

        # Get description (snippet shown in search results)
        description = None
        desc_elem = (
            item.select_one('.a-size-base-plus.a-color-base.a-text-normal') or
            item.select_one('.a-row.a-size-base.a-color-base') or
            item.select_one('[data-cy="title-recipe"] + div')
        )
        if desc_elem:
            desc_text = desc_elem.get_text(strip=True)
            # Filter out non-description text
            if desc_text and len(desc_text) > 20 and not desc_text.startswith('¥'):
                description = desc_text[:500]  # Limit length

        return BookResult(
            title=title,
            authors=authors,
            asin=asin,
            url=url,
            price=price,
            currency="JPY",
            language="ja",
            description=description
        )

    def _extract_book_com(self, item) -> Optional[BookResult]:
        """Extract book info from Amazon.com item."""
        asin = item.get('data-asin', '')
        if not asin:
            return None

        # Get title (try multiple selectors for robustness)
        title_elem = (
            item.select_one('h2 span') or
            item.select_one('h2 a span') or
            item.select_one('.a-size-medium.a-text-normal') or
            item.select_one('h2')
        )
        if not title_elem:
            return None
        title = title_elem.get_text(strip=True)
        if not title:
            return None

        # Skip sponsored items
        if title.lower().startswith('sponsor'):
            return None

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

        # Get description (snippet shown in search results)
        description = None
        desc_elem = (
            item.select_one('.a-size-base-plus.a-color-base.a-text-normal') or
            item.select_one('.a-row.a-size-base.a-color-base') or
            item.select_one('[data-cy="title-recipe"] + div')
        )
        if desc_elem:
            desc_text = desc_elem.get_text(strip=True)
            # Filter out non-description text
            if desc_text and len(desc_text) > 20 and not desc_text.startswith('$'):
                description = desc_text[:500]  # Limit length

        return BookResult(
            title=title,
            authors=authors,
            asin=asin,
            url=url,
            price=price,
            currency="USD",
            language="en",
            description=description
        )


def filter_books_for_search(
    books: List[BookResult],
    language: str = "ja",
    max_count: int = 10,
    verbose: bool = False
) -> Tuple[List[BookResult], Dict[str, int]]:
    """
    Filter out beginner books AND irrelevant content, return top N.
    Wrapper for filter_relevant_books from scripts.filters.

    Returns:
        Tuple of (filtered_books, exclusion_stats)
    """
    if FILTER_AVAILABLE:
        return filter_relevant_books(
            books,
            language=language,
            max_count=max_count,
            title_getter=lambda b: b.title,
            verbose=verbose,
            logger=logger
        )
    else:
        # Fallback: no filtering if module not available
        logger.warning("Filter module not available - returning all books")
        return books[:max_count], {"beginner": 0, "irrelevant": 0, "accepted": len(books[:max_count])}


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
            try:
                logger.info("Building book index for fuzzy matching...")
                self.book_index = BookIndex(master_records)
                logger.info(f"  Index stats: {self.book_index.stats()}")
            except Exception as e:
                logger.warning(f"  Failed to build book index: {e}")
                self.book_index = None

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
    parser.add_argument(
        '--fresh-session',
        action='store_true',
        help='Create a fresh session for each request (helps avoid blocking)'
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
    # Auto-resume if state file exists (unless --reset is specified)
    if args.reset:
        state = SearchState(started_at=datetime.now().isoformat())
        logger.info("Reset progress - starting fresh")
    elif STATE_FILE.exists():
        # Auto-resume from existing state
        state = load_state(STATE_FILE)
        logger.info(f"Resuming from saved state (auto-detected)")
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
    if not dup_checker.book_index:
        logger.warning("  Fuzzy matching disabled (book_index not available)")

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
    searcher = AmazonSearcher(delay=args.delay, fresh_session=args.fresh_session)
    if args.fresh_session:
        logger.info("Fresh session mode enabled - creating new session for each request")

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

                # Filter out beginner books and irrelevant content
                filtered, filter_stats = filter_books_for_search(
                    results,
                    language=language,
                    max_count=args.max_per_keyword * 2,  # Get extra for duplicates
                    verbose=True
                )

                filtered_count = len(results) - len(filtered)
                stats['books_filtered'] += filtered_count
                # Track detailed filter stats
                if 'filter_by_type' not in stats:
                    stats['filter_by_type'] = {'beginner': 0, 'irrelevant': 0}
                stats['filter_by_type']['beginner'] += filter_stats['beginner']
                stats['filter_by_type']['irrelevant'] += filter_stats['irrelevant']
                logger.info(
                    f"  After filtering: {len(filtered)} books "
                    f"(excluded {filter_stats['beginner']} beginner, "
                    f"{filter_stats['irrelevant']} irrelevant)"
                )

                # Add to master (avoiding duplicates)
                added_count = 0
                dup_count_this_keyword = 0
                for book in filtered:
                    # Check for duplicates using multi-tier strategy
                    is_dup, dup_reason, dup_score = dup_checker.is_duplicate(book)
                    if is_dup:
                        stats['books_duplicate'] += 1
                        dup_count_this_keyword += 1
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

                logger.info(f"  Added: {added_count} new books (duplicates skipped: {dup_count_this_keyword})")

                # Update state
                state.keywords_processed[keyword_id] = 'completed'
                state.total_books_added += added_count
                stats['keywords_processed'] += 1

                # Reset error counter on success
                searcher.consecutive_errors = 0

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

            except AmazonSearchError as e:
                logger.error(f"  Amazon search error: {e}")
                state.keywords_processed[keyword_id] = 'failed'
                stats['errors'] += 1
                if e.is_retryable:
                    logger.info(f"  (Marked as failed - will retry on next run)")
                    searcher.consecutive_errors += 1
                    searcher._backoff_wait()
            except Exception as e:
                logger.error(f"  Unexpected error: {e}")
                state.keywords_processed[keyword_id] = 'failed'
                stats['errors'] += 1
                searcher.consecutive_errors += 1
                searcher._backoff_wait()

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
    print(f"  Books filtered: {stats['books_filtered']}")
    # Show filter breakdown if available
    if 'filter_by_type' in stats:
        filter_types = stats['filter_by_type']
        print(f"    - Beginner/Intro books: {filter_types.get('beginner', 0)}")
        print(f"    - Irrelevant content: {filter_types.get('irrelevant', 0)}")
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
    failed = len([k for k, v in state.keywords_processed.items() if v == 'failed'])
    total = state.total_keywords
    if total > 0:
        progress = completed / total * 100
        print(f"\n  Overall progress: {completed}/{total} ({progress:.1f}%)")
        if failed > 0:
            print(f"  Failed keywords (will retry): {failed}")


if __name__ == '__main__':
    main()
