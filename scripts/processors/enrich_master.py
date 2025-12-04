"""
Verify and enrich master DB records with missing information.

This script checks existing records and enriches them with:
- ISBN (via Google Books API, OpenBD, title search)
- Price information
- Amazon/publisher URLs (with validation)
- Publisher normalization
- Segment/subsegment classification from description

Usage:
    # Check completeness of master DB
    python scripts/processors/enrich_master.py --check

    # Enrich records (dry-run)
    python scripts/processors/enrich_master.py --enrich --dry-run

    # Enrich records and update master
    python scripts/processors/enrich_master.py --enrich

    # Enrich specific records by work_id
    python scripts/processors/enrich_master.py --enrich --work-ids id1 id2

    # Auto-apply LLM classification without review (full automation)
    python scripts/processors/enrich_master.py --enrich --llm-classify --auto-apply

    # Export incomplete records for manual review
    python scripts/processors/enrich_master.py --export-incomplete incomplete.jsonl

    # Export ambiguous records for human review
    python scripts/processors/enrich_master.py --export-review review.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple, Set
from urllib.parse import quote_plus, urlparse

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

try:
    from scripts.classifiers.segment_classifier import (
        LLMSegmentClassifier,
        ClassificationResult,
        LLMProvider
    )
    LLM_CLASSIFIER_AVAILABLE = True
except ImportError:
    LLM_CLASSIFIER_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Default paths
DEFAULT_MASTER_FILE = Path("data/master/books.jsonl")
DEFAULT_SEGMENT_TOPICS_FILE = Path("data/config/segment_topics.jsonl")


@dataclass
class CompletenessReport:
    """Report on record completeness."""
    total: int = 0
    with_isbn: int = 0
    with_price: int = 0
    with_url: int = 0
    with_amazon_url: int = 0
    with_valid_url: int = 0
    with_publisher: int = 0
    with_description: int = 0
    with_pages: int = 0
    verified: int = 0

    @property
    def missing_isbn(self) -> int:
        return self.total - self.with_isbn

    @property
    def missing_price(self) -> int:
        return self.total - self.with_price

    @property
    def missing_url(self) -> int:
        return self.total - self.with_url

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total": self.total,
            "with_isbn": self.with_isbn,
            "with_price": self.with_price,
            "with_url": self.with_url,
            "with_amazon_url": self.with_amazon_url,
            "with_valid_url": self.with_valid_url,
            "with_publisher": self.with_publisher,
            "with_description": self.with_description,
            "with_pages": self.with_pages,
            "verified": self.verified,
            "missing_isbn": self.missing_isbn,
            "missing_price": self.missing_price,
            "missing_url": self.missing_url,
            "completeness_isbn": f"{self.with_isbn / self.total * 100:.1f}%" if self.total else "N/A",
            "completeness_price": f"{self.with_price / self.total * 100:.1f}%" if self.total else "N/A",
            "completeness_url": f"{self.with_url / self.total * 100:.1f}%" if self.total else "N/A",
        }


class AmbiguityReason(Enum):
    """Reasons why a record needs human review."""
    LOW_CONFIDENCE_CLASSIFICATION = auto()  # Segment classification confidence < threshold
    MULTIPLE_CLASSIFICATION_MATCHES = auto()  # Multiple segments matched equally
    TITLE_MISMATCH = auto()  # API returned title differs from original
    AUTHOR_MISMATCH = auto()  # API returned authors differ from original
    URL_VALIDATION_FAILED = auto()  # URL validation failed or uncertain
    NO_ISBN_FOUND = auto()  # Could not find ISBN from any source
    AMBIGUOUS_SEARCH_RESULT = auto()  # Title search returned multiple similar results
    MISSING_CRITICAL_DATA = auto()  # Missing important fields after enrichment
    SEGMENT_CHANGE = auto()  # Segment was changed during classification


@dataclass
class ReviewRecord:
    """A record that needs human review."""
    work_id: str
    title: str
    reasons: List[str]  # List of AmbiguityReason names
    details: Dict[str, Any]  # Additional context for each reason
    original_data: Dict[str, Any]  # Original record data
    suggested_changes: Dict[str, Any]  # Suggested changes from enrichment

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "work_id": self.work_id,
            "title": self.title,
            "reasons": self.reasons,
            "details": self.details,
            "original_data": self.original_data,
            "suggested_changes": self.suggested_changes,
            "needs_review": True
        }


# Global list to collect review records during enrichment
_review_records: List[ReviewRecord] = []

# Counter for auto-applied segment changes
_auto_applied_count: int = 0
_auto_applied_records: List[Dict[str, Any]] = []


def increment_auto_applied(record: Dict[str, Any], old_segment: str, new_segment: str,
                           old_subsegment: str, new_subsegment: str, confidence: float) -> None:
    """Track auto-applied segment change."""
    global _auto_applied_count
    _auto_applied_count += 1
    _auto_applied_records.append({
        'work_id': record.get('work_id', ''),
        'title': record.get('title', ''),
        'old_segment': old_segment,
        'new_segment': new_segment,
        'old_subsegment': old_subsegment,
        'new_subsegment': new_subsegment,
        'confidence': confidence
    })


def get_auto_applied_stats() -> Tuple[int, List[Dict[str, Any]]]:
    """Get auto-applied statistics."""
    return _auto_applied_count, _auto_applied_records


def clear_auto_applied_stats() -> None:
    """Clear auto-applied statistics."""
    global _auto_applied_count, _auto_applied_records
    _auto_applied_count = 0
    _auto_applied_records = []


def add_for_review(
    record: Dict[str, Any],
    reasons: List[AmbiguityReason],
    details: Dict[str, Any],
    suggested_changes: Optional[Dict[str, Any]] = None
) -> None:
    """Add a record to the review list."""
    review = ReviewRecord(
        work_id=record.get('work_id', ''),
        title=record.get('title', ''),
        reasons=[r.name for r in reasons],
        details=details,
        original_data={
            'work_id': record.get('work_id'),
            'title': record.get('title'),
            'segment': record.get('segment'),
            'subsegment': record.get('subsegment'),
            'language': record.get('language'),
            'publisher': record.get('publisher'),
            'authors': record.get('authors', []),
        },
        suggested_changes=suggested_changes or {}
    )
    _review_records.append(review)


def get_review_records() -> List[ReviewRecord]:
    """Get all records needing review."""
    return _review_records


def clear_review_records() -> None:
    """Clear the review list."""
    global _review_records
    _review_records = []


@dataclass
class SegmentClassifier:
    """Classify books into segments based on keywords."""
    topics: List[Dict[str, Any]] = field(default_factory=list)
    _keyword_index: Dict[str, List[Tuple[str, str]]] = field(default_factory=dict)

    def load(self, path: Path) -> None:
        """Load segment topics from JSONL file."""
        self.topics = []
        self._keyword_index = {}

        if not path.exists():
            logger.warning(f"Segment topics file not found: {path}")
            return

        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        topic = json.loads(line)
                        self.topics.append(topic)
                        # Build keyword index
                        segment = topic.get('segment', '')
                        subsegment = topic.get('subsegment', '')
                        for kw in topic.get('keywords_ja', []) + topic.get('keywords_en', []):
                            kw_lower = kw.lower()
                            if kw_lower not in self._keyword_index:
                                self._keyword_index[kw_lower] = []
                            self._keyword_index[kw_lower].append((segment, subsegment))
                    except json.JSONDecodeError:
                        pass

        logger.info(f"Loaded {len(self.topics)} topics with {len(self._keyword_index)} keywords")

    def classify(
        self,
        title: str,
        description: Optional[str] = None,
        topics: Optional[List[str]] = None,
        return_all_matches: bool = False
    ) -> Optional[Tuple[str, str, float, Optional[Dict[str, Any]]]]:
        """
        Classify a book based on title, description, and topics.

        Returns:
            Tuple of (segment, subsegment, confidence, match_details) or None
            match_details contains information about all matches (for ambiguity detection)
        """
        if not self._keyword_index:
            return None

        # Combine all text for matching
        text_parts = [title.lower()]
        if description:
            text_parts.append(description.lower())
        if topics:
            text_parts.extend([t.lower() for t in topics])
        combined_text = ' '.join(text_parts)

        # Count segment/subsegment matches
        matches: Dict[Tuple[str, str], int] = {}
        matched_keywords: Set[str] = set()
        keyword_by_segment: Dict[Tuple[str, str], List[str]] = {}

        for keyword, seg_sub_list in self._keyword_index.items():
            # Check if keyword appears in text
            if len(keyword) >= 3 and keyword in combined_text:
                matched_keywords.add(keyword)
                for segment, subsegment in seg_sub_list:
                    key = (segment, subsegment)
                    matches[key] = matches.get(key, 0) + 1
                    if key not in keyword_by_segment:
                        keyword_by_segment[key] = []
                    keyword_by_segment[key].append(keyword)

        if not matches:
            return None

        # Sort matches by count
        sorted_matches = sorted(matches.items(), key=lambda x: x[1], reverse=True)
        best_match = sorted_matches[0]
        (segment, subsegment), count = best_match

        # Calculate confidence based on number of matching keywords
        confidence = min(0.95, 0.5 + count * 0.1)

        # Check for ambiguity - multiple segments with similar counts
        match_details: Optional[Dict[str, Any]] = None
        if return_all_matches or (len(sorted_matches) > 1 and sorted_matches[1][1] >= count * 0.8):
            match_details = {
                'all_matches': [
                    {
                        'segment': seg,
                        'subsegment': sub,
                        'score': cnt,
                        'keywords': keyword_by_segment.get((seg, sub), [])[:5]  # Limit keywords
                    }
                    for (seg, sub), cnt in sorted_matches[:5]  # Top 5 matches
                ],
                'is_ambiguous': len(sorted_matches) > 1 and sorted_matches[1][1] >= count * 0.8,
                'matched_keywords': list(matched_keywords)[:10]  # Limit keywords
            }

        return segment, subsegment, confidence, match_details

    def get_all_segments(self) -> List[str]:
        """Get list of all unique segments."""
        return list(set(t.get('segment', '') for t in self.topics))

    def get_subsegments(self, segment: str) -> List[str]:
        """Get list of subsegments for a given segment."""
        return list(set(
            t.get('subsegment', '')
            for t in self.topics
            if t.get('segment', '') == segment
        ))


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


def extract_isbn(record: Dict[str, Any]) -> Optional[str]:
    """Extract the first ISBN from a record."""
    for edition in record.get('editions', []):
        for fmt in edition.get('formats', []):
            isbn = fmt.get('isbn')
            if isbn:
                return clean_isbn(isbn)
    return None


def extract_all_isbns(record: Dict[str, Any]) -> List[str]:
    """Extract all ISBNs from a record."""
    isbns = []
    for edition in record.get('editions', []):
        for fmt in edition.get('formats', []):
            isbn = fmt.get('isbn')
            if isbn:
                clean = clean_isbn(isbn)
                if clean and clean not in isbns:
                    isbns.append(clean)
    return isbns


def clean_isbn(isbn: str) -> Optional[str]:
    """Clean and validate ISBN."""
    if not isbn:
        return None
    isbn = re.sub(r'[-\s]', '', str(isbn))
    if re.match(r'^(97[89])?\d{9}[\dX]$', isbn):
        return isbn
    return None


def has_price(record: Dict[str, Any]) -> bool:
    """Check if any format has price information."""
    for edition in record.get('editions', []):
        for fmt in edition.get('formats', []):
            if fmt.get('price') and fmt['price'].get('amount'):
                return True
    return False


def has_url(record: Dict[str, Any]) -> bool:
    """Check if any format has a URL."""
    for edition in record.get('editions', []):
        for fmt in edition.get('formats', []):
            if fmt.get('url'):
                return True
    return False


def has_amazon_url(record: Dict[str, Any]) -> bool:
    """Check if any format has an Amazon URL."""
    for edition in record.get('editions', []):
        for fmt in edition.get('formats', []):
            url = fmt.get('url', '')
            if 'amazon' in url.lower():
                return True
    return False


def has_valid_url(record: Dict[str, Any]) -> bool:
    """Check if any format has a validated URL."""
    for edition in record.get('editions', []):
        for fmt in edition.get('formats', []):
            if fmt.get('url_status') == 'valid':
                return True
    return False


def has_description(record: Dict[str, Any]) -> bool:
    """Check if any edition has a description."""
    for edition in record.get('editions', []):
        if edition.get('description'):
            return True
    return False


def has_pages(record: Dict[str, Any]) -> bool:
    """Check if any edition has page count."""
    for edition in record.get('editions', []):
        if edition.get('pages'):
            return True
    return False


def check_completeness(records: List[Dict[str, Any]]) -> CompletenessReport:
    """Check completeness of all records."""
    report = CompletenessReport(total=len(records))

    for record in records:
        if extract_isbn(record):
            report.with_isbn += 1
        if has_price(record):
            report.with_price += 1
        if has_url(record):
            report.with_url += 1
        if has_amazon_url(record):
            report.with_amazon_url += 1
        if has_valid_url(record):
            report.with_valid_url += 1
        if record.get('publisher') and record['publisher'] != 'Unknown':
            report.with_publisher += 1
        if has_description(record):
            report.with_description += 1
        if has_pages(record):
            report.with_pages += 1
        if record.get('dataset_status') == 'verified':
            report.verified += 1

    return report


def get_incomplete_records(
    records: List[Dict[str, Any]],
    require_isbn: bool = True,
    require_price: bool = False,
    require_url: bool = False
) -> List[Dict[str, Any]]:
    """Filter records that are missing required information."""
    incomplete = []
    for record in records:
        missing = []
        if require_isbn and not extract_isbn(record):
            missing.append('isbn')
        if require_price and not has_price(record):
            missing.append('price')
        if require_url and not has_url(record):
            missing.append('url')

        if missing:
            record['_missing'] = missing
            incomplete.append(record)

    return incomplete


class BookEnricher:
    """Enrich book records with missing information from external APIs."""

    def __init__(
        self,
        request_delay: float = 1.0,
        segment_classifier: Optional[SegmentClassifier] = None,
        llm_classifier: Optional['LLMSegmentClassifier'] = None,
        use_llm_classification: bool = False,
        auto_apply_classification: bool = False
    ):
        self.request_delay = request_delay
        self.last_request_time = 0.0
        self._session: Optional[aiohttp.ClientSession] = None
        self.segment_classifier = segment_classifier
        self.llm_classifier = llm_classifier
        self.use_llm_classification = use_llm_classification
        self.auto_apply_classification = auto_apply_classification

    def _wait(self):
        """Rate limiting."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.request_delay:
            time.sleep(self.request_delay - elapsed)
        self.last_request_time = time.time()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15)
            self._session = aiohttp.ClientSession(
                headers={
                    'User-Agent': 'Mozilla/5.0 (compatible; FinLLMDataset/1.0; Book Enrichment)'
                },
                timeout=timeout
            )
        return self._session

    async def close(self):
        """Close the session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def validate_url(self, url: str) -> Tuple[bool, Optional[str]]:
        """
        Validate a URL by actually fetching it.

        Returns:
            Tuple of (is_valid, final_url_or_error)
        """
        if not url:
            return False, "Empty URL"

        if not AIOHTTP_AVAILABLE:
            return self._validate_url_sync(url)

        self._wait()

        try:
            session = await self._get_session()
            async with session.get(url, allow_redirects=True) as response:
                final_url = str(response.url)

                # Check for valid response
                if response.status == 200:
                    # For Amazon, check if we landed on a product page
                    if 'amazon' in url.lower():
                        content = await response.text()
                        # Check if it's a search results page (not a product page)
                        if '/s?' in final_url or 'ref=nb_sb_noss' in final_url:
                            # Search page - try to extract first result
                            return False, "Search page (no direct product match)"
                        # Check for "no results" page
                        if 'ご指定の検索条件に一致する商品はありませんでした' in content:
                            return False, "No results found"
                        if 'did not match any products' in content.lower():
                            return False, "No results found"
                        # Check for actual product page indicators
                        if 'id="productTitle"' in content or 'id="title"' in content:
                            return True, final_url
                        # Accept if we got a dp/ (product) URL
                        if '/dp/' in final_url or '/gp/product/' in final_url:
                            return True, final_url
                        return False, "Not a product page"
                    return True, final_url
                elif response.status == 404:
                    return False, "Not found (404)"
                else:
                    return False, f"HTTP {response.status}"

        except asyncio.TimeoutError:
            return False, "Timeout"
        except Exception as e:
            return False, str(e)

    def _validate_url_sync(self, url: str) -> Tuple[bool, Optional[str]]:
        """Synchronous URL validation."""
        if not REQUESTS_AVAILABLE:
            return False, "requests not available"

        self._wait()

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; FinLLMDataset/1.0; Book Enrichment)'
            }
            response = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
            final_url = response.url

            if response.status_code == 200:
                if 'amazon' in url.lower():
                    if '/s?' in final_url or 'ref=nb_sb_noss' in final_url:
                        return False, "Search page"
                    if '/dp/' in final_url or '/gp/product/' in final_url:
                        return True, final_url
                    return False, "Not a product page"
                return True, final_url
            else:
                return False, f"HTTP {response.status_code}"

        except requests.Timeout:
            return False, "Timeout"
        except Exception as e:
            return False, str(e)

    async def search_openbd(self, isbn: str) -> Optional[Dict[str, Any]]:
        """Search OpenBD API for Japanese book information."""
        if not AIOHTTP_AVAILABLE:
            return self._search_openbd_sync(isbn)

        self._wait()
        url = f"https://api.openbd.jp/v1/get?isbn={isbn}"

        try:
            session = await self._get_session()
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and data[0]:
                        return data[0]
        except Exception as e:
            logger.warning(f"OpenBD API error for {isbn}: {e}")

        return None

    def _search_openbd_sync(self, isbn: str) -> Optional[Dict[str, Any]]:
        """Synchronous version of OpenBD search."""
        if not REQUESTS_AVAILABLE:
            return None

        self._wait()
        url = f"https://api.openbd.jp/v1/get?isbn={isbn}"

        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data and data[0]:
                    return data[0]
        except Exception as e:
            logger.warning(f"OpenBD API error for {isbn}: {e}")

        return None

    async def search_google_books(
        self,
        title: str,
        author: Optional[str] = None,
        isbn: Optional[str] = None,
        language: str = 'en'
    ) -> Optional[Dict[str, Any]]:
        """Search Google Books API by title/author/ISBN."""
        if not AIOHTTP_AVAILABLE:
            return self._search_google_books_sync(title, author, isbn, language)

        self._wait()

        # Build query
        if isbn:
            query = f"isbn:{isbn}"
        else:
            # Search by title
            query = f"intitle:{quote_plus(title)}"
            if author:
                query += f"+inauthor:{quote_plus(author)}"

        # Add language restriction
        lang_param = f"&langRestrict={language}" if language else ""
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=3{lang_param}"

        try:
            session = await self._get_session()
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('totalItems', 0) > 0 and data.get('items'):
                        # Return best match (first result)
                        return data['items'][0]
        except Exception as e:
            logger.warning(f"Google Books API error: {e}")

        return None

    def _search_google_books_sync(
        self,
        title: str,
        author: Optional[str] = None,
        isbn: Optional[str] = None,
        language: str = 'en'
    ) -> Optional[Dict[str, Any]]:
        """Synchronous version of Google Books search."""
        if not REQUESTS_AVAILABLE:
            return None

        self._wait()

        if isbn:
            query = f"isbn:{isbn}"
        else:
            query = f"intitle:{quote_plus(title)}"
            if author:
                query += f"+inauthor:{quote_plus(author)}"

        lang_param = f"&langRestrict={language}" if language else ""
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=3{lang_param}"

        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('totalItems', 0) > 0 and data.get('items'):
                    return data['items'][0]
        except Exception as e:
            logger.warning(f"Google Books API error: {e}")

        return None

    async def search_amazon_product(
        self,
        title: str,
        author: Optional[str] = None,
        isbn: Optional[str] = None,
        language: str = 'ja'
    ) -> Optional[Dict[str, Any]]:
        """
        Search Amazon and return product info if found.
        Note: This uses the search page, not an official API.
        """
        domain = 'amazon.co.jp' if language == 'ja' else 'amazon.com'

        # Build search URL
        if isbn:
            search_url = f"https://www.{domain}/s?k={isbn}"
        else:
            search_term = title
            if author:
                search_term += f" {author}"
            search_url = f"https://www.{domain}/s?k={quote_plus(search_term)}&i=stripbooks"

        is_valid, result = await self.validate_url(search_url)

        if is_valid and '/dp/' in result:
            # Extract ASIN from URL
            asin_match = re.search(r'/dp/([A-Z0-9]{10})', result)
            if asin_match:
                return {
                    'url': result,
                    'asin': asin_match.group(1)
                }

        return None

    def generate_amazon_url(
        self,
        isbn: Optional[str] = None,
        asin: Optional[str] = None,
        title: Optional[str] = None,
        language: str = 'ja'
    ) -> Optional[str]:
        """Generate Amazon URL from ISBN, ASIN, or title."""
        domain = 'amazon.co.jp' if language == 'ja' else 'amazon.com'

        if asin:
            return f"https://www.{domain}/dp/{asin}"
        if isbn:
            # ISBN-10 can be used as ASIN for books
            if len(isbn) == 10:
                return f"https://www.{domain}/dp/{isbn}"
            elif len(isbn) == 13:
                return f"https://www.{domain}/s?k={isbn}"
        if title:
            return f"https://www.{domain}/s?k={quote_plus(title)}"

        return None

    async def search_amazon_url_via_web(
        self,
        title: str,
        author: Optional[str] = None,
        language: str = 'ja'
    ) -> Optional[str]:
        """
        Search for Amazon product URL via web search as fallback.
        Uses DuckDuckGo HTML search to find Amazon product pages.
        """
        if not AIOHTTP_AVAILABLE and not REQUESTS_AVAILABLE:
            return None

        self._wait()

        domain = 'amazon.co.jp' if language == 'ja' else 'amazon.com'
        search_query = f'site:{domain} {title}'
        if author:
            search_query += f' {author}'

        # Use DuckDuckGo HTML search
        ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(search_query)}"

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            if AIOHTTP_AVAILABLE:
                session = await self._get_session()
                async with session.get(ddg_url, headers=headers) as response:
                    if response.status == 200:
                        html = await response.text()
                    else:
                        return None
            else:
                response = requests.get(ddg_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    html = response.text
                else:
                    return None

            # Parse HTML to find Amazon product URLs
            # Look for /dp/ pattern in results
            import re
            amazon_pattern = rf'https?://(?:www\.)?{re.escape(domain)}/[^\s"\'<>]*?/dp/([A-Z0-9]{{10}})'
            matches = re.findall(amazon_pattern, html)

            if matches:
                asin = matches[0]
                product_url = f"https://www.{domain}/dp/{asin}"
                logger.debug(f"Found Amazon URL via web search: {product_url}")
                return product_url

        except Exception as e:
            logger.debug(f"Web search for Amazon URL failed: {e}")

        return None

    def _search_amazon_url_via_web_sync(
        self,
        title: str,
        author: Optional[str] = None,
        language: str = 'ja'
    ) -> Optional[str]:
        """Synchronous version of web search for Amazon URL."""
        if not REQUESTS_AVAILABLE:
            return None

        self._wait()

        domain = 'amazon.co.jp' if language == 'ja' else 'amazon.com'
        search_query = f'site:{domain} {title}'
        if author:
            search_query += f' {author}'

        ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(search_query)}"

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(ddg_url, headers=headers, timeout=10)
            if response.status_code == 200:
                html = response.text
                import re
                amazon_pattern = rf'https?://(?:www\.)?{re.escape(domain)}/[^\s"\'<>]*?/dp/([A-Z0-9]{{10}})'
                matches = re.findall(amazon_pattern, html)
                if matches:
                    asin = matches[0]
                    return f"https://www.{domain}/dp/{asin}"
        except Exception as e:
            logger.debug(f"Sync web search for Amazon URL failed: {e}")

        return None

    def parse_openbd_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse OpenBD response into enrichment data."""
        result = {}

        summary = data.get('summary', {})
        onix = data.get('onix', {})

        # ISBN
        if summary.get('isbn'):
            result['isbn'] = clean_isbn(summary['isbn'])

        # Publisher
        if summary.get('publisher'):
            result['publisher'] = summary['publisher']

        # Title (for verification)
        if summary.get('title'):
            result['title_verified'] = summary['title']

        # Price - handle both dict and list formats
        product_supply = onix.get('ProductSupply', {})
        if product_supply:
            supply_detail = product_supply.get('SupplyDetail', {})
            if isinstance(supply_detail, dict):
                prices = supply_detail.get('Price', [])
                if isinstance(prices, list):
                    for price_detail in prices:
                        if price_detail.get('PriceAmount'):
                            try:
                                result['price'] = {
                                    'amount': float(price_detail['PriceAmount']),
                                    'currency': price_detail.get('CurrencyCode', 'JPY'),
                                    'price_type': 'list'
                                }
                            except (ValueError, TypeError):
                                pass
                            break
                elif isinstance(prices, dict) and prices.get('PriceAmount'):
                    try:
                        result['price'] = {
                            'amount': float(prices['PriceAmount']),
                            'currency': prices.get('CurrencyCode', 'JPY'),
                            'price_type': 'list'
                        }
                    except (ValueError, TypeError):
                        pass

        # Pages
        if 'DescriptiveDetail' in onix:
            extent = onix['DescriptiveDetail'].get('Extent', [])
            if isinstance(extent, list):
                for e in extent:
                    if e.get('ExtentType') == '00':
                        try:
                            result['pages'] = int(e.get('ExtentValue', 0))
                        except (ValueError, TypeError):
                            pass
                        break

        # Description
        if 'CollateralDetail' in onix:
            text_content = onix['CollateralDetail'].get('TextContent', [])
            if isinstance(text_content, list):
                for text in text_content:
                    if text.get('TextType') == '03':
                        result['description'] = text.get('Text', '')
                        break

        # Cover image
        if summary.get('cover'):
            result['cover_url'] = summary['cover']

        return result

    def parse_google_books_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Google Books response into enrichment data."""
        result = {}

        volume_info = data.get('volumeInfo', {})

        # ISBNs
        for identifier in volume_info.get('industryIdentifiers', []):
            if identifier.get('type') == 'ISBN_13':
                result['isbn'] = identifier['identifier']
                break
            elif identifier.get('type') == 'ISBN_10':
                result['isbn'] = identifier['identifier']

        # Pages
        if volume_info.get('pageCount'):
            result['pages'] = volume_info['pageCount']

        # Description
        if volume_info.get('description'):
            result['description'] = volume_info['description']

        # Publisher
        if volume_info.get('publisher'):
            result['publisher'] = volume_info['publisher']

        # Title (for verification)
        if volume_info.get('title'):
            result['title_verified'] = volume_info['title']

        # Authors (for verification)
        if volume_info.get('authors'):
            result['authors_verified'] = volume_info['authors']

        # Price from sale info
        sale_info = data.get('saleInfo', {})
        if sale_info.get('listPrice'):
            result['price'] = {
                'amount': sale_info['listPrice']['amount'],
                'currency': sale_info['listPrice']['currencyCode'],
                'price_type': 'list'
            }

        # Preview link
        if volume_info.get('previewLink'):
            result['google_books_url'] = volume_info['previewLink']

        return result

    async def enrich_record(
        self,
        record: Dict[str, Any],
        add_amazon_url: bool = True,
        validate_urls: bool = True,
        use_openbd: bool = True,
        use_google: bool = True,
        search_by_title: bool = True,
        classify_segment: bool = True
    ) -> Tuple[Dict[str, Any], List[str]]:
        """
        Enrich a single record with missing information.

        Returns:
            Tuple of (enriched_record, list of changes made)
        """
        changes = []
        record = record.copy()
        language = record.get('language', 'en')
        title = record.get('title', '')
        authors = record.get('authors', [])
        first_author = authors[0] if authors else None

        # Get existing ISBN
        existing_isbn = extract_isbn(record)
        found_data = False

        # Try to enrich from OpenBD (Japanese books)
        if use_openbd and language == 'ja':
            openbd_data = None

            if existing_isbn:
                openbd_data = await self.search_openbd(existing_isbn)

            # If no ISBN, try searching by title
            if not openbd_data and search_by_title and not existing_isbn:
                # OpenBD doesn't support title search, skip
                pass

            if openbd_data:
                enrichment = self.parse_openbd_data(openbd_data)
                new_changes = self._apply_enrichment(record, enrichment, 'openbd')
                changes.extend(new_changes)
                if new_changes:
                    found_data = True

        # Try to enrich from Google Books
        if use_google:
            google_data = None

            if existing_isbn:
                google_data = await self.search_google_books(
                    title=title,
                    author=first_author,
                    isbn=existing_isbn,
                    language=language
                )
            elif search_by_title:
                # Search by title if no ISBN
                google_data = await self.search_google_books(
                    title=title,
                    author=first_author,
                    isbn=None,
                    language=language
                )

            if google_data:
                enrichment = self.parse_google_books_data(google_data)
                new_changes = self._apply_enrichment(record, enrichment, 'google')
                changes.extend(new_changes)
                if new_changes:
                    found_data = True

        # Get updated ISBN after enrichment
        current_isbn = extract_isbn(record)

        # Generate and validate Amazon URL
        if add_amazon_url:
            existing_url = None
            for edition in record.get('editions', []):
                if edition.get('is_latest'):
                    for fmt in edition.get('formats', []):
                        if fmt.get('url'):
                            existing_url = fmt.get('url')
                            break
                    break

            # Generate new URL if needed
            if not existing_url or (validate_urls and not has_valid_url(record)):
                amazon_url = self.generate_amazon_url(
                    isbn=current_isbn,
                    title=title if not current_isbn else None,
                    language=language
                )

                if amazon_url:
                    # Validate the URL
                    if validate_urls:
                        is_valid, final_url = await self.validate_url(amazon_url)
                        if is_valid:
                            # Update format with validated URL
                            for edition in record.get('editions', []):
                                if edition.get('is_latest'):
                                    for fmt in edition.get('formats', []):
                                        fmt['url'] = final_url
                                        fmt['url_status'] = 'valid'
                                        fmt['url_verified_at'] = datetime.utcnow().isoformat() + 'Z'
                                        if not existing_url:
                                            changes.append('amazon_url:valid')
                                        else:
                                            changes.append('url_validated')
                                        found_data = True
                                        break
                                    break
                        else:
                            # Fallback: try web search to find Amazon URL
                            logger.debug(f"URL validation failed for {title[:30]}: {final_url}, trying web search...")
                            web_url = await self.search_amazon_url_via_web(
                                title=title,
                                author=first_author,
                                language=language
                            )
                            if web_url:
                                # Validate the web search result
                                is_valid_web, final_web_url = await self.validate_url(web_url)
                                if is_valid_web:
                                    for edition in record.get('editions', []):
                                        if edition.get('is_latest'):
                                            for fmt in edition.get('formats', []):
                                                fmt['url'] = final_web_url
                                                fmt['url_status'] = 'valid'
                                                fmt['url_verified_at'] = datetime.utcnow().isoformat() + 'Z'
                                                changes.append('amazon_url:websearch')
                                                found_data = True
                                                break
                                            break
                                    logger.debug(f"Found valid URL via web search: {final_web_url}")
                    else:
                        # Add without validation
                        for edition in record.get('editions', []):
                            if edition.get('is_latest'):
                                for fmt in edition.get('formats', []):
                                    if not fmt.get('url'):
                                        fmt['url'] = amazon_url
                                        fmt['url_status'] = 'unchecked'
                                        changes.append('amazon_url')
                                        break
                                break
                else:
                    # No URL generated from ISBN/title - try web search as last resort
                    if validate_urls and not existing_url:
                        logger.debug(f"No Amazon URL generated for {title[:30]}, trying web search...")
                        web_url = await self.search_amazon_url_via_web(
                            title=title,
                            author=first_author,
                            language=language
                        )
                        if web_url:
                            is_valid_web, final_web_url = await self.validate_url(web_url)
                            if is_valid_web:
                                for edition in record.get('editions', []):
                                    if edition.get('is_latest'):
                                        for fmt in edition.get('formats', []):
                                            fmt['url'] = final_web_url
                                            fmt['url_status'] = 'valid'
                                            fmt['url_verified_at'] = datetime.utcnow().isoformat() + 'Z'
                                            changes.append('amazon_url:websearch')
                                            found_data = True
                                            break
                                        break
                                logger.debug(f"Found valid URL via web search (fallback): {final_web_url}")

            # Validate existing URL if needed
            elif validate_urls and existing_url:
                for edition in record.get('editions', []):
                    if edition.get('is_latest'):
                        for fmt in edition.get('formats', []):
                            if fmt.get('url') and fmt.get('url_status') != 'valid':
                                is_valid, result = await self.validate_url(fmt['url'])
                                if is_valid:
                                    fmt['url'] = result  # Update to final URL
                                    fmt['url_status'] = 'valid'
                                    fmt['url_verified_at'] = datetime.utcnow().isoformat() + 'Z'
                                    changes.append('url_validated')
                                    found_data = True
                                else:
                                    fmt['url_status'] = 'invalid'
                                    fmt['url_error'] = result
                                break
                        break

        # Classify segment/subsegment if needed
        if classify_segment:
            description = None
            for edition in record.get('editions', []):
                if edition.get('description'):
                    description = edition['description']
                    break

            # Use LLM classifier if enabled, otherwise fall back to keyword-based
            new_segment = None
            new_subsegment = None
            confidence = 0.0
            match_details = None
            reasoning = None

            if self.use_llm_classification and self.llm_classifier:
                # LLM-based classification
                llm_result = self.llm_classifier.classify(
                    title=title,
                    description=description,
                    authors=record.get('authors'),
                    publisher=record.get('publisher'),
                    language=record.get('language')
                )
                if llm_result.is_valid and llm_result.segment:
                    new_segment = llm_result.segment
                    new_subsegment = llm_result.subsegment
                    confidence = llm_result.confidence
                    reasoning = llm_result.reasoning
                    match_details = {
                        'provider': llm_result.provider,
                        'reasoning': reasoning
                    }
                    logger.debug(f"LLM classified '{title[:30]}': {new_segment}/{new_subsegment} ({confidence:.2f})")
                elif llm_result.error:
                    logger.warning(f"LLM classification error for '{title[:30]}': {llm_result.error}")

            elif self.segment_classifier:
                # Keyword-based classification (fallback)
                classification = self.segment_classifier.classify(
                    title=title,
                    description=description,
                    topics=record.get('topics', []),
                    return_all_matches=True
                )
                if classification:
                    new_segment, new_subsegment, confidence, match_details = classification

            if new_segment:
                original_segment = record.get('segment')
                original_subsegment = record.get('subsegment')

                # Track ambiguity reasons
                review_reasons: List[AmbiguityReason] = []
                review_details: Dict[str, Any] = {}

                # Check for low confidence
                if confidence < 0.6:
                    review_reasons.append(AmbiguityReason.LOW_CONFIDENCE_CLASSIFICATION)
                    review_details['confidence'] = confidence
                    review_details['suggested_segment'] = new_segment
                    review_details['suggested_subsegment'] = new_subsegment
                    if reasoning:
                        review_details['reasoning'] = reasoning

                # Check for multiple matches (keyword-based only)
                if match_details and match_details.get('is_ambiguous'):
                    review_reasons.append(AmbiguityReason.MULTIPLE_CLASSIFICATION_MATCHES)
                    review_details['competing_matches'] = match_details.get('all_matches', [])

                # Check for segment change
                if original_segment and original_segment != new_segment:
                    review_reasons.append(AmbiguityReason.SEGMENT_CHANGE)
                    review_details['original_segment'] = original_segment
                    review_details['new_segment'] = new_segment
                    if reasoning:
                        review_details['reasoning'] = reasoning

                # Add for review if ambiguous
                if review_reasons:
                    # Check if auto-apply is enabled and only SEGMENT_CHANGE reason
                    # (with sufficient confidence)
                    can_auto_apply = (
                        self.auto_apply_classification and
                        review_reasons == [AmbiguityReason.SEGMENT_CHANGE] and
                        confidence >= 0.7
                    )

                    if can_auto_apply:
                        # Auto-apply: apply changes without review
                        old_seg = record.get('segment', '')
                        old_subseg = record.get('subsegment', '')
                        if record.get('segment') != new_segment:
                            record['segment'] = new_segment
                            changes.append(f'segment:{new_segment[:20]}(auto)')
                        if record.get('subsegment') != new_subsegment:
                            record['subsegment'] = new_subsegment
                            changes.append(f'subsegment:{new_subsegment[:20]}(auto)')
                        # Track auto-applied change
                        increment_auto_applied(
                            record=record,
                            old_segment=old_seg,
                            new_segment=new_segment,
                            old_subsegment=old_subseg,
                            new_subsegment=new_subsegment,
                            confidence=confidence
                        )
                        logger.info(
                            f"Auto-applied: {title[:40]} | "
                            f"{old_seg}/{old_subseg} -> {new_segment}/{new_subsegment} "
                            f"(conf: {confidence:.2f})"
                        )
                    else:
                        # Add for human review
                        add_for_review(
                            record=record,
                            reasons=review_reasons,
                            details=review_details,
                            suggested_changes={
                                'segment': new_segment,
                                'subsegment': new_subsegment,
                                'confidence': confidence
                            }
                        )
                        # Don't apply changes if ambiguous
                        logger.debug(f"Ambiguous classification for {title[:30]}: {[r.name for r in review_reasons]}")
                elif confidence >= 0.6:
                    # Update if confidence is high enough and not ambiguous
                    if record.get('segment') != new_segment:
                        record['segment'] = new_segment
                        changes.append(f'segment:{new_segment[:20]}')
                    if record.get('subsegment') != new_subsegment:
                        record['subsegment'] = new_subsegment
                        changes.append(f'subsegment:{new_subsegment[:20]}')

        # Update status if data was found and verified
        if found_data:
            # Mark as verified if we have key information
            has_key_info = (
                extract_isbn(record) and
                has_price(record) and
                has_valid_url(record)
            )
            if has_key_info and record.get('dataset_status') != 'verified':
                record['dataset_status'] = 'verified'
                changes.append('status:verified')

            # Update timestamp
            record['updated_at'] = datetime.utcnow().isoformat() + 'Z'
            if 'enriched' not in record.get('data_sources', []):
                record.setdefault('data_sources', []).append('enriched')

        # Check for missing critical data after enrichment and flag for review
        post_enrich_review_reasons: List[AmbiguityReason] = []
        post_enrich_details: Dict[str, Any] = {}

        # Flag if still no ISBN after searching
        if search_by_title and not extract_isbn(record):
            post_enrich_review_reasons.append(AmbiguityReason.NO_ISBN_FOUND)
            post_enrich_details['searched_title'] = title
            post_enrich_details['searched_author'] = first_author

        # Flag if URL validation failed
        for edition in record.get('editions', []):
            for fmt in edition.get('formats', []):
                if fmt.get('url_status') == 'invalid':
                    post_enrich_review_reasons.append(AmbiguityReason.URL_VALIDATION_FAILED)
                    post_enrich_details['url'] = fmt.get('url')
                    post_enrich_details['url_error'] = fmt.get('url_error')
                    break
            if AmbiguityReason.URL_VALIDATION_FAILED in post_enrich_review_reasons:
                break

        # Flag records missing critical data
        missing_fields = []
        if not extract_isbn(record):
            missing_fields.append('isbn')
        if not has_price(record):
            missing_fields.append('price')
        if not has_valid_url(record):
            missing_fields.append('valid_url')
        if not has_description(record):
            missing_fields.append('description')

        if len(missing_fields) >= 3:  # Missing 3+ critical fields
            post_enrich_review_reasons.append(AmbiguityReason.MISSING_CRITICAL_DATA)
            post_enrich_details['missing_fields'] = missing_fields

        # Add for review if issues found (but only if not already flagged)
        if post_enrich_review_reasons:
            # Check if already in review list
            existing_ids = {r.work_id for r in get_review_records()}
            if record.get('work_id') not in existing_ids:
                add_for_review(
                    record=record,
                    reasons=post_enrich_review_reasons,
                    details=post_enrich_details,
                    suggested_changes={}
                )

        return record, changes

    def _apply_enrichment(
        self,
        record: Dict[str, Any],
        enrichment: Dict[str, Any],
        source: str
    ) -> List[str]:
        """Apply enrichment data to record, only filling missing fields."""
        changes = []
        title = record.get('title', '')
        authors = record.get('authors', [])

        # Check for title mismatch (potential wrong book match)
        review_reasons: List[AmbiguityReason] = []
        review_details: Dict[str, Any] = {}

        if enrichment.get('title_verified'):
            verified_title = enrichment['title_verified']
            # Simple similarity check - if less than 50% of words match, flag for review
            original_words = set(title.lower().split())
            verified_words = set(verified_title.lower().split())
            if original_words and verified_words:
                overlap = len(original_words & verified_words)
                max_len = max(len(original_words), len(verified_words))
                similarity = overlap / max_len if max_len > 0 else 0
                if similarity < 0.5:
                    review_reasons.append(AmbiguityReason.TITLE_MISMATCH)
                    review_details['original_title'] = title
                    review_details['api_title'] = verified_title
                    review_details['similarity'] = similarity

        # Check for author mismatch
        if enrichment.get('authors_verified') and authors:
            verified_authors = enrichment['authors_verified']
            original_set = set(a.lower() for a in authors)
            verified_set = set(a.lower() for a in verified_authors)
            if not (original_set & verified_set):  # No overlap
                review_reasons.append(AmbiguityReason.AUTHOR_MISMATCH)
                review_details['original_authors'] = authors
                review_details['api_authors'] = verified_authors

        # If significant mismatch found, add for review and skip enrichment
        if review_reasons:
            add_for_review(
                record=record,
                reasons=review_reasons,
                details=review_details,
                suggested_changes={
                    'source': source,
                    'suggested_isbn': enrichment.get('isbn'),
                    'suggested_price': enrichment.get('price'),
                }
            )
            logger.debug(f"Title/Author mismatch for {title[:30]}: {[r.name for r in review_reasons]}")
            return []  # Don't apply changes if mismatch

        # ISBN - add to latest edition's format if missing
        if enrichment.get('isbn') and not extract_isbn(record):
            for edition in record.get('editions', []):
                if edition.get('is_latest'):
                    for fmt in edition.get('formats', []):
                        if not fmt.get('isbn'):
                            fmt['isbn'] = enrichment['isbn']
                            changes.append(f'isbn:{source}')
                            break
                    break

        # Price - add to format if missing
        if enrichment.get('price') and not has_price(record):
            for edition in record.get('editions', []):
                if edition.get('is_latest'):
                    for fmt in edition.get('formats', []):
                        if not fmt.get('price'):
                            fmt['price'] = enrichment['price']
                            changes.append(f'price:{source}')
                            break
                    break

        # Pages - add to edition if missing
        if enrichment.get('pages') and not has_pages(record):
            for edition in record.get('editions', []):
                if edition.get('is_latest') and not edition.get('pages'):
                    edition['pages'] = enrichment['pages']
                    changes.append(f'pages:{source}')
                    break

        # Description - add to edition if missing
        if enrichment.get('description') and not has_description(record):
            for edition in record.get('editions', []):
                if edition.get('is_latest') and not edition.get('description'):
                    edition['description'] = enrichment['description'][:2000]
                    changes.append(f'description:{source}')
                    break

        # Publisher - update if unknown or empty
        current_publisher = record.get('publisher', '')
        if enrichment.get('publisher') and (not current_publisher or current_publisher == 'Unknown'):
            record['publisher'] = enrichment['publisher']
            changes.append(f'publisher:{source}')

        return changes


async def enrich_records(
    records: List[Dict[str, Any]],
    segment_classifier: Optional[SegmentClassifier] = None,
    llm_classifier: Optional['LLMSegmentClassifier'] = None,
    work_ids: Optional[List[str]] = None,
    dry_run: bool = False,
    max_records: Optional[int] = None,
    validate_urls: bool = True,
    search_by_title: bool = True,
    classify_segment: bool = True,
    use_llm_classification: bool = False,
    auto_apply: bool = False
) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]]]:
    """
    Enrich multiple records.

    Returns:
        Tuple of (enriched_records, dict of work_id -> changes)
    """
    # Clear statistics from previous runs
    clear_review_records()
    clear_auto_applied_stats()

    enricher = BookEnricher(
        request_delay=1.5,
        segment_classifier=segment_classifier,
        llm_classifier=llm_classifier,
        use_llm_classification=use_llm_classification,
        auto_apply_classification=auto_apply
    )
    all_changes = {}

    # Filter by work_ids if specified
    if work_ids:
        target_records = [r for r in records if r.get('work_id') in work_ids]
    else:
        target_records = records

    # Limit number of records
    if max_records:
        target_records = target_records[:max_records]

    # Create lookup for updating
    record_lookup = {r.get('work_id'): r for r in records}

    logger.info(f"Enriching {len(target_records)} records...")

    try:
        for i, record in enumerate(target_records):
            work_id = record.get('work_id', '')

            if dry_run:
                logger.info(f"[DRY-RUN] Would enrich: {record.get('title', '')[:50]}...")
                continue

            enriched, changes = await enricher.enrich_record(
                record,
                validate_urls=validate_urls,
                search_by_title=search_by_title,
                classify_segment=classify_segment
            )

            if changes:
                all_changes[work_id] = changes
                record_lookup[work_id] = enriched
                logger.info(
                    f"Enriched [{i+1}/{len(target_records)}]: "
                    f"{record.get('title', '')[:40]}... ({', '.join(changes[:5])})"
                )
            else:
                logger.debug(
                    f"No changes [{i+1}/{len(target_records)}]: "
                    f"{record.get('title', '')[:40]}..."
                )

    finally:
        await enricher.close()

    # Reconstruct full record list
    enriched_records = [record_lookup.get(r.get('work_id'), r) for r in records]

    return enriched_records, all_changes


def main():
    parser = argparse.ArgumentParser(
        description='Verify and enrich master DB records'
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--check',
        action='store_true',
        help='Check completeness of master DB'
    )
    group.add_argument(
        '--enrich',
        action='store_true',
        help='Enrich records with missing information'
    )
    group.add_argument(
        '--export-incomplete',
        type=Path,
        metavar='FILE',
        help='Export incomplete records to file for manual review'
    )
    group.add_argument(
        '--export-review',
        type=Path,
        metavar='FILE',
        help='Export ambiguous records that need human review (run after --enrich)'
    )

    parser.add_argument(
        '--master',
        type=Path,
        default=DEFAULT_MASTER_FILE,
        help=f'Master file path (default: {DEFAULT_MASTER_FILE})'
    )
    parser.add_argument(
        '--segment-topics',
        type=Path,
        default=DEFAULT_SEGMENT_TOPICS_FILE,
        help=f'Segment topics file path (default: {DEFAULT_SEGMENT_TOPICS_FILE})'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    parser.add_argument(
        '--work-ids',
        nargs='+',
        help='Only process specific work IDs'
    )
    parser.add_argument(
        '--max-records',
        type=int,
        help='Maximum number of records to enrich'
    )
    parser.add_argument(
        '--no-validate-urls',
        action='store_true',
        help='Skip URL validation'
    )
    parser.add_argument(
        '--no-title-search',
        action='store_true',
        help='Skip searching by title (only use ISBN)'
    )
    parser.add_argument(
        '--no-classify',
        action='store_true',
        help='Skip segment/subsegment classification'
    )
    parser.add_argument(
        '--llm-classify',
        action='store_true',
        help='Use LLM for segment classification (requires API key in secrets.json)'
    )
    parser.add_argument(
        '--llm-provider',
        choices=['claude', 'openai', 'gemini'],
        default='claude',
        help='LLM provider for classification (default: claude)'
    )
    parser.add_argument(
        '--llm-model',
        type=str,
        help='LLM model name (provider-specific, optional)'
    )
    parser.add_argument(
        '--auto-apply',
        action='store_true',
        help='Auto-apply LLM classification without review (use with --llm-classify)'
    )
    parser.add_argument(
        '--require-isbn',
        action='store_true',
        default=True,
        help='Include records missing ISBN in incomplete list'
    )
    parser.add_argument(
        '--require-price',
        action='store_true',
        help='Include records missing price in incomplete list'
    )
    parser.add_argument(
        '--require-url',
        action='store_true',
        help='Include records missing URL in incomplete list'
    )
    parser.add_argument(
        '--review-file',
        type=Path,
        metavar='FILE',
        help='Save ambiguous records to file during enrichment (for human review)'
    )

    args = parser.parse_args()

    # Validate --auto-apply usage
    if args.auto_apply and not args.llm_classify:
        logger.warning("--auto-apply is only effective with --llm-classify. Ignoring.")

    # Load master
    logger.info(f"Loading master from {args.master}")
    records = load_jsonl(args.master)
    logger.info(f"Loaded {len(records)} records")

    # Load segment classifier
    segment_classifier = None
    llm_classifier = None

    if args.enrich and not args.no_classify:
        if args.llm_classify:
            # Use LLM-based classification
            if not LLM_CLASSIFIER_AVAILABLE:
                logger.error("LLM classifier not available. Install anthropic/openai/google-generativeai package.")
                sys.exit(1)

            logger.info(f"Initializing LLM classifier ({args.llm_provider})...")
            try:
                llm_classifier = LLMSegmentClassifier.create(
                    provider=args.llm_provider,
                    model=args.llm_model
                )
                logger.info(f"LLM classifier ready ({len(llm_classifier.segments)} segments loaded)")
            except Exception as e:
                logger.error(f"Failed to initialize LLM classifier: {e}")
                sys.exit(1)
        else:
            # Use keyword-based classification
            segment_classifier = SegmentClassifier()
            segment_classifier.load(args.segment_topics)

    if args.check:
        report = check_completeness(records)
        print("\n" + "=" * 60)
        print("Master DB Completeness Report")
        print("=" * 60)
        for key, value in report.to_dict().items():
            print(f"  {key}: {value}")

        # Show by language
        ja_records = [r for r in records if r.get('language') == 'ja']
        en_records = [r for r in records if r.get('language') == 'en']

        if ja_records:
            ja_report = check_completeness(ja_records)
            print(f"\nJapanese books ({len(ja_records)}):")
            print(f"  ISBN: {ja_report.with_isbn} ({ja_report.with_isbn/len(ja_records)*100:.1f}%)")
            print(f"  Price: {ja_report.with_price} ({ja_report.with_price/len(ja_records)*100:.1f}%)")
            print(f"  URL (valid): {ja_report.with_valid_url} ({ja_report.with_valid_url/len(ja_records)*100:.1f}%)")
            print(f"  Verified: {ja_report.verified} ({ja_report.verified/len(ja_records)*100:.1f}%)")

        if en_records:
            en_report = check_completeness(en_records)
            print(f"\nEnglish books ({len(en_records)}):")
            print(f"  ISBN: {en_report.with_isbn} ({en_report.with_isbn/len(en_records)*100:.1f}%)")
            print(f"  Price: {en_report.with_price} ({en_report.with_price/len(en_records)*100:.1f}%)")
            print(f"  URL (valid): {en_report.with_valid_url} ({en_report.with_valid_url/len(en_records)*100:.1f}%)")
            print(f"  Verified: {en_report.verified} ({en_report.verified/len(en_records)*100:.1f}%)")

        # Show by status
        status_counts = {}
        for r in records:
            status = r.get('dataset_status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
        print("\nBy status:")
        for status, count in sorted(status_counts.items()):
            print(f"  {status}: {count}")

    elif args.enrich:
        if not AIOHTTP_AVAILABLE and not REQUESTS_AVAILABLE:
            logger.error("Neither aiohttp nor requests is available. Install one to use enrichment.")
            return

        enriched, changes = asyncio.run(enrich_records(
            records,
            segment_classifier=segment_classifier,
            llm_classifier=llm_classifier,
            work_ids=args.work_ids,
            dry_run=args.dry_run,
            max_records=args.max_records,
            validate_urls=not args.no_validate_urls,
            search_by_title=not args.no_title_search,
            classify_segment=not args.no_classify,
            use_llm_classification=args.llm_classify,
            auto_apply=args.auto_apply
        ))

        print(f"\n{'=' * 60}")
        print("Enrichment Summary")
        print(f"{'=' * 60}")
        print(f"  Total records: {len(records)}")
        print(f"  Records enriched: {len(changes)}")

        if changes:
            # Count by change type
            change_counts = {}
            for work_changes in changes.values():
                for change in work_changes:
                    change_type = change.split(':')[0]
                    change_counts[change_type] = change_counts.get(change_type, 0) + 1

            print(f"\nChanges by type:")
            for change_type, count in sorted(change_counts.items()):
                print(f"    {change_type}: {count}")

            # Count verified
            verified_count = sum(
                1 for work_changes in changes.values()
                if 'status:verified' in work_changes
            )
            if verified_count:
                print(f"\n  Newly verified: {verified_count}")

        if not args.dry_run and changes:
            save_jsonl(enriched, args.master)
            logger.info(f"Saved enriched master to {args.master}")

        # Show auto-applied statistics if --auto-apply was used
        auto_count, auto_records = get_auto_applied_stats()
        if auto_count > 0:
            print(f"\n{'=' * 60}")
            print("Auto-Applied Segment Changes")
            print(f"{'=' * 60}")
            print(f"  Total auto-applied: {auto_count}")

            # Show segment change summary
            segment_changes: Dict[str, int] = {}
            for rec in auto_records:
                key = f"{rec['old_segment']} -> {rec['new_segment']}"
                segment_changes[key] = segment_changes.get(key, 0) + 1

            print("\n  Segment changes:")
            for change, count in sorted(segment_changes.items(), key=lambda x: -x[1]):
                print(f"    {change}: {count}")

            # Show details if verbose or small number
            if len(auto_records) <= 10:
                print("\n  Details:")
                for rec in auto_records:
                    print(f"    - {rec['title'][:50]}")
                    print(f"      {rec['old_segment']}/{rec['old_subsegment']} -> "
                          f"{rec['new_segment']}/{rec['new_subsegment']} "
                          f"(conf: {rec['confidence']:.2f})")

        # Export review records if any
        review_records = get_review_records()
        if review_records:
            print(f"\n{'=' * 60}")
            print("Records Requiring Human Review")
            print(f"{'=' * 60}")
            print(f"  Total needing review: {len(review_records)}")

            # Count by reason
            reason_counts: Dict[str, int] = {}
            for r in review_records:
                for reason in r.reasons:
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1

            print("\n  Review reasons breakdown:")
            for reason, count in sorted(reason_counts.items()):
                print(f"    {reason}: {count}")

            # Show which reasons are NOT auto-applied
            non_auto_reasons = [r for r in reason_counts.keys() if r != 'SEGMENT_CHANGE']
            if non_auto_reasons and args.auto_apply:
                print("\n  Note: The following reasons require manual review even with --auto-apply:")
                for reason in non_auto_reasons:
                    print(f"    - {reason}")

            # Save if review file specified
            review_file = args.review_file
            if not review_file:
                # Default to same directory as master with _review suffix
                review_file = args.master.parent / f"{args.master.stem}_review.jsonl"

            if not args.dry_run:
                with open(review_file, 'w', encoding='utf-8') as f:
                    for r in review_records:
                        f.write(json.dumps(r.to_dict(), ensure_ascii=False) + '\n')
                print(f"\n  Saved review records to {review_file}")
        elif args.auto_apply and auto_count > 0:
            print("\n  No records requiring human review (all eligible changes auto-applied)")

    elif args.export_review:
        # Load existing review file if it exists
        if args.export_review.exists():
            logger.info(f"Loading existing review file: {args.export_review}")
            existing_reviews = load_jsonl(args.export_review)
            print(f"\nExisting review file: {args.export_review}")
            print(f"  Total records: {len(existing_reviews)}")

            # Count by reason
            reason_counts: Dict[str, int] = {}
            for r in existing_reviews:
                for reason in r.get('reasons', []):
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1

            print("\n  Reasons breakdown:")
            for reason, count in sorted(reason_counts.items()):
                print(f"    {reason}: {count}")

            # Show sample records
            print("\n  Sample records needing review:")
            for r in existing_reviews[:5]:
                print(f"    - {r.get('title', '')[:50]}... ({', '.join(r.get('reasons', [])[:2])})")

        else:
            print(f"\nNo review file found at {args.export_review}")
            print("Run --enrich first to generate review records.")
            print("\nUsage:")
            print(f"  python {sys.argv[0]} --enrich --max-records 100")
            print(f"  python {sys.argv[0]} --export-review {args.export_review}")

    elif args.export_incomplete:
        incomplete = get_incomplete_records(
            records,
            require_isbn=args.require_isbn,
            require_price=args.require_price,
            require_url=args.require_url
        )

        save_jsonl(incomplete, args.export_incomplete)
        print(f"\nExported {len(incomplete)} incomplete records to {args.export_incomplete}")

        # Summary
        missing_counts = {}
        for r in incomplete:
            for field in r.get('_missing', []):
                missing_counts[field] = missing_counts.get(field, 0) + 1

        print("\nMissing fields:")
        for field, count in sorted(missing_counts.items()):
            print(f"  {field}: {count}")


if __name__ == '__main__':
    main()
