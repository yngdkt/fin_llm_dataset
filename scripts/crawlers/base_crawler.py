"""Base crawler class with common functionality for v2 schema."""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


@dataclass
class FormatInfo:
    """Information about a specific format of a book."""
    format_type: str  # hardcover, paperback, ebook, pdf, kindle, etc.
    isbn: Optional[str] = None
    asin: Optional[str] = None
    price_amount: Optional[float] = None
    price_currency: Optional[str] = None
    price_type: Optional[str] = None  # list, sale, rental
    url: Optional[str] = None
    url_status: str = "unchecked"
    url_verified_at: Optional[str] = None
    availability: str = "unknown"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: Dict[str, Any] = {"format_type": self.format_type}

        if self.isbn:
            result["isbn"] = self.isbn
        if self.asin:
            result["asin"] = self.asin
        if self.price_amount is not None and self.price_currency:
            result["price"] = {
                "amount": self.price_amount,
                "currency": self.price_currency
            }
            if self.price_type:
                result["price"]["price_type"] = self.price_type
        if self.url:
            result["url"] = self.url
            result["url_status"] = self.url_status
            if self.url_verified_at:
                result["url_verified_at"] = self.url_verified_at
        if self.availability != "unknown":
            result["availability"] = self.availability

        return result


@dataclass
class EditionInfo:
    """Information about a specific edition of a book."""
    edition_number: int
    publication_year: int
    is_latest: bool
    formats: List[FormatInfo]
    edition_label: Optional[str] = None
    pages: Optional[int] = None
    description: Optional[str] = None
    table_of_contents: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: Dict[str, Any] = {
            "edition_number": self.edition_number,
            "publication_year": self.publication_year,
            "is_latest": self.is_latest,
            "formats": [f.to_dict() for f in self.formats]
        }

        if self.edition_label:
            result["edition_label"] = self.edition_label
        if self.pages:
            result["pages"] = self.pages
        if self.description:
            result["description"] = self.description
        if self.table_of_contents:
            result["table_of_contents"] = self.table_of_contents

        return result


@dataclass
class BookRecordV2:
    """Book record matching the v2 JSON schema with multi-edition/format support."""
    # Required fields
    work_id: str
    title: str
    language: str
    publisher: str
    segment: str
    subsegment: str
    perspective: str
    editions: List[EditionInfo]
    dataset_status: str = "draft"

    # Optional fields
    subtitle: Optional[str] = None
    authors: List[str] = field(default_factory=list)
    editors: List[str] = field(default_factory=list)
    series: Optional[str] = None
    topics: List[str] = field(default_factory=list)
    instrument_or_asset_class: Optional[str] = None
    jurisdiction: Optional[str] = None
    audience_level: Optional[str] = None
    official_status: Optional[str] = None
    related_regulation: Optional[str] = None
    qualification_target: Optional[str] = None
    notes: Optional[str] = None
    recommended_for: Optional[str] = None
    data_sources: List[str] = field(default_factory=list)
    importance_score: Optional[float] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    last_reviewed_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values and empty lists."""
        result: Dict[str, Any] = {
            "work_id": self.work_id,
            "title": self.title,
            "language": self.language,
            "publisher": self.publisher,
            "segment": self.segment,
            "subsegment": self.subsegment,
            "perspective": self.perspective,
            "editions": [e.to_dict() for e in self.editions],
            "dataset_status": self.dataset_status,
        }

        # Optional fields
        if self.subtitle:
            result["subtitle"] = self.subtitle
        if self.authors:
            result["authors"] = self.authors
        if self.editors:
            result["editors"] = self.editors
        if self.series:
            result["series"] = self.series
        if self.topics:
            result["topics"] = self.topics
        if self.instrument_or_asset_class:
            result["instrument_or_asset_class"] = self.instrument_or_asset_class
        if self.jurisdiction:
            result["jurisdiction"] = self.jurisdiction
        if self.audience_level:
            result["audience_level"] = self.audience_level
        if self.official_status:
            result["official_status"] = self.official_status
        if self.related_regulation:
            result["related_regulation"] = self.related_regulation
        if self.qualification_target:
            result["qualification_target"] = self.qualification_target
        if self.notes:
            result["notes"] = self.notes
        if self.recommended_for:
            result["recommended_for"] = self.recommended_for
        if self.data_sources:
            result["data_sources"] = self.data_sources
        if self.importance_score is not None:
            result["importance_score"] = self.importance_score
        if self.created_at:
            result["created_at"] = self.created_at
        if self.updated_at:
            result["updated_at"] = self.updated_at
        if self.last_reviewed_at:
            result["last_reviewed_at"] = self.last_reviewed_at

        return result

    @property
    def latest_edition(self) -> Optional[EditionInfo]:
        """Get the latest edition."""
        for edition in self.editions:
            if edition.is_latest:
                return edition
        return self.editions[0] if self.editions else None

    @property
    def latest_year(self) -> Optional[int]:
        """Get the publication year of the latest edition."""
        latest = self.latest_edition
        return latest.publication_year if latest else None


class BaseCrawlerV2(ABC):
    """Abstract base class for publisher crawlers (v2 schema)."""

    def __init__(
        self,
        publisher_name: str,
        output_dir: Path,
        request_delay: float = 2.0,
        max_pages: Optional[int] = None
    ):
        self.publisher_name = publisher_name
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.request_delay = request_delay
        self.max_pages = max_pages
        self.logger = logging.getLogger(self.__class__.__name__)
        self.crawled_count = 0
        self.error_count = 0

    @abstractmethod
    async def crawl_category(self, category_url: str, category_name: str) -> List[BookRecordV2]:
        """Crawl a single category page. Must be implemented by subclasses."""
        pass

    @abstractmethod
    async def get_book_details(self, book_url: str) -> Optional[BookRecordV2]:
        """Get detailed information for a single book. Must be implemented by subclasses."""
        pass

    def save_records(self, records: List[BookRecordV2], filename: str):
        """Save records to JSONL file."""
        output_path = self.output_dir / filename
        with open(output_path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record.to_dict(), ensure_ascii=False) + "\n")
        self.logger.info(f"Saved {len(records)} records to {output_path}")

    def generate_work_id(self, title: str, first_author: Optional[str] = None) -> str:
        """
        Generate a stable work_id from title and author.
        Format: {publisher}_{hash}
        """
        # Normalize title
        normalized = self._normalize_for_id(title)
        if first_author:
            normalized += "_" + self._normalize_for_id(first_author)

        # Create hash
        hash_str = hashlib.md5(normalized.encode()).hexdigest()[:12]

        return f"{self.publisher_name.lower()}_{hash_str}"

    def _normalize_for_id(self, text: str) -> str:
        """Normalize text for ID generation."""
        # Lowercase
        text = text.lower()
        # Remove edition info
        text = re.sub(r',?\s*\d+(st|nd|rd|th)\s+edition', '', text, flags=re.IGNORECASE)
        text = re.sub(r',?\s*第\d+版', '', text)
        # Remove special characters
        text = re.sub(r'[^\w\s]', '', text)
        # Normalize whitespace
        text = re.sub(r'\s+', '_', text.strip())
        return text

    def parse_edition_from_title(self, title: str) -> tuple[str, Optional[int], Optional[str]]:
        """
        Extract edition info from title.
        Returns: (clean_title, edition_number, edition_label)
        """
        # Match patterns like "3rd Edition", "第2版", "2nd ed."
        patterns = [
            (r',?\s*(\d+)(st|nd|rd|th)\s+Edition', r'\1'),  # "3rd Edition"
            (r',?\s*(\d+)(st|nd|rd|th)\s+ed\.?', r'\1'),     # "3rd ed."
            (r',?\s*第(\d+)版', r'\1'),                       # "第2版"
            (r',?\s*Edition\s+(\d+)', r'\1'),                 # "Edition 3"
        ]

        for pattern, group in patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                edition_num = int(match.group(1))
                edition_label = match.group(0).strip().lstrip(',').strip()
                clean_title = re.sub(pattern, '', title, flags=re.IGNORECASE).strip()
                return clean_title, edition_num, edition_label

        return title, 1, None  # Default to 1st edition

    def parse_isbn(self, isbn_str: str) -> Optional[str]:
        """Clean and validate ISBN string."""
        if not isbn_str:
            return None

        # Remove hyphens and spaces
        isbn = re.sub(r'[-\s]', '', isbn_str)

        # Check if valid ISBN-10 or ISBN-13
        if re.match(r'^(97[89])?\d{9}[\dX]$', isbn):
            return isbn

        return None

    def parse_price(self, price_str: str) -> tuple[Optional[float], Optional[str]]:
        """Parse price string into amount and currency."""
        if not price_str:
            return None, None

        # Currency patterns
        currency_patterns = [
            (r'\$\s*([\d,]+\.?\d*)', 'USD'),
            (r'¥\s*([\d,]+)', 'JPY'),
            (r'€\s*([\d,]+\.?\d*)', 'EUR'),
            (r'£\s*([\d,]+\.?\d*)', 'GBP'),
            (r'([\d,]+\.?\d*)\s*USD', 'USD'),
            (r'([\d,]+)\s*円', 'JPY'),
        ]

        for pattern, currency in currency_patterns:
            match = re.search(pattern, price_str)
            if match:
                amount_str = match.group(1).replace(',', '')
                try:
                    amount = float(amount_str)
                    return amount, currency
                except ValueError:
                    continue

        return None, None

    def wait(self):
        """Wait between requests to respect rate limits."""
        time.sleep(self.request_delay)


# Backward compatibility: keep old classes for migration
@dataclass
class BookRecord:
    """Legacy book record (v1 schema). Use BookRecordV2 for new code."""
    record_id: str
    title: str
    language: str
    publication_year: int
    publisher: str
    segment: str
    subsegment: str
    perspective: str
    access_type: str
    dataset_status: str = "draft"

    # Optional fields
    subtitle: Optional[str] = None
    authors: List[str] = field(default_factory=list)
    edition: Optional[str] = None
    isbn_or_issn: Optional[str] = None
    topics: List[str] = field(default_factory=list)
    instrument_or_asset_class: Optional[str] = None
    jurisdiction: Optional[str] = None
    audience_level: Optional[str] = None
    format: Optional[str] = None
    pages: Optional[int] = None
    series: Optional[str] = None
    official_status: Optional[str] = None
    source_url: Optional[str] = None
    catalog_url: Optional[str] = None
    notes: Optional[str] = None
    related_regulation: Optional[str] = None
    qualification_target: Optional[str] = None
    recommended_for: Optional[str] = None
    data_source: Optional[str] = None
    last_reviewed_at: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary, excluding None values."""
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None and v != []}


class BaseCrawler(ABC):
    """Legacy base crawler (v1 schema). Use BaseCrawlerV2 for new code."""

    def __init__(
        self,
        publisher_name: str,
        output_dir: Path,
        request_delay: float = 2.0,
        max_pages: Optional[int] = None
    ):
        self.publisher_name = publisher_name
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.request_delay = request_delay
        self.max_pages = max_pages
        self.logger = logging.getLogger(self.__class__.__name__)
        self.crawled_count = 0
        self.error_count = 0

    @abstractmethod
    async def crawl_category(self, category_url: str, category_name: str) -> List[BookRecord]:
        """Crawl a single category page. Must be implemented by subclasses."""
        pass

    @abstractmethod
    async def get_book_details(self, book_url: str) -> Optional[BookRecord]:
        """Get detailed information for a single book. Must be implemented by subclasses."""
        pass

    def save_records(self, records: List[BookRecord], filename: str):
        """Save records to JSONL file."""
        output_path = self.output_dir / filename
        with open(output_path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record.to_dict(), ensure_ascii=False) + "\n")
        self.logger.info(f"Saved {len(records)} records to {output_path}")

    def generate_record_id(self, isbn: Optional[str] = None, title: str = "") -> str:
        """Generate a unique record ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        if isbn:
            return f"{self.publisher_name.lower()}_{isbn}_{timestamp}"
        title_hash = abs(hash(title)) % 100000
        return f"{self.publisher_name.lower()}_{title_hash}_{timestamp}"

    def wait(self):
        """Wait between requests to respect rate limits."""
        time.sleep(self.request_delay)
