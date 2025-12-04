#!/usr/bin/env python3
"""
Wiley Finance & Investments Book Crawler (v2 Schema)

Crawls Wiley's finance book catalog using Playwright for JavaScript-rendered pages.
Handles pagination, extracts book metadata with multi-format support, and saves to JSONL format.

Usage:
    python wiley_crawler.py --output data/raw/wiley/
    python wiley_crawler.py --category general_finance --max-pages 5
    python wiley_crawler.py --category all --fetch-details
"""
from __future__ import annotations

import argparse
import asyncio
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from playwright.async_api import async_playwright, Page, Browser
except ImportError:
    print("Playwright is required. Install with: pip install playwright && playwright install")
    sys.exit(1)

from scripts.crawlers.base_crawler import (
    BaseCrawlerV2, BookRecordV2, EditionInfo, FormatInfo
)


# Wiley Finance category URLs
WILEY_CATEGORIES = {
    "general_finance": {
        "name": "General Finance & Investments",
        "url": "https://www.wiley.com/en-us/General+Finance+%26+Investments-c-FI00",
        "segment": "Securities & Investment Banking",
        "subsegment": "General Finance",
    },
    "corporate_finance": {
        "name": "Corporate Finance",
        "url": "https://www.wiley.com/en-us/Corporate+Finance-c-FI20",
        "segment": "Securities & Investment Banking",
        "subsegment": "Corporate Finance",
    },
    "valuation": {
        "name": "Valuation",
        "url": "https://www.wiley.com/en-us/Valuation-c-FI2040",
        "segment": "Securities & Investment Banking",
        "subsegment": "Valuation",
    },
    "mergers_acquisitions": {
        "name": "Mergers & Acquisitions",
        "url": "https://www.wiley.com/en-us/Mergers+%26+Acquisitions-c-FI2020",
        "segment": "Securities & Investment Banking",
        "subsegment": "M&A",
    },
    "investment_management": {
        "name": "Investment Management",
        "url": "https://www.wiley.com/en-us/Investment+Management-c-FI40",
        "segment": "Asset Management",
        "subsegment": "Investment Management",
    },
    "portfolio_management": {
        "name": "Portfolio Management",
        "url": "https://www.wiley.com/en-us/Portfolio+Management-c-FI4020",
        "segment": "Asset Management",
        "subsegment": "Portfolio Management",
    },
    "derivatives": {
        "name": "Derivatives",
        "url": "https://www.wiley.com/en-us/Derivatives-c-FI4010",
        "segment": "Securities & Investment Banking",
        "subsegment": "Derivatives",
    },
    "risk_management": {
        "name": "Risk Management",
        "url": "https://www.wiley.com/en-us/Risk+Management-c-FI60",
        "segment": "Securities & Investment Banking",
        "subsegment": "Risk Management",
    },
    "quantitative_finance": {
        "name": "Quantitative Finance",
        "url": "https://www.wiley.com/en-us/Quantitative+Finance-c-FI4030",
        "segment": "Asset Management",
        "subsegment": "Quantitative Finance",
    },
    "banking": {
        "name": "Banking",
        "url": "https://www.wiley.com/en-us/Banking-c-FI10",
        "segment": "Banking & Payments",
        "subsegment": "General Banking",
    },
    "insurance": {
        "name": "Insurance & Risk Management",
        "url": "https://www.wiley.com/en-us/Insurance+%26+Risk+Management-c-ACIN",
        "segment": "Insurance",
        "subsegment": "General Insurance",
    },
    "accounting": {
        "name": "Accounting",
        "url": "https://www.wiley.com/en-us/Accounting-c-AC00",
        "segment": "Finance, Tax & Accounting",
        "subsegment": "Accounting",
    },
}


@dataclass
class WileyBookRaw:
    """Raw book data from Wiley website."""
    title: str
    url: str
    authors: List[str]
    isbn: Optional[str] = None
    price: Optional[str] = None
    format_type: Optional[str] = None
    publication_date: Optional[str] = None
    edition_label: Optional[str] = None
    pages: Optional[int] = None
    description: Optional[str] = None
    series: Optional[str] = None
    # Multiple formats from detail page
    formats: Optional[List[Dict[str, Any]]] = None


class WileyCrawlerV2(BaseCrawlerV2):
    """Crawler for Wiley book catalog (v2 schema with multi-format support)."""

    BASE_URL = "https://www.wiley.com"

    def __init__(
        self,
        output_dir: Path,
        request_delay: float = 2.0,
        max_pages: Optional[int] = None,
        headless: bool = True,
        fetch_details: bool = False
    ):
        super().__init__(
            publisher_name="Wiley",
            output_dir=output_dir,
            request_delay=request_delay,
            max_pages=max_pages
        )
        self.headless = headless
        self.fetch_details = fetch_details
        self.browser: Optional[Browser] = None

    async def setup_browser(self):
        """Initialize Playwright browser."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080}
        )
        self.logger.info("Browser initialized")

    async def close_browser(self):
        """Close browser and cleanup."""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.logger.info("Browser closed")

    async def crawl_category(
        self,
        category_url: str,
        category_name: str,
        segment: str = "Securities & Investment Banking",
        subsegment: str = "General"
    ) -> List[BookRecordV2]:
        """Crawl a Wiley category page and extract book listings."""
        records: List[BookRecordV2] = []
        page = await self.context.new_page()
        current_page = 1

        try:
            self.logger.info(f"Crawling category: {category_name}")

            while True:
                # Build URL with pagination
                if current_page == 1:
                    url = category_url
                else:
                    separator = "&" if "?" in category_url else "?"
                    url = f"{category_url}{separator}pn={current_page}"

                self.logger.info(f"Fetching page {current_page}: {url}")

                await page.goto(url, wait_until="networkidle", timeout=60000)
                await asyncio.sleep(2)

                # Extract book listings
                raw_books = await self._extract_book_listings(page)

                if not raw_books:
                    self.logger.info(f"No more books found on page {current_page}")
                    break

                self.logger.info(f"Found {len(raw_books)} books on page {current_page}")

                # Optionally fetch detailed info for each book
                if self.fetch_details:
                    for i, book in enumerate(raw_books):
                        self.logger.info(f"Fetching details {i+1}/{len(raw_books)}: {book.title[:50]}...")
                        detailed = await self._fetch_book_details(book.url)
                        if detailed:
                            book.formats = detailed.get("formats")
                            book.description = detailed.get("description")
                            book.pages = detailed.get("pages")
                            book.series = detailed.get("series")
                        self.wait()

                # Convert to BookRecordV2
                for book in raw_books:
                    record = self._convert_to_record_v2(book, segment, subsegment)
                    records.append(record)
                    self.crawled_count += 1

                # Check max pages limit
                if self.max_pages and current_page >= self.max_pages:
                    self.logger.info(f"Reached max pages limit ({self.max_pages})")
                    break

                # Check for next page
                has_next = await self._has_next_page(page)
                if not has_next:
                    self.logger.info("No more pages available")
                    break

                current_page += 1
                self.wait()

        except Exception as e:
            self.logger.error(f"Error crawling category {category_name}: {e}")
            self.error_count += 1

        finally:
            await page.close()

        return records

    async def _extract_book_listings(self, page: Page) -> List[WileyBookRaw]:
        """Extract book information from the current page."""
        books: List[WileyBookRaw] = []

        try:
            await page.wait_for_selector(
                ".product-item, .product-card, [data-testid='product-card']",
                timeout=10000
            )
        except Exception:
            self.logger.warning("Could not find product listings with standard selectors")

        book_data = await page.evaluate("""
            () => {
                const books = [];
                const selectors = [
                    '.product-item',
                    '.product-card',
                    '[data-testid="product-card"]',
                    '.product-listing-item',
                    'article[class*="product"]',
                    '.search-result-item',
                    '[class*="ProductCard"]'
                ];

                let items = [];
                for (const selector of selectors) {
                    items = document.querySelectorAll(selector);
                    if (items.length > 0) break;
                }

                items.forEach(item => {
                    const titleEl = item.querySelector(
                        'h2 a, h3 a, .product-title a, [class*="title"] a, a[class*="Title"]'
                    );
                    const title = titleEl ? titleEl.textContent.trim() : '';
                    const url = titleEl ? titleEl.href : '';

                    const authorEl = item.querySelector(
                        '.author, .authors, [class*="author"], [class*="Author"]'
                    );
                    const authorText = authorEl ? authorEl.textContent.trim() : '';

                    let isbn = '';
                    if (url) {
                        const isbnMatch = url.match(/\\d{13}|\\d{10}/);
                        if (isbnMatch) isbn = isbnMatch[0];
                    }

                    const priceEl = item.querySelector(
                        '.price, [class*="price"], [class*="Price"]'
                    );
                    const price = priceEl ? priceEl.textContent.trim() : '';

                    const formatEl = item.querySelector(
                        '.format, [class*="format"], [class*="Format"]'
                    );
                    const format = formatEl ? formatEl.textContent.trim() : '';

                    if (title && url) {
                        books.push({ title, url, authorText, isbn, price, format });
                    }
                });

                return books;
            }
        """)

        for data in book_data:
            authors = self._parse_authors(data.get("authorText", ""))
            format_type = self._normalize_format(data.get("format", ""))

            book = WileyBookRaw(
                title=data["title"],
                url=data["url"],
                authors=authors,
                isbn=data.get("isbn") or None,
                price=data.get("price") or None,
                format_type=format_type
            )
            books.append(book)

        return books

    async def _fetch_book_details(self, book_url: str) -> Optional[Dict[str, Any]]:
        """Fetch detailed book information including all formats."""
        page = await self.context.new_page()

        try:
            await page.goto(book_url, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(1)

            details = await page.evaluate("""
                () => {
                    const result = { formats: [] };

                    // Description
                    const descEl = document.querySelector(
                        '.product-description, [class*="description"], .about-book'
                    );
                    if (descEl) {
                        result.description = descEl.textContent.trim().substring(0, 1000);
                    }

                    // Pages
                    const pagesText = document.body.innerText;
                    const pagesMatch = pagesText.match(/(\\d+)\\s*pages?/i);
                    if (pagesMatch) {
                        result.pages = parseInt(pagesMatch[1]);
                    }

                    // Series
                    const seriesEl = document.querySelector('[class*="series"]');
                    if (seriesEl) {
                        result.series = seriesEl.textContent.trim();
                    }

                    // Try to find all format options
                    const formatSelectors = [
                        '[class*="format-option"]',
                        '[class*="product-format"]',
                        '[data-format]',
                        '.buying-options li',
                        '[class*="BuyingOption"]'
                    ];

                    for (const selector of formatSelectors) {
                        const formatEls = document.querySelectorAll(selector);
                        formatEls.forEach(el => {
                            const formatText = el.textContent || '';
                            let formatType = 'other';

                            if (/hardcover|hardback/i.test(formatText)) formatType = 'hardcover';
                            else if (/paperback|softcover/i.test(formatText)) formatType = 'paperback';
                            else if (/e-?book|digital|epub/i.test(formatText)) formatType = 'ebook';
                            else if (/pdf/i.test(formatText)) formatType = 'pdf';
                            else if (/kindle/i.test(formatText)) formatType = 'kindle';

                            // Price
                            const priceMatch = formatText.match(/\\$([\\d,]+\\.?\\d*)/);
                            const price = priceMatch ? parseFloat(priceMatch[1].replace(',', '')) : null;

                            // ISBN
                            const isbnMatch = formatText.match(/(97[89]\\d{10}|\\d{10})/);
                            const isbn = isbnMatch ? isbnMatch[1] : null;

                            // URL
                            const link = el.querySelector('a');
                            const url = link ? link.href : null;

                            if (formatType !== 'other' || price || isbn) {
                                result.formats.push({
                                    format_type: formatType,
                                    price: price,
                                    isbn: isbn,
                                    url: url
                                });
                            }
                        });
                    }

                    return result;
                }
            """)

            return details

        except Exception as e:
            self.logger.error(f"Error fetching details from {book_url}: {e}")
            return None

        finally:
            await page.close()

    def _parse_authors(self, author_text: str) -> List[str]:
        """Parse author string into list of names."""
        if not author_text:
            return []

        author_text = re.sub(r"^(By|Author[s]?:?)\s*", "", author_text, flags=re.IGNORECASE)
        authors = re.split(r",\s*(?:and\s+)?|;\s*|\s+and\s+|\s*&\s*", author_text)
        authors = [a.strip() for a in authors if a.strip()]
        return authors

    def _normalize_format(self, format_str: str) -> str:
        """Normalize format string to enum value."""
        if not format_str:
            return "other"

        format_lower = format_str.lower()
        if "hardcover" in format_lower or "hardback" in format_lower:
            return "hardcover"
        elif "paperback" in format_lower or "softcover" in format_lower:
            return "paperback"
        elif "ebook" in format_lower or "e-book" in format_lower or "digital" in format_lower:
            return "ebook"
        elif "pdf" in format_lower:
            return "pdf"
        elif "kindle" in format_lower:
            return "kindle"
        elif "audio" in format_lower:
            return "audiobook"
        elif "online" in format_lower:
            return "online_access"
        elif "bundle" in format_lower or "+" in format_lower:
            return "bundle"

        return "other"

    async def _has_next_page(self, page: Page) -> bool:
        """Check if there's a next page available."""
        next_selectors = [
            'a[aria-label="Next"]',
            'button[aria-label="Next"]',
            '.pagination-next:not(.disabled)',
            'a.next:not(.disabled)',
            '[class*="next"]:not(.disabled)',
        ]

        for selector in next_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    is_disabled = await element.get_attribute("disabled")
                    if not is_disabled:
                        return True
            except Exception:
                continue

        return False

    async def get_book_details(self, book_url: str) -> Optional[BookRecordV2]:
        """Get detailed information for a single book."""
        details = await self._fetch_book_details(book_url)
        if not details:
            return None

        # This would need more context to create a full record
        # For now, return None - use crawl_category with fetch_details=True instead
        return None

    def _convert_to_record_v2(
        self,
        book: WileyBookRaw,
        segment: str,
        subsegment: str
    ) -> BookRecordV2:
        """Convert WileyBookRaw to BookRecordV2."""
        # Parse edition from title
        clean_title, edition_num, edition_label = self.parse_edition_from_title(book.title)

        # Parse publication year (default to current year if not found)
        pub_year = datetime.now().year
        if book.publication_date:
            year_match = re.search(r"20\d{2}|19\d{2}", book.publication_date)
            if year_match:
                pub_year = int(year_match.group())

        # Build formats list
        formats: List[FormatInfo] = []

        if book.formats:
            # Use detailed formats from product page
            for fmt_data in book.formats:
                price_amount, price_currency = None, None
                if fmt_data.get("price"):
                    price_amount = fmt_data["price"]
                    price_currency = "USD"

                fmt = FormatInfo(
                    format_type=fmt_data.get("format_type", "other"),
                    isbn=self.parse_isbn(fmt_data.get("isbn", "")),
                    price_amount=price_amount,
                    price_currency=price_currency,
                    url=fmt_data.get("url"),
                    url_status="unchecked",
                    availability="available"
                )
                formats.append(fmt)
        else:
            # Use basic info from listing
            price_amount, price_currency = self.parse_price(book.price or "")
            fmt = FormatInfo(
                format_type=book.format_type or "other",
                isbn=self.parse_isbn(book.isbn or ""),
                price_amount=price_amount,
                price_currency=price_currency,
                url=book.url,
                url_status="unchecked",
                url_verified_at=datetime.now().strftime("%Y-%m-%d"),
                availability="available"
            )
            formats.append(fmt)

        # Create edition
        edition = EditionInfo(
            edition_number=edition_num,
            edition_label=edition_label,
            publication_year=pub_year,
            is_latest=True,
            pages=book.pages,
            description=book.description[:500] if book.description else None,
            formats=formats
        )

        # Generate work_id
        first_author = book.authors[0] if book.authors else None
        work_id = self.generate_work_id(clean_title, first_author)

        return BookRecordV2(
            work_id=work_id,
            title=clean_title,
            authors=book.authors,
            language="en",
            publisher="Wiley",
            series=book.series or "Wiley Finance",
            segment=segment,
            subsegment=subsegment,
            perspective="practice",
            jurisdiction="Global",
            editions=[edition],
            data_sources=["wiley_crawler"],
            dataset_status="draft",
            created_at=datetime.now().isoformat(),
            last_reviewed_at=datetime.now().strftime("%Y-%m-%d")
        )


async def main():
    parser = argparse.ArgumentParser(description="Crawl Wiley Finance book catalog (v2 schema)")
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=Path("data/raw/wiley"),
        help="Output directory for JSONL files"
    )
    parser.add_argument(
        "--category", "-c",
        type=str,
        choices=list(WILEY_CATEGORIES.keys()) + ["all"],
        default="general_finance",
        help="Category to crawl (default: general_finance)"
    )
    parser.add_argument(
        "--max-pages", "-m",
        type=int,
        default=None,
        help="Maximum number of pages to crawl per category"
    )
    parser.add_argument(
        "--delay", "-d",
        type=float,
        default=2.0,
        help="Delay between requests in seconds"
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Run browser in visible mode (for debugging)"
    )
    parser.add_argument(
        "--fetch-details",
        action="store_true",
        help="Fetch detailed info for each book (slower but gets all formats)"
    )

    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)

    crawler = WileyCrawlerV2(
        output_dir=args.output,
        request_delay=args.delay,
        max_pages=args.max_pages,
        headless=not args.no_headless,
        fetch_details=args.fetch_details
    )

    try:
        await crawler.setup_browser()

        all_records: List[BookRecordV2] = []

        if args.category == "all":
            categories = WILEY_CATEGORIES.items()
        else:
            categories = [(args.category, WILEY_CATEGORIES[args.category])]

        for cat_key, cat_info in categories:
            records = await crawler.crawl_category(
                category_url=cat_info["url"],
                category_name=cat_info["name"],
                segment=cat_info["segment"],
                subsegment=cat_info["subsegment"]
            )
            all_records.extend(records)

            if records:
                crawler.save_records(records, f"wiley_{cat_key}_v2.jsonl")

        if all_records:
            crawler.save_records(all_records, "wiley_all_v2.jsonl")

        print(f"\n{'='*50}")
        print(f"Crawling complete!")
        print(f"Total books crawled: {crawler.crawled_count}")
        print(f"Errors: {crawler.error_count}")
        print(f"Output directory: {args.output}")
        print(f"Schema version: v2 (multi-edition/format)")
        print(f"{'='*50}")

    finally:
        await crawler.close_browser()


if __name__ == "__main__":
    asyncio.run(main())
