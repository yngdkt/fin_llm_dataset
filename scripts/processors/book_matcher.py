"""Book matching and deduplication with fuzzy title normalization."""
from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Any
from difflib import SequenceMatcher


@dataclass
class MatchResult:
    """Result of a book matching operation."""
    is_match: bool
    confidence: float  # 0.0 - 1.0
    match_type: str  # exact, normalized, fuzzy, isbn
    normalized_title_a: str
    normalized_title_b: str
    details: Dict[str, Any]


class TitleNormalizer:
    """
    Normalize book titles for comparison, handling various notation variations.
    """

    # Edition patterns to remove
    EDITION_PATTERNS = [
        # English patterns
        r',?\s*(\d+)(st|nd|rd|th)\s+[Ee]dition',
        r',?\s*(\d+)(st|nd|rd|th)\s+[Ee]d\.?',
        r',?\s*[Ee]dition\s+(\d+)',
        r',?\s*(\d+)e\b',  # "3e" format
        r',?\s*[Ff]irst\s+[Ee]dition',
        r',?\s*[Ss]econd\s+[Ee]dition',
        r',?\s*[Tt]hird\s+[Ee]dition',
        r',?\s*[Ff]ourth\s+[Ee]dition',
        r',?\s*[Ff]ifth\s+[Ee]dition',
        r',?\s*[Rr]evised\s+[Ee]dition',
        r',?\s*[Uu]pdated\s+[Ee]dition',
        r',?\s*[Nn]ew\s+[Ee]dition',
        # Japanese patterns (with kanji numbers)
        r',?\s*第\s*[0-9０-９一二三四五六七八九十]+\s*版',
        r',?\s*改訂\s*[0-9０-９一二三四五六七八九十]*\s*版',
        r',?\s*新版',
        r',?\s*増補\s*版',
        r',?\s*改訂新版',
        r'【[^】]*版[^】]*】',
        r'\[[^\]]*版[^\]]*\]',
    ]

    # Kanji number mapping
    KANJI_NUMBERS = {
        '一': '1', '二': '2', '三': '3', '四': '4', '五': '5',
        '六': '6', '七': '7', '八': '8', '九': '9', '十': '10',
        '〇': '0', '零': '0',
    }

    # Bracket variations to normalize
    BRACKET_MAP = {
        '（': '(',
        '）': ')',
        '【': '[',
        '】': ']',
        '［': '[',
        '］': ']',
        '〔': '[',
        '〕': ']',
        '｛': '{',
        '｝': '}',
        '〈': '<',
        '〉': '>',
        '《': '<',
        '》': '>',
        '「': '"',
        '」': '"',
        '『': '"',
        '』': '"',
        '"': '"',
        '"': '"',
        ''': "'",
        ''': "'",
    }

    # Common abbreviations and expansions
    ABBREVIATIONS = {
        r'\b[Mm]&[Aa]\b': 'mergers and acquisitions',
        r'\b[Ii][Pp][Oo]\b': 'initial public offering',
        r'\b[Ee][Ss][Gg]\b': 'environmental social governance',
        r'\b[Rr][Oo][Ii]\b': 'return on investment',
        r'\b[Rr][Oo][Ee]\b': 'return on equity',
        r'\b[Dd][Cc][Ff]\b': 'discounted cash flow',
        r'\b[Ww][Aa][Cc][Cc]\b': 'weighted average cost of capital',
        r'\b[Cc][Ff][Aa]\b': 'chartered financial analyst',
        r'\b[Ff][Rr][Mm]\b': 'financial risk manager',
        r'\b[Vv]a[Rr]\b': 'value at risk',
    }

    # Subtitle separators
    SUBTITLE_SEPARATORS = [
        ': ',
        ' : ',
        ' - ',
        ' – ',  # en dash
        ' — ',  # em dash
        ' | ',
        '／',
        ' / ',
    ]

    # Articles to potentially remove
    ARTICLES = ['the', 'a', 'an']

    # Common words that might be dropped
    FILLER_WORDS = [
        'introduction', 'guide', 'handbook', 'manual', 'primer',
        'fundamentals', 'essentials', 'basics', 'principles',
        'comprehensive', 'complete', 'practical', 'advanced',
    ]

    def __init__(self, aggressive: bool = False):
        """
        Initialize normalizer.

        Args:
            aggressive: If True, apply more aggressive normalization
                       (remove subtitles, articles, etc.)
        """
        self.aggressive = aggressive

    def normalize(self, title: str) -> str:
        """
        Normalize a book title for comparison.

        Args:
            title: Original book title

        Returns:
            Normalized title string
        """
        if not title:
            return ""

        text = title

        # Step 1: Unicode normalization (NFKC - compatibility decomposition)
        text = unicodedata.normalize('NFKC', text)

        # Step 2: Full-width to half-width conversion
        text = self._fullwidth_to_halfwidth(text)

        # Step 3: Normalize brackets
        for full, half in self.BRACKET_MAP.items():
            text = text.replace(full, half)

        # Step 4: Convert kanji numbers to arabic
        for kanji, arabic in self.KANJI_NUMBERS.items():
            text = text.replace(kanji, arabic)

        # Step 5: Remove edition information
        for pattern in self.EDITION_PATTERNS:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)

        # Step 6: Lowercase
        text = text.lower()

        # Step 6: Normalize punctuation and symbols
        text = self._normalize_punctuation(text)

        # Step 7: Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()

        # Step 8: Remove content in brackets (often supplementary info)
        if self.aggressive:
            text = re.sub(r'\([^)]*\)', '', text)
            text = re.sub(r'\[[^\]]*\]', '', text)
            text = re.sub(r'<[^>]*>', '', text)

        # Step 9: Remove leading articles (aggressive mode)
        if self.aggressive:
            for article in self.ARTICLES:
                if text.startswith(article + ' '):
                    text = text[len(article) + 1:]

        # Step 10: Extract main title (before subtitle separator)
        if self.aggressive:
            for sep in self.SUBTITLE_SEPARATORS:
                if sep in text:
                    text = text.split(sep)[0]
                    break

        # Step 11: Remove all spaces for aggressive comparison (especially for Japanese)
        if self.aggressive:
            text = re.sub(r'\s+', '', text)
        else:
            # Final cleanup for non-aggressive mode
            text = re.sub(r'\s+', ' ', text).strip()

        return text

    def _fullwidth_to_halfwidth(self, text: str) -> str:
        """Convert full-width characters to half-width."""
        result = []
        for char in text:
            code = ord(char)
            # Full-width ASCII variants (！to ～)
            if 0xFF01 <= code <= 0xFF5E:
                result.append(chr(code - 0xFEE0))
            # Full-width space
            elif code == 0x3000:
                result.append(' ')
            else:
                result.append(char)
        return ''.join(result)

    def _normalize_punctuation(self, text: str) -> str:
        """Normalize punctuation marks."""
        # Normalize dashes
        text = re.sub(r'[‐‑‒–—―−]', '-', text)

        # Normalize quotes
        text = re.sub(r'[""「」『』]', '"', text)
        text = re.sub(r"[''`]", "'", text)

        # Normalize dots and commas
        text = text.replace('．', '.')
        text = text.replace('，', ',')
        text = text.replace('、', ',')
        text = text.replace('。', '.')

        # Normalize ampersand
        text = re.sub(r'\s*&\s*', ' and ', text)

        # Remove certain punctuation (Japanese mid-dot, colons, semicolons)
        text = re.sub(r'[・：；･]', ' ', text)

        # Normalize colons used in subtitles
        text = re.sub(r'\s*:\s*', ': ', text)

        return text

    def generate_canonical_key(self, title: str, author: Optional[str] = None) -> str:
        """
        Generate a canonical key for deduplication.

        Args:
            title: Book title
            author: First author name (optional)

        Returns:
            A hash-based canonical key
        """
        normalized_title = self.normalize(title)

        # For key generation, use aggressive normalization
        aggressive_normalizer = TitleNormalizer(aggressive=True)
        key_title = aggressive_normalizer.normalize(title)

        # Remove all non-alphanumeric characters for key
        key_title = re.sub(r'[^a-z0-9\u3040-\u309f\u30a0-\u30ff\u4e00-\u9fff]', '', key_title)

        key_parts = [key_title]

        if author:
            # Normalize author name
            normalized_author = self._normalize_author(author)
            key_parts.append(normalized_author)

        combined = '_'.join(key_parts)
        return hashlib.md5(combined.encode('utf-8')).hexdigest()[:16]

    def _normalize_author(self, author: str) -> str:
        """Normalize author name for matching."""
        # Unicode normalization
        name = unicodedata.normalize('NFKC', author)

        # Full-width to half-width
        name = self._fullwidth_to_halfwidth(name)

        # Lowercase
        name = name.lower()

        # Remove titles and suffixes
        name = re.sub(r'\b(dr|prof|mr|ms|mrs|jr|sr|phd|md|cfa|cpa)\.?\b', '', name, flags=re.IGNORECASE)

        # Remove punctuation
        name = re.sub(r'[^\w\s]', '', name)

        # Normalize whitespace and get just alphanumeric
        name = re.sub(r'\s+', '', name)

        return name


class BookMatcher:
    """
    Match books across different sources with fuzzy matching support.
    """

    # Similarity thresholds
    EXACT_MATCH_THRESHOLD = 1.0
    HIGH_CONFIDENCE_THRESHOLD = 0.90
    MEDIUM_CONFIDENCE_THRESHOLD = 0.80
    LOW_CONFIDENCE_THRESHOLD = 0.70

    def __init__(self):
        self.normalizer = TitleNormalizer(aggressive=False)
        self.aggressive_normalizer = TitleNormalizer(aggressive=True)

    def match(
        self,
        title_a: str,
        title_b: str,
        author_a: Optional[str] = None,
        author_b: Optional[str] = None,
        isbn_a: Optional[str] = None,
        isbn_b: Optional[str] = None,
    ) -> MatchResult:
        """
        Determine if two books are the same work.

        Args:
            title_a: First book title
            title_b: Second book title
            author_a: First book's primary author
            author_b: Second book's primary author
            isbn_a: First book's ISBN
            isbn_b: Second book's ISBN

        Returns:
            MatchResult with match status and confidence
        """
        details: Dict[str, Any] = {}

        # Normalize titles
        norm_title_a = self.normalizer.normalize(title_a)
        norm_title_b = self.normalizer.normalize(title_b)

        agg_title_a = self.aggressive_normalizer.normalize(title_a)
        agg_title_b = self.aggressive_normalizer.normalize(title_b)

        # Check 1: ISBN match (highest confidence)
        if isbn_a and isbn_b:
            clean_isbn_a = self._clean_isbn(isbn_a)
            clean_isbn_b = self._clean_isbn(isbn_b)

            if clean_isbn_a and clean_isbn_b:
                # Same ISBN = same book
                if clean_isbn_a == clean_isbn_b:
                    return MatchResult(
                        is_match=True,
                        confidence=1.0,
                        match_type='isbn_exact',
                        normalized_title_a=norm_title_a,
                        normalized_title_b=norm_title_b,
                        details={'isbn_match': True}
                    )

                # Check ISBN-13 prefix match (same work, different edition/format)
                if len(clean_isbn_a) == 13 and len(clean_isbn_b) == 13:
                    if clean_isbn_a[:12] == clean_isbn_b[:12]:
                        details['isbn_prefix_match'] = True

        # Check 2: Exact normalized match
        if norm_title_a == norm_title_b:
            confidence = 0.95
            if self._authors_match(author_a, author_b):
                confidence = 1.0
            return MatchResult(
                is_match=True,
                confidence=confidence,
                match_type='normalized_exact',
                normalized_title_a=norm_title_a,
                normalized_title_b=norm_title_b,
                details=details
            )

        # Check 3: Aggressive normalized match
        if agg_title_a == agg_title_b:
            confidence = 0.90
            if self._authors_match(author_a, author_b):
                confidence = 0.95
            return MatchResult(
                is_match=True,
                confidence=confidence,
                match_type='aggressive_normalized',
                normalized_title_a=norm_title_a,
                normalized_title_b=norm_title_b,
                details=details
            )

        # Check 4: Fuzzy matching
        similarity = self._calculate_similarity(norm_title_a, norm_title_b)
        aggressive_similarity = self._calculate_similarity(agg_title_a, agg_title_b)

        details['title_similarity'] = similarity
        details['aggressive_similarity'] = aggressive_similarity

        # Use the higher similarity score
        best_similarity = max(similarity, aggressive_similarity)

        # Boost confidence if authors match
        author_match = self._authors_match(author_a, author_b)
        if author_match:
            best_similarity = min(1.0, best_similarity + 0.1)
            details['author_match'] = True

        # Determine match status
        if best_similarity >= self.HIGH_CONFIDENCE_THRESHOLD:
            return MatchResult(
                is_match=True,
                confidence=best_similarity,
                match_type='fuzzy_high',
                normalized_title_a=norm_title_a,
                normalized_title_b=norm_title_b,
                details=details
            )
        elif best_similarity >= self.MEDIUM_CONFIDENCE_THRESHOLD:
            # Medium confidence - likely match but needs review
            return MatchResult(
                is_match=True,
                confidence=best_similarity,
                match_type='fuzzy_medium',
                normalized_title_a=norm_title_a,
                normalized_title_b=norm_title_b,
                details=details
            )
        elif best_similarity >= self.LOW_CONFIDENCE_THRESHOLD:
            # Low confidence - possible match
            return MatchResult(
                is_match=False,  # Not auto-match, but flag for review
                confidence=best_similarity,
                match_type='fuzzy_low',
                normalized_title_a=norm_title_a,
                normalized_title_b=norm_title_b,
                details=details
            )
        else:
            return MatchResult(
                is_match=False,
                confidence=best_similarity,
                match_type='no_match',
                normalized_title_a=norm_title_a,
                normalized_title_b=norm_title_b,
                details=details
            )

    def _clean_isbn(self, isbn: str) -> Optional[str]:
        """Clean and validate ISBN."""
        if not isbn:
            return None

        # Remove hyphens and spaces
        clean = re.sub(r'[-\s]', '', isbn)

        # Validate format
        if re.match(r'^(97[89])?\d{9}[\dX]$', clean, re.IGNORECASE):
            return clean.upper()

        return None

    def _authors_match(self, author_a: Optional[str], author_b: Optional[str]) -> bool:
        """Check if two author names likely refer to the same person."""
        if not author_a or not author_b:
            return False

        norm_a = self.normalizer._normalize_author(author_a)
        norm_b = self.normalizer._normalize_author(author_b)

        if not norm_a or not norm_b:
            return False

        # Exact match after normalization
        if norm_a == norm_b:
            return True

        # One contains the other (partial name match)
        if norm_a in norm_b or norm_b in norm_a:
            return True

        # Fuzzy match for author names
        similarity = SequenceMatcher(None, norm_a, norm_b).ratio()
        return similarity >= 0.85

    def _calculate_similarity(self, text_a: str, text_b: str) -> float:
        """Calculate similarity between two strings."""
        if not text_a or not text_b:
            return 0.0

        # Use SequenceMatcher for basic similarity
        base_similarity = SequenceMatcher(None, text_a, text_b).ratio()

        # Also check token-based similarity (word overlap)
        tokens_a = set(text_a.split())
        tokens_b = set(text_b.split())

        if tokens_a and tokens_b:
            intersection = tokens_a & tokens_b
            union = tokens_a | tokens_b
            jaccard = len(intersection) / len(union)
        else:
            jaccard = 0.0

        # Combined score (weighted average)
        return 0.6 * base_similarity + 0.4 * jaccard

    def find_matches(
        self,
        new_book: Dict[str, Any],
        existing_books: List[Dict[str, Any]],
        threshold: float = 0.80
    ) -> List[Tuple[Dict[str, Any], MatchResult]]:
        """
        Find potential matches for a new book in existing collection.

        Args:
            new_book: New book record with 'title', 'authors', 'isbn' fields
            existing_books: List of existing book records
            threshold: Minimum confidence threshold for matches

        Returns:
            List of (existing_book, match_result) tuples sorted by confidence
        """
        matches = []

        new_title = new_book.get('title', '')
        new_author = new_book.get('authors', [None])[0] if new_book.get('authors') else None

        # Get ISBN from various possible locations
        new_isbn = self._extract_isbn(new_book)

        for existing in existing_books:
            existing_title = existing.get('title', '')
            existing_author = existing.get('authors', [None])[0] if existing.get('authors') else None
            existing_isbn = self._extract_isbn(existing)

            result = self.match(
                title_a=new_title,
                title_b=existing_title,
                author_a=new_author,
                author_b=existing_author,
                isbn_a=new_isbn,
                isbn_b=existing_isbn
            )

            if result.confidence >= threshold:
                matches.append((existing, result))

        # Sort by confidence descending
        matches.sort(key=lambda x: x[1].confidence, reverse=True)

        return matches

    def _extract_isbn(self, book: Dict[str, Any]) -> Optional[str]:
        """Extract ISBN from various record formats."""
        # v2 format: nested in editions -> formats
        if 'editions' in book:
            for edition in book.get('editions', []):
                for fmt in edition.get('formats', []):
                    if fmt.get('isbn'):
                        return fmt['isbn']

        # v1 format
        if 'isbn_or_issn' in book:
            return book['isbn_or_issn']

        return None


def create_dedup_index(books: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Create an index for efficient deduplication lookup.

    Args:
        books: List of book records

    Returns:
        Dictionary mapping canonical keys to lists of books
    """
    normalizer = TitleNormalizer(aggressive=True)
    index: Dict[str, List[Dict[str, Any]]] = {}

    for book in books:
        title = book.get('title', '')
        authors = book.get('authors', [])
        first_author = authors[0] if authors else None

        key = normalizer.generate_canonical_key(title, first_author)

        if key not in index:
            index[key] = []
        index[key].append(book)

    return index


class BookIndex:
    """
    High-performance index for book deduplication.

    Uses multiple index strategies for O(1) to O(k) lookups instead of O(N).
    Fuzzy matching is only applied to a small candidate set.
    """

    def __init__(self, books: Optional[List[Dict[str, Any]]] = None):
        self.normalizer = TitleNormalizer(aggressive=False)
        self.aggressive_normalizer = TitleNormalizer(aggressive=True)
        self.matcher = BookMatcher()

        # Index structures
        self._books: List[Dict[str, Any]] = []
        self._isbn_index: Dict[str, int] = {}  # isbn -> book index
        self._normalized_index: Dict[str, List[int]] = {}  # normalized title -> book indices
        self._aggressive_index: Dict[str, List[int]] = {}  # aggressive title -> book indices
        self._ngram_index: Dict[str, set] = {}  # 3-gram -> book indices

        if books:
            self.build(books)

    def build(self, books: List[Dict[str, Any]]) -> None:
        """
        Build all indices from a list of books.

        Args:
            books: List of book records
        """
        self._books = books
        self._isbn_index.clear()
        self._normalized_index.clear()
        self._aggressive_index.clear()
        self._ngram_index.clear()

        for idx, book in enumerate(books):
            self._index_book(idx, book)

    def _index_book(self, idx: int, book: Dict[str, Any]) -> None:
        """Index a single book at the given index."""
        title = book.get('title', '')

        # ISBN index
        isbn = self.matcher._extract_isbn(book)
        if isbn:
            clean_isbn = self.matcher._clean_isbn(isbn)
            if clean_isbn:
                self._isbn_index[clean_isbn] = idx
                # Also index ISBN-13 prefix for edition matching
                if len(clean_isbn) == 13:
                    self._isbn_index[clean_isbn[:12]] = idx

        # Normalized title index
        norm_title = self.normalizer.normalize(title)
        if norm_title:
            if norm_title not in self._normalized_index:
                self._normalized_index[norm_title] = []
            self._normalized_index[norm_title].append(idx)

        # Aggressive normalized title index
        agg_title = self.aggressive_normalizer.normalize(title)
        if agg_title:
            if agg_title not in self._aggressive_index:
                self._aggressive_index[agg_title] = []
            self._aggressive_index[agg_title].append(idx)

            # N-gram index for fuzzy matching (use 3-grams)
            for ngram in self._generate_ngrams(agg_title, n=3):
                if ngram not in self._ngram_index:
                    self._ngram_index[ngram] = set()
                self._ngram_index[ngram].add(idx)

    def _generate_ngrams(self, text: str, n: int = 3) -> List[str]:
        """Generate n-grams from text."""
        if len(text) < n:
            return [text] if text else []
        return [text[i:i+n] for i in range(len(text) - n + 1)]

    def add(self, book: Dict[str, Any]) -> int:
        """
        Add a new book to the index.

        Args:
            book: Book record to add

        Returns:
            Index of the added book
        """
        idx = len(self._books)
        self._books.append(book)
        self._index_book(idx, book)
        return idx

    def find_match(
        self,
        book: Dict[str, Any],
        fuzzy_threshold: float = 0.80
    ) -> Optional[Tuple[Dict[str, Any], MatchResult]]:
        """
        Find the best match for a book in the index.

        This is the main entry point for deduplication checks.
        Uses a tiered approach for performance:
        1. ISBN exact match (O(1))
        2. Normalized title exact match (O(1))
        3. Aggressive normalized match (O(1))
        4. N-gram candidate selection + fuzzy match (O(k) where k << N)

        Args:
            book: Book record to search for
            fuzzy_threshold: Minimum similarity for fuzzy matches

        Returns:
            Tuple of (matched_book, match_result) or None if no match
        """
        title = book.get('title', '')
        authors = book.get('authors', [])
        first_author = authors[0] if authors else None
        isbn = self.matcher._extract_isbn(book)

        # Tier 1: ISBN exact match
        if isbn:
            clean_isbn = self.matcher._clean_isbn(isbn)
            if clean_isbn:
                # Check exact ISBN
                if clean_isbn in self._isbn_index:
                    matched_idx = self._isbn_index[clean_isbn]
                    matched_book = self._books[matched_idx]
                    return (matched_book, MatchResult(
                        is_match=True,
                        confidence=1.0,
                        match_type='isbn_exact',
                        normalized_title_a=self.normalizer.normalize(title),
                        normalized_title_b=self.normalizer.normalize(matched_book.get('title', '')),
                        details={'isbn_match': True}
                    ))
                # Check ISBN-13 prefix
                if len(clean_isbn) == 13 and clean_isbn[:12] in self._isbn_index:
                    matched_idx = self._isbn_index[clean_isbn[:12]]
                    matched_book = self._books[matched_idx]
                    return (matched_book, MatchResult(
                        is_match=True,
                        confidence=0.98,
                        match_type='isbn_prefix',
                        normalized_title_a=self.normalizer.normalize(title),
                        normalized_title_b=self.normalizer.normalize(matched_book.get('title', '')),
                        details={'isbn_prefix_match': True}
                    ))

        # Tier 2: Normalized title exact match
        norm_title = self.normalizer.normalize(title)
        if norm_title in self._normalized_index:
            for matched_idx in self._normalized_index[norm_title]:
                matched_book = self._books[matched_idx]
                matched_author = matched_book.get('authors', [None])[0] if matched_book.get('authors') else None

                confidence = 0.95
                if self.matcher._authors_match(first_author, matched_author):
                    confidence = 1.0

                return (matched_book, MatchResult(
                    is_match=True,
                    confidence=confidence,
                    match_type='normalized_exact',
                    normalized_title_a=norm_title,
                    normalized_title_b=norm_title,
                    details={'author_match': confidence == 1.0}
                ))

        # Tier 3: Aggressive normalized match
        agg_title = self.aggressive_normalizer.normalize(title)
        if agg_title in self._aggressive_index:
            for matched_idx in self._aggressive_index[agg_title]:
                matched_book = self._books[matched_idx]
                matched_author = matched_book.get('authors', [None])[0] if matched_book.get('authors') else None

                confidence = 0.90
                if self.matcher._authors_match(first_author, matched_author):
                    confidence = 0.95

                return (matched_book, MatchResult(
                    is_match=True,
                    confidence=confidence,
                    match_type='aggressive_normalized',
                    normalized_title_a=norm_title,
                    normalized_title_b=self.normalizer.normalize(matched_book.get('title', '')),
                    details={'author_match': confidence == 0.95}
                ))

        # Tier 4: N-gram based candidate selection + fuzzy matching
        candidates = self._find_candidates_by_ngram(agg_title)

        if candidates:
            best_match = None
            best_result = None

            for matched_idx in candidates:
                matched_book = self._books[matched_idx]
                matched_author = matched_book.get('authors', [None])[0] if matched_book.get('authors') else None
                matched_isbn = self.matcher._extract_isbn(matched_book)

                result = self.matcher.match(
                    title_a=title,
                    title_b=matched_book.get('title', ''),
                    author_a=first_author,
                    author_b=matched_author,
                    isbn_a=isbn,
                    isbn_b=matched_isbn
                )

                if result.confidence >= fuzzy_threshold:
                    if best_result is None or result.confidence > best_result.confidence:
                        best_match = matched_book
                        best_result = result

            if best_match and best_result:
                return (best_match, best_result)

        return None

    def _find_candidates_by_ngram(self, agg_title: str, min_overlap: float = 0.3) -> set:
        """
        Find candidate books by n-gram overlap.

        Args:
            agg_title: Aggressively normalized title
            min_overlap: Minimum fraction of n-grams that must match

        Returns:
            Set of book indices that are potential matches
        """
        if not agg_title:
            return set()

        ngrams = self._generate_ngrams(agg_title, n=3)
        if not ngrams:
            return set()

        # Count how many n-grams each candidate shares
        candidate_counts: Dict[int, int] = {}
        for ngram in ngrams:
            if ngram in self._ngram_index:
                for idx in self._ngram_index[ngram]:
                    candidate_counts[idx] = candidate_counts.get(idx, 0) + 1

        # Filter candidates by minimum overlap
        min_matches = max(1, int(len(ngrams) * min_overlap))
        return {idx for idx, count in candidate_counts.items() if count >= min_matches}

    def find_all_matches(
        self,
        book: Dict[str, Any],
        threshold: float = 0.70
    ) -> List[Tuple[Dict[str, Any], MatchResult]]:
        """
        Find all potential matches above threshold (for review).

        Args:
            book: Book record to search for
            threshold: Minimum similarity threshold

        Returns:
            List of (matched_book, match_result) tuples sorted by confidence
        """
        title = book.get('title', '')
        agg_title = self.aggressive_normalizer.normalize(title)

        # Get candidates from n-gram index
        candidates = self._find_candidates_by_ngram(agg_title, min_overlap=0.2)

        # Also add normalized matches
        norm_title = self.normalizer.normalize(title)
        if norm_title in self._normalized_index:
            candidates.update(self._normalized_index[norm_title])
        if agg_title in self._aggressive_index:
            candidates.update(self._aggressive_index[agg_title])

        matches = []
        authors = book.get('authors', [])
        first_author = authors[0] if authors else None
        isbn = self.matcher._extract_isbn(book)

        for matched_idx in candidates:
            matched_book = self._books[matched_idx]
            matched_author = matched_book.get('authors', [None])[0] if matched_book.get('authors') else None
            matched_isbn = self.matcher._extract_isbn(matched_book)

            result = self.matcher.match(
                title_a=title,
                title_b=matched_book.get('title', ''),
                author_a=first_author,
                author_b=matched_author,
                isbn_a=isbn,
                isbn_b=matched_isbn
            )

            if result.confidence >= threshold:
                matches.append((matched_book, result))

        matches.sort(key=lambda x: x[1].confidence, reverse=True)
        return matches

    def __len__(self) -> int:
        return len(self._books)

    def stats(self) -> Dict[str, Any]:
        """Return index statistics."""
        return {
            'total_books': len(self._books),
            'isbn_entries': len(self._isbn_index),
            'normalized_titles': len(self._normalized_index),
            'aggressive_titles': len(self._aggressive_index),
            'ngram_entries': len(self._ngram_index),
        }


# Utility functions for command-line usage
def demo():
    """Demonstrate the matching functionality."""
    test_cases = [
        # Same book, different notation
        ("Investment Banking: Valuation, LBOs, M&A, and IPOs, 3rd Edition",
         "Investment Banking：Valuation，LBOs，M＆A，and IPOs（Third Edition）"),

        # Full-width vs half-width
        ("Ｆｉｎａｎｃｉａｌ　Ｒｉｓｋ　Ｍａｎａｇｅｍｅｎｔ",
         "Financial Risk Management"),

        # Japanese variations
        ("コーポレート・ファイナンス【第3版】",
         "コーポレートファイナンス 第三版"),

        # Subtitle variations
        ("Valuation: Measuring and Managing the Value of Companies",
         "Valuation - Measuring and Managing the Value of Companies"),

        # Article differences
        ("The Art of M&A",
         "Art of M&A"),

        # Different books (should not match)
        ("Corporate Finance",
         "Personal Finance"),
    ]

    matcher = BookMatcher()
    normalizer = TitleNormalizer()

    print("=" * 80)
    print("Book Matching Demo")
    print("=" * 80)

    for title_a, title_b in test_cases:
        print(f"\nTitle A: {title_a}")
        print(f"Title B: {title_b}")
        print(f"Normalized A: {normalizer.normalize(title_a)}")
        print(f"Normalized B: {normalizer.normalize(title_b)}")

        result = matcher.match(title_a, title_b)
        print(f"Match: {result.is_match} (confidence: {result.confidence:.2%})")
        print(f"Type: {result.match_type}")
        print("-" * 40)


def benchmark():
    """Benchmark BookIndex performance."""
    import time
    import random

    # Generate synthetic book data
    print("\n" + "=" * 80)
    print("BookIndex Performance Benchmark")
    print("=" * 80)

    base_titles = [
        "Investment Banking", "Corporate Finance", "Financial Risk Management",
        "Portfolio Theory", "Derivatives Pricing", "Options Trading",
        "Asset Management", "Hedge Fund Strategies", "Private Equity",
        "Venture Capital", "M&A Strategy", "Valuation Methods",
        "Credit Analysis", "Fixed Income", "Equity Research",
    ]

    subtitles = [
        "A Complete Guide", "Principles and Practice", "Theory and Applications",
        "Advanced Concepts", "Modern Approaches", "Strategic Perspectives",
    ]

    authors = [
        "John Smith", "Jane Doe", "Michael Johnson", "Sarah Williams",
        "Robert Brown", "Emily Davis", "David Miller", "Jennifer Wilson",
    ]

    # Generate 10,000 books
    print("\nGenerating 10,000 synthetic books...")
    books = []
    for i in range(10000):
        title = random.choice(base_titles)
        if random.random() > 0.5:
            title += f": {random.choice(subtitles)}"
        if random.random() > 0.7:
            title += f", {random.randint(2, 5)}th Edition"

        books.append({
            'title': title,
            'authors': [random.choice(authors)],
            'work_id': f'test_{i:05d}',
        })

    # Build index
    print("Building index...")
    start = time.time()
    index = BookIndex(books)
    build_time = time.time() - start
    print(f"Index built in {build_time:.3f}s")
    print(f"Index stats: {index.stats()}")

    # Test queries
    test_queries = [
        {'title': 'Investment Banking: A Complete Guide, 3rd Edition', 'authors': ['John Smith']},
        {'title': 'Ｉｎｖｅｓｔｍｅｎｔ　Ｂａｎｋｉｎｇ', 'authors': ['Jane Doe']},
        {'title': 'コーポレート・ファイナンス【第3版】', 'authors': []},
        {'title': 'Completely New Title That Does Not Exist', 'authors': ['Unknown']},
    ]

    print("\nRunning single query tests...")
    for query in test_queries:
        start = time.time()
        result = index.find_match(query)
        query_time = (time.time() - start) * 1000

        if result:
            matched_book, match_result = result
            print(f"  Query: '{query['title'][:50]}...'")
            print(f"    Match: '{matched_book['title'][:50]}...' ({match_result.confidence:.2%})")
            print(f"    Time: {query_time:.3f}ms")
        else:
            print(f"  Query: '{query['title'][:50]}...'")
            print(f"    No match found")
            print(f"    Time: {query_time:.3f}ms")

    # Bulk query benchmark
    print("\nBulk query benchmark (1000 queries)...")
    query_books = random.sample(books, 100) + [
        {'title': f'Random Title {i}', 'authors': ['Test']}
        for i in range(900)
    ]
    random.shuffle(query_books)

    start = time.time()
    matches_found = 0
    for query in query_books:
        result = index.find_match(query)
        if result:
            matches_found += 1
    bulk_time = time.time() - start

    print(f"  Total time: {bulk_time:.3f}s")
    print(f"  Avg per query: {bulk_time/1000*1000:.3f}ms")
    print(f"  Queries/second: {1000/bulk_time:.0f}")
    print(f"  Matches found: {matches_found}/1000")


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--benchmark':
        benchmark()
    else:
        demo()
