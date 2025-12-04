#!/usr/bin/env python3
"""
Automated review processor using LLM + WebSearch.

This script processes books_review.jsonl and automatically resolves
ambiguous records using web search and LLM verification.

Usage:
    # Dry-run (show what would be done)
    python scripts/processors/auto_review.py --dry-run

    # Process all review records
    python scripts/processors/auto_review.py

    # Process specific reasons only
    python scripts/processors/auto_review.py --reasons TITLE_MISMATCH AUTHOR_MISMATCH

    # Limit number of records
    python scripts/processors/auto_review.py --max-records 10
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum, auto

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from scripts.classifiers.segment_classifier import (
        LLMSegmentClassifier,
        BaseLLMClient,
        create_client,
        LLMProvider
    )
    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent.parent
MASTER_FILE = BASE_DIR / "data" / "master" / "books.jsonl"
REVIEW_FILE = BASE_DIR / "data" / "master" / "books_review.jsonl"
PROCESSED_FILE = BASE_DIR / "data" / "master" / ".auto_review_processed.json"
CHANGELOG_FILE = BASE_DIR / "data" / "master" / "auto_review_changelog.jsonl"


class ReviewAction(Enum):
    """Actions that can be taken on a review record."""
    KEEP_ORIGINAL = auto()      # Keep original data, discard suggestions
    APPLY_SUGGESTION = auto()   # Apply suggested changes
    DELETE_FROM_MASTER = auto() # Remove record from master
    SKIP = auto()               # Skip (needs manual review)
    UPDATE_SEGMENT = auto()     # Update segment/subsegment


@dataclass
class ReviewResult:
    """Result of automated review."""
    work_id: str
    title: str
    action: ReviewAction
    reason: str
    review_reasons: List[str] = field(default_factory=list)
    before_data: Dict[str, Any] = field(default_factory=dict)
    after_data: Dict[str, Any] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)
    updated_data: Optional[Dict[str, Any]] = None

    def to_changelog_dict(self) -> Dict[str, Any]:
        """Convert to changelog entry."""
        from datetime import datetime
        return {
            'timestamp': datetime.now().isoformat(),
            'work_id': self.work_id,
            'title': self.title,
            'action': self.action.name,
            'review_reasons': self.review_reasons,
            'reason': self.reason,
            'before': self.before_data,
            'after': self.after_data,
            'llm_analysis': self.details
        }


class WebSearcher:
    """Simple web search using DuckDuckGo or Google."""

    def __init__(self, delay: float = 2.0):
        self.delay = delay
        self.last_request = 0.0

    def _wait(self):
        elapsed = time.time() - self.last_request
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request = time.time()

    def search(self, query: str, num_results: int = 5) -> List[Dict[str, str]]:
        """
        Search the web for a query.
        Returns list of {title, url, snippet} dicts.
        """
        if not REQUESTS_AVAILABLE:
            logger.warning("requests not available for web search")
            return []

        self._wait()

        # Use DuckDuckGo HTML search (no API key needed)
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            results = []
            # Simple regex parsing of DuckDuckGo HTML results
            pattern = r'<a rel="nofollow" class="result__a" href="([^"]+)"[^>]*>([^<]+)</a>'
            snippet_pattern = r'<a class="result__snippet"[^>]*>([^<]+)</a>'

            matches = re.findall(pattern, response.text)
            snippets = re.findall(snippet_pattern, response.text)

            for i, (href, title) in enumerate(matches[:num_results]):
                snippet = snippets[i] if i < len(snippets) else ""
                results.append({
                    'url': href,
                    'title': title.strip(),
                    'snippet': snippet.strip()
                })

            return results

        except Exception as e:
            logger.warning(f"Web search failed: {e}")
            return []


class AutoReviewer:
    """Automated review processor using LLM + WebSearch."""

    def __init__(
        self,
        llm_client: BaseLLMClient,
        web_searcher: WebSearcher,
        dry_run: bool = False
    ):
        self.llm = llm_client
        self.searcher = web_searcher
        self.dry_run = dry_run

        # Statistics
        self.stats = {
            'processed': 0,
            'kept_original': 0,
            'applied_suggestion': 0,
            'deleted': 0,
            'skipped': 0,
            'updated_segment': 0,
            'errors': 0
        }

    def process_review(self, review: Dict[str, Any]) -> ReviewResult:
        """Process a single review record."""
        work_id = review.get('work_id', '')
        title = review.get('title', '')
        reasons = review.get('reasons', [])
        original = review.get('original_data', {})

        logger.info(f"Processing: {title[:50]}... (reasons: {reasons})")

        try:
            # Determine primary reason and route to appropriate handler
            if 'NO_ISBN_FOUND' in reasons or 'MISSING_CRITICAL_DATA' in reasons:
                result = self._handle_missing_data(review)
            elif 'TITLE_MISMATCH' in reasons or 'AUTHOR_MISMATCH' in reasons:
                result = self._handle_mismatch(review)
            elif 'SEGMENT_CHANGE' in reasons:
                result = self._handle_segment_change(review)
            elif 'LOW_CONFIDENCE_CLASSIFICATION' in reasons:
                result = self._handle_low_confidence(review)
            elif 'MULTIPLE_CLASSIFICATION_MATCHES' in reasons:
                result = self._handle_multiple_matches(review)
            else:
                result = ReviewResult(
                    work_id=work_id,
                    title=title,
                    action=ReviewAction.SKIP,
                    reason=f"Unknown review reasons: {reasons}",
                    review_reasons=reasons,
                    before_data=original
                )

            # Ensure title and review_reasons are set
            result.title = title
            result.review_reasons = reasons
            if not result.before_data:
                result.before_data = original

            return result

        except Exception as e:
            logger.error(f"Error processing {work_id}: {e}")
            self.stats['errors'] += 1
            return ReviewResult(
                work_id=work_id,
                title=title,
                action=ReviewAction.SKIP,
                reason=f"Processing error: {e}",
                review_reasons=reasons,
                before_data=original
            )

    def _handle_missing_data(self, review: Dict[str, Any]) -> ReviewResult:
        """Handle NO_ISBN_FOUND / MISSING_CRITICAL_DATA."""
        work_id = review.get('work_id', '')
        original = review.get('original_data', {})
        title = original.get('title', '')
        authors = original.get('authors', [])
        publisher = original.get('publisher', '')
        language = original.get('language', 'en')

        # Build search query
        author_str = authors[0] if authors else ""
        if language == 'ja':
            query = f"{title} {author_str} 書籍"
        else:
            query = f'"{title}" {author_str} book ISBN'

        # Search the web
        search_results = self.searcher.search(query)

        if not search_results:
            # No search results - might not exist
            return self._verify_book_exists(review, [])

        # Use LLM to analyze search results
        return self._analyze_missing_data_results(review, search_results)

    def _analyze_missing_data_results(
        self,
        review: Dict[str, Any],
        search_results: List[Dict[str, str]]
    ) -> ReviewResult:
        """Analyze search results for missing data case."""
        work_id = review.get('work_id', '')
        original = review.get('original_data', {})
        title = original.get('title', '')
        authors = original.get('authors', [])
        segment = original.get('segment', '')

        # Format search results for LLM
        results_text = "\n".join([
            f"- {r['title']}: {r['snippet'][:200]}"
            for r in search_results[:5]
        ])

        prompt = f"""以下の書籍について、Web検索結果を分析して判定してください。

# 書籍情報
タイトル: {title}
著者: {', '.join(authors) if authors else '不明'}
セグメント: {segment}

# Web検索結果
{results_text}

# 判定基準
1. 書籍が実在するか（検索結果に書籍情報があるか）
2. 金融・経済・ビジネス関連の書籍か
3. ゲーム攻略本・漫画・小説など、データセットに不適切な書籍ではないか

# 出力形式（JSON）
```json
{{
  "exists": true/false,
  "is_finance_related": true/false,
  "is_inappropriate": true/false,
  "inappropriate_reason": "不適切な理由（該当する場合）",
  "found_isbn": "見つかったISBN（あれば）",
  "confidence": 0.0-1.0,
  "reasoning": "判定理由"
}}
```"""

        try:
            response = self.llm.call(prompt)
            result_data = self._parse_json_response(response)

            if result_data.get('is_inappropriate', False):
                return ReviewResult(
                    work_id=work_id,
                    title=title,
                    action=ReviewAction.DELETE_FROM_MASTER,
                    reason=f"不適切な書籍: {result_data.get('inappropriate_reason', '')}",
                    before_data=original,
                    after_data={'action': 'DELETED'},
                    details=result_data
                )

            if not result_data.get('exists', True):
                return ReviewResult(
                    work_id=work_id,
                    title=title,
                    action=ReviewAction.DELETE_FROM_MASTER,
                    reason="書籍の実在が確認できない",
                    before_data=original,
                    after_data={'action': 'DELETED'},
                    details=result_data
                )

            if not result_data.get('is_finance_related', True):
                return ReviewResult(
                    work_id=work_id,
                    title=title,
                    action=ReviewAction.DELETE_FROM_MASTER,
                    reason="金融・経済関連ではない書籍",
                    before_data=original,
                    after_data={'action': 'DELETED'},
                    details=result_data
                )

            # Book exists and is appropriate - keep with any found data
            updated = None
            after = {'action': 'KEPT'}
            if result_data.get('found_isbn'):
                updated = {'isbn': result_data['found_isbn']}
                after['added_isbn'] = result_data['found_isbn']

            return ReviewResult(
                work_id=work_id,
                title=title,
                action=ReviewAction.KEEP_ORIGINAL,
                reason="書籍の実在を確認、データを維持",
                before_data=original,
                after_data=after,
                details=result_data,
                updated_data=updated
            )

        except Exception as e:
            logger.warning(f"LLM analysis failed: {e}")
            return ReviewResult(
                work_id=work_id,
                title=title,
                action=ReviewAction.SKIP,
                reason=f"LLM分析失敗: {e}",
                before_data=original
            )

    def _verify_book_exists(
        self,
        review: Dict[str, Any],
        search_results: List[Dict[str, str]]
    ) -> ReviewResult:
        """Verify if book exists when no search results."""
        work_id = review.get('work_id', '')
        original = review.get('original_data', {})
        title = original.get('title', '')

        # If no search results and known problematic patterns
        problematic_patterns = [
            'バイオハザード', 'ファイナルファンタジー', 'ドラゴンクエスト',
            'ゲーム', 'RPG', 'サプリメント', 'TRPG'
        ]

        for pattern in problematic_patterns:
            if pattern in title:
                return ReviewResult(
                    work_id=work_id,
                    title=title,
                    action=ReviewAction.DELETE_FROM_MASTER,
                    reason=f"ゲーム関連書籍と推定: {pattern}",
                    before_data=original,
                    after_data={'action': 'DELETED', 'matched_pattern': pattern},
                    details={'matched_pattern': pattern}
                )

        # Can't confirm - skip for manual review
        return ReviewResult(
            work_id=work_id,
            title=title,
            action=ReviewAction.SKIP,
            reason="検索結果なし、手動確認が必要",
            before_data=original
        )

    def _handle_mismatch(self, review: Dict[str, Any]) -> ReviewResult:
        """Handle TITLE_MISMATCH / AUTHOR_MISMATCH."""
        work_id = review.get('work_id', '')
        original = review.get('original_data', {})
        details = review.get('details', {})
        suggested = review.get('suggested_changes', {})

        title = original.get('title', '')
        authors = original.get('authors', [])

        # Get mismatch info
        api_title = details.get('api_title', '')
        api_authors = details.get('api_authors', [])

        # Search for the original title
        query = f'"{title}" {authors[0] if authors else ""} book'
        search_results = self.searcher.search(query)

        # Use LLM to determine which is correct
        return self._analyze_mismatch_results(review, search_results)

    def _analyze_mismatch_results(
        self,
        review: Dict[str, Any],
        search_results: List[Dict[str, str]]
    ) -> ReviewResult:
        """Analyze mismatch using search results."""
        work_id = review.get('work_id', '')
        original = review.get('original_data', {})
        details = review.get('details', {})
        suggested = review.get('suggested_changes', {})

        title = original.get('title', '')
        authors = original.get('authors', [])
        api_title = details.get('api_title', '')
        api_authors = details.get('api_authors', [])

        results_text = "\n".join([
            f"- {r['title']}: {r['snippet'][:200]}"
            for r in search_results[:5]
        ]) if search_results else "検索結果なし"

        prompt = f"""以下の書籍情報の不一致を分析してください。

# 元のデータ
タイトル: {title}
著者: {', '.join(authors) if authors else '不明'}

# API取得データ
タイトル: {api_title}
著者: {', '.join(api_authors) if api_authors else '不明'}

# Web検索結果
{results_text}

# 判定
1. 元のタイトル/著者が正しいか
2. API取得のタイトル/著者が正しいか
3. 両方とも同じ書籍を指しているか（表記揺れ）
4. 全く別の書籍か

# 出力形式（JSON）
```json
{{
  "original_correct": true/false,
  "api_correct": true/false,
  "same_book": true/false,
  "is_notation_variation": true/false,
  "recommended_action": "keep_original" / "apply_api" / "delete",
  "confidence": 0.0-1.0,
  "reasoning": "判定理由"
}}
```"""

        try:
            response = self.llm.call(prompt)
            result_data = self._parse_json_response(response)

            action_map = {
                'keep_original': ReviewAction.KEEP_ORIGINAL,
                'apply_api': ReviewAction.APPLY_SUGGESTION,
                'delete': ReviewAction.DELETE_FROM_MASTER
            }

            recommended = result_data.get('recommended_action', 'keep_original')
            action = action_map.get(recommended, ReviewAction.SKIP)

            # If it's just notation variation (e.g., accent differences)
            if result_data.get('is_notation_variation', False):
                action = ReviewAction.KEEP_ORIGINAL

            # Build before/after data
            before = {
                'title': title,
                'authors': authors,
            }
            after = {}
            if action == ReviewAction.APPLY_SUGGESTION:
                after = {
                    'title': api_title if api_title else title,
                    'authors': api_authors if api_authors else authors,
                    'action': 'APPLIED_API_DATA'
                }
            elif action == ReviewAction.DELETE_FROM_MASTER:
                after = {'action': 'DELETED'}
            else:
                after = {'action': 'KEPT_ORIGINAL'}

            return ReviewResult(
                work_id=work_id,
                title=title,
                action=action,
                reason=result_data.get('reasoning', ''),
                before_data=before,
                after_data=after,
                details=result_data,
                updated_data=suggested if action == ReviewAction.APPLY_SUGGESTION else None
            )

        except Exception as e:
            logger.warning(f"LLM analysis failed: {e}")
            return ReviewResult(
                work_id=work_id,
                title=title,
                action=ReviewAction.SKIP,
                reason=f"LLM分析失敗: {e}",
                before_data={'title': title, 'authors': authors}
            )

    def _handle_segment_change(self, review: Dict[str, Any]) -> ReviewResult:
        """Handle SEGMENT_CHANGE - auto-apply if high confidence."""
        work_id = review.get('work_id', '')
        original = review.get('original_data', {})
        suggested = review.get('suggested_changes', {})
        details = review.get('details', {})
        title = original.get('title', '')

        confidence = suggested.get('confidence', 0.0)
        old_segment = original.get('segment', '')
        old_subsegment = original.get('subsegment', '')
        new_segment = suggested.get('segment', '')
        new_subsegment = suggested.get('subsegment', '')

        before = {
            'segment': old_segment,
            'subsegment': old_subsegment
        }

        if confidence >= 0.7:
            after = {
                'segment': new_segment,
                'subsegment': new_subsegment,
                'action': 'SEGMENT_UPDATED'
            }
            return ReviewResult(
                work_id=work_id,
                title=title,
                action=ReviewAction.UPDATE_SEGMENT,
                reason=f"セグメント変更を適用 (信頼度: {confidence:.2f})",
                before_data=before,
                after_data=after,
                details=details,
                updated_data={
                    'segment': new_segment,
                    'subsegment': new_subsegment
                }
            )
        else:
            return ReviewResult(
                work_id=work_id,
                title=title,
                action=ReviewAction.SKIP,
                reason=f"信頼度が低い ({confidence:.2f})",
                before_data=before
            )

    def _handle_low_confidence(self, review: Dict[str, Any]) -> ReviewResult:
        """Handle LOW_CONFIDENCE_CLASSIFICATION."""
        # For low confidence, use web search to get more context
        work_id = review.get('work_id', '')
        original = review.get('original_data', {})
        title = original.get('title', '')
        segment = original.get('segment', '')
        subsegment = original.get('subsegment', '')

        before = {
            'segment': segment,
            'subsegment': subsegment
        }

        query = f'"{title}" finance economics book'
        search_results = self.searcher.search(query)

        if not search_results:
            return ReviewResult(
                work_id=work_id,
                title=title,
                action=ReviewAction.SKIP,
                reason="追加情報が見つからない",
                before_data=before
            )

        # Re-classify with search context
        return self._reclassify_with_context(review, search_results)

    def _handle_multiple_matches(self, review: Dict[str, Any]) -> ReviewResult:
        """Handle MULTIPLE_CLASSIFICATION_MATCHES."""
        # Similar to low confidence - use web search for more context
        return self._handle_low_confidence(review)

    def _reclassify_with_context(
        self,
        review: Dict[str, Any],
        search_results: List[Dict[str, str]]
    ) -> ReviewResult:
        """Re-classify using web search context."""
        work_id = review.get('work_id', '')
        original = review.get('original_data', {})
        suggested = review.get('suggested_changes', {})

        title = original.get('title', '')
        current_segment = original.get('segment', '')
        current_subsegment = original.get('subsegment', '')
        suggested_segment = suggested.get('segment', '')
        suggested_subsegment = suggested.get('subsegment', '')

        before = {
            'segment': current_segment,
            'subsegment': current_subsegment
        }

        results_text = "\n".join([
            f"- {r['title']}: {r['snippet'][:200]}"
            for r in search_results[:5]
        ])

        prompt = f"""以下の書籍の分類を確認してください。

# 書籍情報
タイトル: {title}
現在のセグメント: {current_segment}
提案されたセグメント: {suggested_segment} / {suggested_subsegment}

# Web検索結果
{results_text}

# 判定
検索結果を参考に、どちらのセグメントが適切か判定してください。

# 出力形式（JSON）
```json
{{
  "keep_current": true/false,
  "apply_suggestion": true/false,
  "confidence": 0.0-1.0,
  "reasoning": "判定理由"
}}
```"""

        try:
            response = self.llm.call(prompt)
            result_data = self._parse_json_response(response)

            if result_data.get('apply_suggestion', False) and result_data.get('confidence', 0) >= 0.7:
                after = {
                    'segment': suggested_segment,
                    'subsegment': suggested_subsegment,
                    'action': 'SEGMENT_UPDATED'
                }
                return ReviewResult(
                    work_id=work_id,
                    title=title,
                    action=ReviewAction.UPDATE_SEGMENT,
                    reason=result_data.get('reasoning', ''),
                    before_data=before,
                    after_data=after,
                    details=result_data,
                    updated_data={
                        'segment': suggested_segment,
                        'subsegment': suggested_subsegment
                    }
                )
            else:
                return ReviewResult(
                    work_id=work_id,
                    title=title,
                    action=ReviewAction.KEEP_ORIGINAL,
                    reason=result_data.get('reasoning', ''),
                    before_data=before,
                    after_data={'action': 'KEPT_ORIGINAL'},
                    details=result_data
                )

        except Exception as e:
            return ReviewResult(
                work_id=work_id,
                title=title,
                action=ReviewAction.SKIP,
                reason=f"LLM分析失敗: {e}",
                before_data=before
            )

    def _parse_json_response(self, response: str) -> Dict[str, Any]:
        """Parse JSON from LLM response."""
        # Try to find JSON block
        json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            # Try to find raw JSON
            json_match = re.search(r'\{[^{}]*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
            else:
                raise ValueError("No JSON found in response")

        return json.loads(json_str)


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


def load_processed_ids(path: Path) -> set:
    """Load already processed work_ids."""
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return set(data.get('processed_ids', []))
    return set()


def save_processed_ids(ids: set, path: Path) -> None:
    """Save processed work_ids."""
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({'processed_ids': list(ids)}, f, ensure_ascii=False, indent=2)


def append_changelog(results: List[ReviewResult], path: Path) -> int:
    """Append review results to changelog file.

    Returns number of entries written.
    """
    # Filter out SKIPs with no meaningful data
    meaningful_results = [
        r for r in results
        if r.action != ReviewAction.SKIP or r.before_data or r.after_data
    ]

    if not meaningful_results:
        return 0

    with open(path, 'a', encoding='utf-8') as f:
        for result in meaningful_results:
            entry = result.to_changelog_dict()
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')

    return len(meaningful_results)


def main():
    parser = argparse.ArgumentParser(description='Automated review processor')
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    parser.add_argument(
        '--reasons',
        nargs='+',
        choices=[
            'TITLE_MISMATCH', 'AUTHOR_MISMATCH', 'NO_ISBN_FOUND',
            'MISSING_CRITICAL_DATA', 'SEGMENT_CHANGE',
            'LOW_CONFIDENCE_CLASSIFICATION', 'MULTIPLE_CLASSIFICATION_MATCHES'
        ],
        help='Only process specific reasons'
    )
    parser.add_argument(
        '--max-records',
        type=int,
        help='Maximum number of records to process'
    )
    parser.add_argument(
        '--llm-provider',
        choices=['claude', 'openai', 'gemini'],
        default='claude',
        help='LLM provider (default: claude)'
    )
    parser.add_argument(
        '--reset-progress',
        action='store_true',
        help='Reset processed IDs and start fresh'
    )
    parser.add_argument(
        '--master',
        type=Path,
        default=MASTER_FILE,
        help=f'Master file path (default: {MASTER_FILE})'
    )
    parser.add_argument(
        '--review-file',
        type=Path,
        default=REVIEW_FILE,
        help=f'Review file path (default: {REVIEW_FILE})'
    )

    args = parser.parse_args()

    if not LLM_AVAILABLE:
        logger.error("LLM classifier not available. Install anthropic/openai/google-generativeai.")
        sys.exit(1)

    # Load data
    logger.info(f"Loading master from {args.master}")
    master_records = load_jsonl(args.master)
    logger.info(f"  Loaded {len(master_records)} records")

    logger.info(f"Loading reviews from {args.review_file}")
    review_records = load_jsonl(args.review_file)
    logger.info(f"  Loaded {len(review_records)} review records")

    if not review_records:
        logger.info("No review records to process")
        return

    # Load processed IDs
    if args.reset_progress:
        processed_ids = set()
    else:
        processed_ids = load_processed_ids(PROCESSED_FILE)
        logger.info(f"  Already processed: {len(processed_ids)}")

    # Filter reviews
    pending_reviews = [
        r for r in review_records
        if r.get('work_id') not in processed_ids
    ]

    if args.reasons:
        pending_reviews = [
            r for r in pending_reviews
            if any(reason in r.get('reasons', []) for reason in args.reasons)
        ]

    if args.max_records:
        pending_reviews = pending_reviews[:args.max_records]

    logger.info(f"  Pending reviews: {len(pending_reviews)}")

    if not pending_reviews:
        logger.info("No pending reviews to process")
        return

    # Initialize components
    logger.info(f"Initializing LLM client ({args.llm_provider})...")
    llm_client = create_client(args.llm_provider)
    web_searcher = WebSearcher(delay=2.0)

    reviewer = AutoReviewer(
        llm_client=llm_client,
        web_searcher=web_searcher,
        dry_run=args.dry_run
    )

    # Create master lookup
    master_lookup = {r.get('work_id'): r for r in master_records}

    # Process reviews
    results: List[ReviewResult] = []
    deleted_ids: set = set()
    updated_ids: set = set()

    try:
        for i, review in enumerate(pending_reviews):
            work_id = review.get('work_id', '')

            logger.info(f"\n[{i+1}/{len(pending_reviews)}] Processing {work_id}")
            result = reviewer.process_review(review)
            results.append(result)

            # Log result with before/after details
            logger.info(f"  Title: {result.title[:60]}..." if len(result.title) > 60 else f"  Title: {result.title}")
            logger.info(f"  Action: {result.action.name}")
            logger.info(f"  Reason: {result.reason}")
            if result.before_data:
                logger.info(f"  Before: {json.dumps(result.before_data, ensure_ascii=False)}")
            if result.after_data:
                logger.info(f"  After:  {json.dumps(result.after_data, ensure_ascii=False)}")

            if not args.dry_run:
                # Apply action to master
                if result.action == ReviewAction.DELETE_FROM_MASTER:
                    deleted_ids.add(work_id)
                    reviewer.stats['deleted'] += 1
                elif result.action == ReviewAction.KEEP_ORIGINAL:
                    reviewer.stats['kept_original'] += 1
                elif result.action == ReviewAction.APPLY_SUGGESTION:
                    if work_id in master_lookup and result.updated_data:
                        master_lookup[work_id].update(result.updated_data)
                        updated_ids.add(work_id)
                    reviewer.stats['applied_suggestion'] += 1
                elif result.action == ReviewAction.UPDATE_SEGMENT:
                    if work_id in master_lookup and result.updated_data:
                        master_lookup[work_id].update(result.updated_data)
                        updated_ids.add(work_id)
                    reviewer.stats['updated_segment'] += 1
                elif result.action == ReviewAction.SKIP:
                    reviewer.stats['skipped'] += 1

                processed_ids.add(work_id)

            reviewer.stats['processed'] += 1

    except KeyboardInterrupt:
        logger.info("\n\nInterrupted by user")

    # Summary
    print(f"\n{'=' * 60}")
    print("Auto Review Summary")
    print(f"{'=' * 60}")
    print(f"  Processed: {reviewer.stats['processed']}")
    print(f"  Kept original: {reviewer.stats['kept_original']}")
    print(f"  Applied suggestion: {reviewer.stats['applied_suggestion']}")
    print(f"  Updated segment: {reviewer.stats['updated_segment']}")
    print(f"  Deleted: {reviewer.stats['deleted']}")
    print(f"  Skipped: {reviewer.stats['skipped']}")
    print(f"  Errors: {reviewer.stats['errors']}")

    if not args.dry_run:
        # Save updated master (excluding deleted records)
        if deleted_ids or updated_ids:
            new_master = [
                r for r in master_records
                if r.get('work_id') not in deleted_ids
            ]

            # Update with changes
            for work_id in updated_ids:
                if work_id in master_lookup:
                    for i, r in enumerate(new_master):
                        if r.get('work_id') == work_id:
                            new_master[i] = master_lookup[work_id]
                            break

            save_jsonl(new_master, args.master)
            logger.info(f"\nSaved master ({len(master_records)} -> {len(new_master)} records)")

        # Update review file (remove processed records)
        remaining_reviews = [
            r for r in review_records
            if r.get('work_id') not in processed_ids
        ]
        save_jsonl(remaining_reviews, args.review_file)
        logger.info(f"Updated review file ({len(review_records)} -> {len(remaining_reviews)} records)")

        # Save progress
        save_processed_ids(processed_ids, PROCESSED_FILE)
        logger.info(f"Saved progress ({len(processed_ids)} processed IDs)")

        # Save changelog
        changelog_count = append_changelog(results, CHANGELOG_FILE)
        if changelog_count > 0:
            logger.info(f"Appended {changelog_count} entries to changelog: {CHANGELOG_FILE}")
    else:
        print("\n[DRY-RUN] No changes were made")

        # Show what would be deleted
        if any(r.action == ReviewAction.DELETE_FROM_MASTER for r in results):
            print("\nRecords that would be deleted:")
            for r in results:
                if r.action == ReviewAction.DELETE_FROM_MASTER:
                    print(f"  - {r.work_id}: {r.reason}")

        # Show what would be updated
        if any(r.action in (ReviewAction.UPDATE_SEGMENT, ReviewAction.APPLY_SUGGESTION) for r in results):
            print("\nRecords that would be updated:")
            for r in results:
                if r.action in (ReviewAction.UPDATE_SEGMENT, ReviewAction.APPLY_SUGGESTION):
                    print(f"  - {r.work_id} ({r.title[:40]}...)")
                    if r.before_data and r.after_data:
                        print(f"    Before: {r.before_data}")
                        print(f"    After:  {r.after_data}")


if __name__ == '__main__':
    main()
