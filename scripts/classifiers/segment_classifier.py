#!/usr/bin/env python3
"""
LLM-based segment classifier for financial books.

Supports multiple LLM providers:
- Claude (Anthropic)
- ChatGPT (OpenAI)
- Gemini (Google)
"""
from __future__ import annotations

import json
import logging
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

logger = logging.getLogger(__name__)

# Default paths
DEFAULT_SEGMENTS_FILE = Path(__file__).parent.parent.parent / 'data' / 'config' / 'segments.jsonl'
DEFAULT_SECRETS_FILE = Path(__file__).parent.parent.parent / 'secrets.json'

# Cache for secrets
_secrets_cache: Optional[Dict[str, str]] = None


def load_secrets(path: Optional[Path] = None) -> Dict[str, str]:
    """Load API keys from secrets.json file."""
    global _secrets_cache
    if _secrets_cache is not None:
        return _secrets_cache

    path = path or DEFAULT_SECRETS_FILE
    if path.exists():
        try:
            with open(path, 'r', encoding='utf-8') as f:
                _secrets_cache = json.load(f)
                return _secrets_cache
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load secrets from {path}: {e}")
    _secrets_cache = {}
    return _secrets_cache


def get_api_key(key_name: str, secrets_path: Optional[Path] = None) -> Optional[str]:
    """Get API key from environment or secrets file."""
    # First check environment
    value = os.environ.get(key_name)
    if value:
        return value
    # Then check secrets file
    secrets = load_secrets(secrets_path)
    return secrets.get(key_name)


class LLMProvider(Enum):
    """Supported LLM providers."""
    CLAUDE = "claude"
    OPENAI = "openai"
    GEMINI = "gemini"


@dataclass
class ClassificationResult:
    """Result of segment classification."""
    segment: str
    subsegment: str
    confidence: float
    reasoning: str
    is_valid: bool = True
    error: Optional[str] = None
    provider: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'segment': self.segment,
            'subsegment': self.subsegment,
            'confidence': self.confidence,
            'reasoning': self.reasoning,
            'is_valid': self.is_valid,
            'error': self.error,
            'provider': self.provider
        }


class BaseLLMClient(ABC):
    """Abstract base class for LLM clients."""

    @abstractmethod
    def call(self, prompt: str) -> str:
        """Call LLM with prompt and return response text."""
        pass

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Return provider name."""
        pass


class ClaudeClient(BaseLLMClient):
    """Claude (Anthropic) client."""

    def __init__(self, model: str = "claude-sonnet-4-20250514"):
        self.model = model
        self._client = None

        try:
            import anthropic
            api_key = get_api_key('ANTHROPIC_API_KEY')
            if api_key:
                self._client = anthropic.Anthropic(api_key=api_key)
        except ImportError:
            logger.warning("anthropic package not installed")

    def call(self, prompt: str) -> str:
        if not self._client:
            raise RuntimeError("Claude client not initialized (missing API key or package)")

        response = self._client.messages.create(
            model=self.model,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text

    @property
    def provider_name(self) -> str:
        return "claude"


class OpenAIClient(BaseLLMClient):
    """OpenAI (ChatGPT) client."""

    def __init__(self, model: str = "gpt-4o"):
        self.model = model
        self._client = None

        try:
            import openai
            api_key = get_api_key('OPENAI_API_KEY')
            if api_key:
                self._client = openai.OpenAI(api_key=api_key)
        except ImportError:
            logger.warning("openai package not installed")

    def call(self, prompt: str) -> str:
        if not self._client:
            raise RuntimeError("OpenAI client not initialized (missing API key or package)")

        response = self._client.chat.completions.create(
            model=self.model,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content

    @property
    def provider_name(self) -> str:
        return "openai"


class GeminiClient(BaseLLMClient):
    """Google Gemini client."""

    def __init__(self, model: str = "gemini-1.5-flash"):
        self.model = model
        self._client = None

        try:
            import google.generativeai as genai
            api_key = get_api_key('GOOGLE_API_KEY') or get_api_key('GEMINI_API_KEY')
            if api_key:
                genai.configure(api_key=api_key)
                self._client = genai.GenerativeModel(model)
        except ImportError:
            logger.warning("google-generativeai package not installed")

    def call(self, prompt: str) -> str:
        if not self._client:
            raise RuntimeError("Gemini client not initialized (missing API key or package)")

        response = self._client.generate_content(prompt)
        return response.text

    @property
    def provider_name(self) -> str:
        return "gemini"


def create_client(
    provider: LLMProvider | str,
    model: Optional[str] = None
) -> BaseLLMClient:
    """
    Create LLM client for specified provider.

    Args:
        provider: LLMProvider enum or string ('claude', 'openai', 'gemini')
        model: Optional model name override

    Returns:
        BaseLLMClient instance
    """
    if isinstance(provider, str):
        provider = LLMProvider(provider.lower())

    if provider == LLMProvider.CLAUDE:
        return ClaudeClient(model=model or "claude-sonnet-4-20250514")
    elif provider == LLMProvider.OPENAI:
        return OpenAIClient(model=model or "gpt-4o")
    elif provider == LLMProvider.GEMINI:
        return GeminiClient(model=model or "gemini-1.5-flash")
    else:
        raise ValueError(f"Unknown provider: {provider}")


@dataclass
class LLMSegmentClassifier:
    """Classify books into segments using LLM."""

    segments: List[Dict[str, Any]] = field(default_factory=list)
    _client: Optional[BaseLLMClient] = field(default=None, repr=False)
    _cache: Dict[str, ClassificationResult] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        provider: LLMProvider | str = LLMProvider.CLAUDE,
        model: Optional[str] = None,
        segments_path: Optional[Path] = None
    ) -> 'LLMSegmentClassifier':
        """
        Create classifier with specified provider.

        Args:
            provider: LLM provider ('claude', 'openai', 'gemini')
            model: Optional model name
            segments_path: Path to segments.jsonl

        Returns:
            Configured LLMSegmentClassifier
        """
        classifier = cls()
        classifier._client = create_client(provider, model)
        classifier.load_segments(segments_path)
        return classifier

    def load_segments(self, path: Optional[Path] = None) -> None:
        """Load valid segments from JSONL file."""
        path = path or DEFAULT_SEGMENTS_FILE
        self.segments = []

        if not path.exists():
            logger.warning(f"Segments file not found: {path}")
            return

        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        self.segments.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass

        logger.info(f"Loaded {len(self.segments)} segments")

    def _build_segments_text(self) -> str:
        """Build formatted segment list for prompt."""
        lines = []
        for i, seg in enumerate(self.segments, 1):
            segment_name = seg.get('segment', '')
            segment_ja = seg.get('segment_ja', '')
            lines.append(f"\n## {i}. {segment_name} ({segment_ja})")
            lines.append("サブセグメント:")
            for sub in seg.get('subsegments', []):
                sub_name = sub.get('subsegment', '')
                sub_ja = sub.get('subsegment_ja', '')
                lines.append(f"  - {sub_name} ({sub_ja})")
        return '\n'.join(lines)

    def _build_prompt(
        self,
        title: str,
        description: Optional[str] = None,
        authors: Optional[List[str]] = None,
        publisher: Optional[str] = None,
        language: Optional[str] = None
    ) -> str:
        """Build classification prompt."""
        segments_text = self._build_segments_text()

        book_info = [f"タイトル: {title}"]
        if description:
            # Truncate long descriptions
            desc = description[:1000] + '...' if len(description) > 1000 else description
            book_info.append(f"説明: {desc}")
        if authors:
            book_info.append(f"著者: {', '.join(authors)}")
        if publisher:
            book_info.append(f"出版社: {publisher}")
        if language:
            book_info.append(f"言語: {language}")

        book_text = '\n'.join(book_info)

        return f"""以下の書籍を金融・保険・経済分野のセグメントに分類してください。

# セグメント一覧
{segments_text}

# 書籍情報
{book_text}

# 指示
1. 書籍の内容を分析し、最も適切なセグメントとサブセグメントを選択してください
2. セグメント名・サブセグメント名は上記リストの英語名を正確に使用してください
3. 確信度(confidence)は0.0〜1.0で指定してください
4. 判断理由を簡潔に説明してください

# 出力形式（JSON）
必ず以下の形式で出力してください：
```json
{{
  "segment": "英語セグメント名",
  "subsegment": "英語サブセグメント名",
  "confidence": 0.85,
  "reasoning": "判断理由（日本語）"
}}
```"""

    def classify(
        self,
        title: str,
        description: Optional[str] = None,
        authors: Optional[List[str]] = None,
        publisher: Optional[str] = None,
        language: Optional[str] = None,
        use_cache: bool = True
    ) -> ClassificationResult:
        """
        Classify a book into segment/subsegment.

        Args:
            title: Book title
            description: Book description
            authors: List of authors
            publisher: Publisher name
            language: Book language
            use_cache: Whether to use cached results

        Returns:
            ClassificationResult with segment, subsegment, confidence, reasoning
        """
        if not self._client:
            return ClassificationResult(
                segment='',
                subsegment='',
                confidence=0.0,
                reasoning='',
                is_valid=False,
                error='No LLM client configured'
            )

        if not self.segments:
            return ClassificationResult(
                segment='',
                subsegment='',
                confidence=0.0,
                reasoning='',
                is_valid=False,
                error='No segments loaded'
            )

        # Check cache
        cache_key = f"{title}|{description or ''}|{authors or []}"
        if use_cache and cache_key in self._cache:
            return self._cache[cache_key]

        prompt = self._build_prompt(
            title=title,
            description=description,
            authors=authors,
            publisher=publisher,
            language=language
        )

        try:
            content = self._client.call(prompt)

            # Try to find JSON block
            json_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to find raw JSON object
                json_match = re.search(r'\{[^{}]*"segment"[^{}]*\}', content, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                else:
                    json_str = content

            result_data = json.loads(json_str)

            # Validate segment/subsegment
            segment = result_data.get('segment', '')
            subsegment = result_data.get('subsegment', '')
            is_valid = self._validate_classification(segment, subsegment)

            result = ClassificationResult(
                segment=segment,
                subsegment=subsegment,
                confidence=float(result_data.get('confidence', 0.0)),
                reasoning=result_data.get('reasoning', ''),
                is_valid=is_valid,
                error=None if is_valid else 'Invalid segment/subsegment combination',
                provider=self._client.provider_name
            )

            # Cache result
            if use_cache:
                self._cache[cache_key] = result

            return result

        except json.JSONDecodeError as e:
            return ClassificationResult(
                segment='',
                subsegment='',
                confidence=0.0,
                reasoning='',
                is_valid=False,
                error=f'Failed to parse JSON response: {e}',
                provider=self._client.provider_name if self._client else None
            )
        except Exception as e:
            return ClassificationResult(
                segment='',
                subsegment='',
                confidence=0.0,
                reasoning='',
                is_valid=False,
                error=f'API error: {e}',
                provider=self._client.provider_name if self._client else None
            )

    def _validate_classification(self, segment: str, subsegment: str) -> bool:
        """Validate that segment/subsegment combination is valid."""
        for seg in self.segments:
            if seg.get('segment') == segment:
                for sub in seg.get('subsegments', []):
                    if sub.get('subsegment') == subsegment:
                        return True
        return False

    def get_valid_segments(self) -> List[str]:
        """Get list of valid segment names."""
        return [s.get('segment', '') for s in self.segments]

    def get_valid_subsegments(self, segment: str) -> List[str]:
        """Get list of valid subsegment names for a segment."""
        for seg in self.segments:
            if seg.get('segment') == segment:
                return [sub.get('subsegment', '') for sub in seg.get('subsegments', [])]
        return []


def classify_batch(
    classifier: LLMSegmentClassifier,
    records: List[Dict[str, Any]],
    max_concurrent: int = 5
) -> List[Tuple[Dict[str, Any], ClassificationResult]]:
    """
    Classify multiple records.

    Args:
        classifier: LLMSegmentClassifier instance
        records: List of book records
        max_concurrent: Maximum concurrent requests (not used in sync version)

    Returns:
        List of (record, result) tuples
    """
    results = []
    for record in records:
        result = classifier.classify(
            title=record.get('title', ''),
            description=record.get('description') or _get_description(record),
            authors=record.get('authors'),
            publisher=record.get('publisher'),
            language=record.get('language')
        )
        results.append((record, result))
    return results


def _get_description(record: Dict[str, Any]) -> Optional[str]:
    """Extract description from record editions."""
    for edition in record.get('editions', []):
        if edition.get('description'):
            return edition['description']
    return None


# CLI for testing
def main():
    import argparse

    parser = argparse.ArgumentParser(description='LLM-based segment classifier')
    parser.add_argument('--title', required=True, help='Book title')
    parser.add_argument('--description', help='Book description')
    parser.add_argument('--authors', nargs='+', help='Book authors')
    parser.add_argument('--provider', choices=['claude', 'openai', 'gemini'],
                       default='claude', help='LLM provider')
    parser.add_argument('--model', help='Model name (provider-specific)')
    parser.add_argument('--segments', type=Path, default=DEFAULT_SEGMENTS_FILE,
                       help='Path to segments.jsonl')

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    classifier = LLMSegmentClassifier.create(
        provider=args.provider,
        model=args.model,
        segments_path=args.segments
    )

    result = classifier.classify(
        title=args.title,
        description=args.description,
        authors=args.authors
    )

    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
