#!/usr/bin/env python3
"""
Convert segment_topics.md to segment_topics.jsonl

Parses the markdown format and outputs JSONL with:
- segment: English segment name
- subsegment: English subsegment name
- topic: Topic name (Japanese)
- topic_en: Topic name (English)
- keywords_ja: List of Japanese keywords
- keywords_en: List of English keywords
"""

import json
import re
from pathlib import Path


def parse_markdown(md_path: Path) -> list:
    """Parse segment_topics.md and return list of topic records."""
    records = []

    with open(md_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split by segment headers (## N. ...)
    segment_pattern = r'^## \d+\. (.+?) \((.+?)\)$'
    subsegment_pattern = r'^### \d+-\d+\. (.+?) \((.+?)\)$'
    topic_pattern = r'^- \*\*(.+?)(?:\s*\((.+?)\))?:\*\*\s*(.+)$'

    current_segment_ja = None
    current_segment_en = None
    current_subsegment_ja = None
    current_subsegment_en = None

    for line in content.split('\n'):
        line = line.strip()

        # Check for segment header
        segment_match = re.match(segment_pattern, line)
        if segment_match:
            current_segment_ja = segment_match.group(1)
            current_segment_en = segment_match.group(2)
            current_subsegment_ja = None
            current_subsegment_en = None
            continue

        # Check for subsegment header
        subsegment_match = re.match(subsegment_pattern, line)
        if subsegment_match:
            current_subsegment_ja = subsegment_match.group(1)
            current_subsegment_en = subsegment_match.group(2)
            continue

        # Check for topic line
        topic_match = re.match(topic_pattern, line)
        if topic_match and current_segment_en and current_subsegment_en:
            topic_name = topic_match.group(1)
            topic_en = topic_match.group(2)
            keywords_str = topic_match.group(3)

            # Parse keywords - split by comma
            keywords = [k.strip() for k in keywords_str.split(',')]

            # Separate Japanese and English keywords
            keywords_ja = []
            keywords_en = []

            for kw in keywords:
                if kw:
                    # Check if keyword is primarily English (ASCII)
                    if re.match(r'^[A-Za-z0-9\s\-\./&\'\(\)]+$', kw):
                        keywords_en.append(kw)
                    else:
                        keywords_ja.append(kw)

            record = {
                'segment': current_segment_en,
                'segment_ja': current_segment_ja,
                'subsegment': current_subsegment_en,
                'subsegment_ja': current_subsegment_ja,
                'topic': topic_name,
                'topic_en': topic_en,
                'keywords_ja': keywords_ja,
                'keywords_en': keywords_en
            }
            records.append(record)

    return records


def main():
    base_dir = Path(__file__).parent.parent.parent
    md_path = base_dir / 'data' / 'config' / 'segment_topics.md'
    jsonl_path = base_dir / 'data' / 'config' / 'segment_topics.jsonl'

    print(f"Reading: {md_path}")
    records = parse_markdown(md_path)

    print(f"Parsed {len(records)} topic records")

    # Count by segment
    segment_counts = {}
    for r in records:
        seg = r['segment']
        segment_counts[seg] = segment_counts.get(seg, 0) + 1

    print("\nTopics by segment:")
    for seg, count in sorted(segment_counts.items()):
        print(f"  {seg}: {count}")

    # Write JSONL
    with open(jsonl_path, 'w', encoding='utf-8') as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')

    print(f"\nWritten to: {jsonl_path}")


if __name__ == '__main__':
    main()
