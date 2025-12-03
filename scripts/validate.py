"""Validate book records (JSON Lines) against the JSON Schema."""
import argparse
import json
from pathlib import Path

try:
    import jsonschema
except ImportError as exc:  # pragma: no cover
    raise SystemExit("jsonschema is required. Install with `pip install jsonschema`." ) from exc


def load_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            try:
                yield lineno, json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"JSON parse error at line {lineno}: {exc}") from exc


def main():
    parser = argparse.ArgumentParser(description="Validate JSONL book records against schema.")
    parser.add_argument("data", type=Path, nargs="?", default=Path("samples/books.sample.jsonl"), help="Path to JSONL file.")
    parser.add_argument("--schema", type=Path, default=Path("schema/book_record.schema.json"), help="Path to JSON Schema file.")
    args = parser.parse_args()

    schema = json.loads(args.schema.read_text(encoding="utf-8"))
    validator = jsonschema.Draft202012Validator(schema)

    errors = []
    for lineno, record in load_jsonl(args.data):
        for err in validator.iter_errors(record):
            errors.append((lineno, err.message))

    if errors:
        for lineno, msg in errors:
            print(f"Line {lineno}: {msg}")
        raise SystemExit(f"Validation failed with {len(errors)} error(s).")

    print(f"Validation passed: {args.data} ({sum(1 for _ in load_jsonl(args.data))} records)")


if __name__ == "__main__":
    main()
