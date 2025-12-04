# Financial LLM Dataset

金融・保険・経済領域の専門LLM向けに、実在する専門書籍および公的・民間データソースを体系化した10,000件規模の書籍データベースを構築するプロジェクトです。詳細な作業指針は `AGENTS.md` を参照してください。

## 目的
- 金融・保険・経済の各セグメント×観点（法制度/規制・理論・実務・事例・トレンド・資格）で網羅的な書誌メタデータを整備
- 日本語・英語の両言語で信頼できる出典に基づくデータセットを構築
- LLM学習用にクリーンで正規化された書誌レコードとソース情報を提供

## セットアップ

```bash
# 仮想環境の有効化
source .venv/bin/activate

# 依存パッケージのインストール
pip install -r requirements.txt

# Playwrightブラウザのインストール（クローラー用）
playwright install chromium
```

## ディレクトリ構成

```
fin_llm_dataset/
├── AGENTS.md               # エージェント作業指針（セグメント定義等）
├── README.md               # 本ファイル
├── requirements.txt        # Python依存パッケージ
├── schema/
│   ├── book_record.schema.json      # v1スキーマ（レガシー）
│   └── book_record.v2.schema.json   # v2スキーマ（推奨）
├── scripts/
│   ├── crawlers/           # 出版社別クローラー
│   │   ├── base_crawler.py
│   │   └── wiley_crawler.py
│   ├── processors/         # データ処理ツール
│   │   ├── book_matcher.py      # 重複検出・表記ゆれ対応
│   │   └── migrate_v1_to_v2.py  # v1→v2移行
│   └── validate.py         # スキーマバリデーション
├── data/
│   ├── raw/                # クローラー生データ
│   │   └── wiley/
│   ├── processed/          # 処理済みデータ
│   └── *.jsonl             # セグメント別書籍データ
├── samples/
│   └── books.sample.jsonl
└── reports/
    └── coverage_pivot.sql
```

---

## データスキーマ

### v2スキーマ（推奨）

複数版・複数フォーマットを1レコードで管理する階層構造:

```
Work (書籍作品)
 └── Edition (版: 1st, 2nd, 3rd...)
      └── Format (形式: hardcover, ebook, pdf...)
           └── ISBN, 価格, URL
```

**主要フィールド:**

| フィールド | 必須 | 説明 |
|-----------|------|------|
| `work_id` | ✓ | 作品の一意識別子 (`{publisher}_{hash}`) |
| `title` | ✓ | 書籍タイトル（版情報除去後） |
| `authors` | | 著者リスト |
| `language` | ✓ | `ja`, `en`, `other` |
| `publisher` | ✓ | 出版社名 |
| `segment` | ✓ | セグメント（例: Securities & Investment Banking） |
| `subsegment` | ✓ | サブセグメント（例: Valuation） |
| `perspective` | ✓ | 観点: `law_regulation`, `theory`, `practice`, `case`, `trend`, `qualification` |
| `editions` | ✓ | 版情報の配列 |
| `dataset_status` | ✓ | `draft`, `validated`, `rejected` |

**editions配列の構造:**
```json
{
  "edition_number": 3,
  "edition_label": "3rd Edition",
  "publication_year": 2024,
  "is_latest": true,
  "pages": 450,
  "formats": [
    {
      "format_type": "hardcover",
      "isbn": "9781234567890",
      "price": {"amount": 89.99, "currency": "USD"},
      "url": "https://...",
      "availability": "available"
    },
    {
      "format_type": "ebook",
      "isbn": "9781234567891",
      "price": {"amount": 59.99, "currency": "USD"}
    }
  ]
}
```

詳細: [schema/book_record.v2.schema.json](schema/book_record.v2.schema.json)

---

## クローラー

### Wileyクローラー

```bash
# 単一カテゴリのクロール
python scripts/crawlers/wiley_crawler.py --category general_finance

# 全カテゴリのクロール
python scripts/crawlers/wiley_crawler.py --all

# 詳細情報（全フォーマット）を取得
python scripts/crawlers/wiley_crawler.py --category valuation --fetch-details
```

**対応カテゴリ:**
- `general_finance` - General Finance & Investments
- `corporate_finance` - Corporate Finance
- `valuation` - Valuation
- `ma` - Mergers & Acquisitions
- `banking` - Banking
- `risk_management` - Risk Management
- その他（`--list-categories`で確認）

---

## 重複検出・表記ゆれ対応

`BookIndex`クラスで高速な重複チェックを提供:

### 対応する表記ゆれ

| パターン | 例 |
|---------|-----|
| 全角⇔半角 | `Ｆｉｎａｎｃｅ` ⇔ `Finance` |
| 括弧の種類 | `【第3版】` ⇔ `(3rd Edition)` |
| 漢数字 | `第三版` ⇔ `第3版` |
| 中黒・記号 | `コーポレート・ファイナンス` ⇔ `コーポレートファイナンス` |
| Edition表記 | `3rd Edition` ⇔ `Third Edition` ⇔ `3e` |
| サブタイトル区切り | `:` ⇔ `-` ⇔ `—` |
| 冠詞 | `The Art of M&A` ⇔ `Art of M&A` |

### 使用方法

```python
from scripts.processors.book_matcher import BookIndex

# 既存データベースからインデックス構築（起動時1回）
import json
with open('data/master_books.jsonl') as f:
    existing_books = [json.loads(line) for line in f]

index = BookIndex(existing_books)
print(index.stats())
# {'total_books': 10000, 'isbn_entries': 8500, ...}

# 新規書籍の重複チェック
new_book = {
    'title': 'コーポレート・ファイナンス【第3版】',
    'authors': ['著者名'],
}

result = index.find_match(new_book)
if result:
    matched_book, match_result = result
    print(f"既存書籍と一致: {matched_book['title']}")
    print(f"信頼度: {match_result.confidence:.0%}")
    print(f"マッチ種別: {match_result.match_type}")
else:
    # 新規書籍として追加
    index.add(new_book)
```

### パフォーマンス

| 指標 | 値 |
|------|-----|
| インデックス構築 (10,000冊) | ~1秒 |
| クエリ応答時間 | ~0.2ms |
| スループット | 6,000+ queries/秒 |

```bash
# ベンチマーク実行
python scripts/processors/book_matcher.py --benchmark
```

### マッチング判定基準

| match_type | confidence | 説明 |
|------------|------------|------|
| `isbn_exact` | 100% | ISBN完全一致 |
| `isbn_prefix` | 98% | ISBN-13の先頭12桁一致（版違い） |
| `normalized_exact` | 95-100% | 正規化後タイトル一致 |
| `aggressive_normalized` | 90-95% | 積極的正規化後一致 |
| `fuzzy_high` | 80-90% | 高信頼度Fuzzyマッチ |
| `fuzzy_medium` | 70-80% | 中信頼度（要レビュー） |

---

## データ移行

### v1 → v2 移行

```bash
# 単一ファイル
python scripts/processors/migrate_v1_to_v2.py \
  data/books_valuation.jsonl \
  data/processed/books_valuation_v2.jsonl

# ディレクトリ一括変換
python scripts/processors/migrate_v1_to_v2.py \
  data/ \
  data/processed/ \
  --pattern "books_*.jsonl"
```

移行処理:
- タイトルから版番号を自動抽出（`8th Edition` → `edition_number: 8`）
- `work_id`を自動生成
- 同一`work_id`のレコードを自動マージ
- フォーマット情報をネスト構造に変換

---

## バリデーション

```bash
# v2スキーマでの検証
python scripts/validate.py data/processed/books_valuation_v2.jsonl --schema v2

# サンプルデータの検証
python scripts/validate.py samples/books.sample.jsonl
```

---

## SQLiteデータベース

JONLファイルからSQLiteデータベースを構築し、複雑なクエリや分析を実行できます。

### データ構造の考え方

| 用途 | 形式 | 理由 |
|------|------|------|
| **マスターデータ** | JSONL | Git管理、スキーマ柔軟性、ポータビリティ |
| **検索・分析** | SQLite | 複雑なクエリ、集計、JOIN |
| **重複検出** | BookIndex (メモリ) | 高速 (6,000+ queries/秒) |

### DB構築

```bash
# v2形式のJSONLからSQLiteを構築
python scripts/processors/build_db.py \
  --input data/processed \
  --output data/books.db \
  --summary
```

### テーブル構成

```
works (書籍マスター)
 ├── authors (著者)
 ├── editors (編者)
 ├── topics (トピック/キーワード)
 ├── data_sources (データソース)
 └── editions (版)
      ├── table_of_contents (目次)
      └── formats (フォーマット: ISBN, 価格, URL)
```

### 便利なビュー

| ビュー | 説明 |
|--------|------|
| `v_books_flat` | 書籍情報をフラット化（著者・ISBN結合済み） |
| `v_coverage` | セグメント×観点×言語のカバレッジ集計 |
| `v_recent` | 2020年以降の新刊一覧 |

### クエリ例

```python
import sqlite3
conn = sqlite3.connect('data/books.db')

# カバレッジ分析
for row in conn.execute('''
    SELECT segment, subsegment, perspective, book_count
    FROM v_coverage
    ORDER BY book_count DESC
'''):
    print(row)

# 特定出版社の書籍検索
for row in conn.execute('''
    SELECT title, publication_year, formats
    FROM v_books_flat
    WHERE publisher = 'Wiley'
    ORDER BY publication_year DESC
    LIMIT 10
'''):
    print(row)

# ISBNで検索
row = conn.execute('''
    SELECT w.title, w.publisher, e.publication_year
    FROM works w
    JOIN editions e ON w.work_id = e.work_id
    JOIN formats f ON e.id = f.edition_id
    WHERE f.isbn = '9781234567890'
''').fetchone()
```

---

## カバレッジ集計

```bash
# SQLiteのビューを使用
python -c "
import sqlite3
conn = sqlite3.connect('data/books.db')
for row in conn.execute('SELECT * FROM v_coverage'):
    print(row)
"

# または DuckDB（別途インストールが必要）
duckdb -c "PRAGMA threads=4;" -s "$(cat reports/coverage_pivot.sql)"
```

出力:
- `reports/coverage_long.csv` - セグメント×観点の件数
- `reports/coverage_pivot.csv` - ピボットテーブル
- `reports/gaps.csv` - 未充足領域リスト

---

## 参考

- 詳細なセグメント定義・収集フロー: [AGENTS.md](AGENTS.md)
- v2スキーマ定義: [schema/book_record.v2.schema.json](schema/book_record.v2.schema.json)
- v1スキーマ（レガシー）: [schema/book_record.schema.json](schema/book_record.schema.json)
