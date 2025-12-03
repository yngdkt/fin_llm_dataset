# Financial LLM Dataset

金融・保険・経済領域の専門LLM向けに、実在する専門書籍および公的・民間データソースを体系化した10,000件規模の書籍データベースを構築するプロジェクトです。詳細な作業指針は `AGENTS.md` を参照してください。

## 目的
- 金融・保険・経済の各セグメント×観点（法制度/規制・理論・実務・事例・トレンド・資格）で網羅的な書誌メタデータを整備
- 日本語・英語の両言語で信頼できる出典に基づくデータセットを構築
- LLM学習用にクリーンで正規化された書誌レコードとソース情報を提供

## 成果物（予定）
- 書誌データ: CSV/Parquet/JSON Lines 形式で `data/` 以下に配置
- スキーマ: JSON Schema で書誌レコードの必須・任意項目を定義（`schema/book_record.schema.json`）
- 検証: スキーマ検証用スクリプトとサンプルデータ（`scripts/`, `samples/`）
- カバレッジ: セグメント×観点の件数ピボット、未充足領域のリスト（`reports/coverage.md` 等）

## ディレクトリ構成（案）
- `AGENTS.md`: エージェント作業指針
- `README.md`: 本ファイル
- `schema/`: JSON Schema 等のスキーマ定義
- `data/`: クリーニング済み書誌データ（追って追加）
- `raw/`: 一次取得データやソースリスト（公開可否を確認して配置）
- `scripts/`: 収集・正規化・検証スクリプト
- `reports/`: カバレッジレポートやメトリクス
- `samples/`: スキーマ準拠のサンプルデータ

## サンプルと検証
- サンプル: `samples/books.sample.jsonl` にスキーマ準拠のレコード例を3件格納
- 検証スクリプト: `scripts/validate.py`（要 `jsonschema`）
  ```bash
  pip install jsonschema  # 未インストールの場合
  python scripts/validate.py samples/books.sample.jsonl
  ```

## カバレッジ集計テンプレート（DuckDB例）
- SQLテンプレート: `reports/coverage_pivot.sql`（`data/books.csv` を入力として想定）
- 実行例
  ```bash
  duckdb -c "PRAGMA threads=4;" -s \"$(cat reports/coverage_pivot.sql)\"
  ```
  出力: `reports/coverage_long.csv`, `reports/coverage_pivot.csv`, `reports/gaps.csv`

## 次のアクション
1. スキーマ定義と必須フィールドの確定（JSON Schema 草案を追加）
2. サンプルデータ数件を作成し、スキーマ検証を通す
3. カバレッジ集計のテンプレート（ピボット/メトリクス）を `reports/` に追加
4. ソースリストの初期セット（出版社・公的機関・業界団体など）を `raw/` に格納

## 参考
- 詳細な領域/観点と収集フローは `AGENTS.md` を参照
- スキーマ項目の背景や妥当性チェックは `schema/book_record.schema.json` と `reports/coverage.md` に追記予定
