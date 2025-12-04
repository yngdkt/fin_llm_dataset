# AGENTS

本ドキュメントは、金融・保険・経済領域の専門LLM向け学習データセットを構築するためのエージェント作業指針です。実在する専門書籍と公的・民間データソースを体系化し、セグメント×観点のカバレッジで合計10,000冊の書籍DBを構築します。

# スコープと目標
- 言語: 日本語および英語
- 対象: 金融・保険・経済関連の実在する専門書、規制当局/公的機関/業界団体/学会の公式資料やデータソース（有償・無償）
- 目標: 10,000冊の書籍レコードをセグメント別に整備（不足分は優先セグメントから補完）
- フレーム: セグメント × 観点（法制度/規制・理論・実務・事例・トレンド・資格）でカバレッジを確認
- 除外: 個人ブログや真偽不明の出典、フィクション、非公開資料、スキャン違法コピー

# 観点の定義
- 法制度・規制: 法律、監督指針、規制フレームワーク、監督当局・取引所のルール
- 理論: アカデミック理論、モデル、計量手法
- 実務: オペレーション、業務プロセス、プロダクト設計、内部管理
- 事例: ケーススタディ、判例、失敗事例、ベストプラクティス
- トレンド: 最新制度改正、技術動向、マーケット動向、実務動向
- 資格: 資格試験対策、公式テキスト、ガイドライン

# セグメント分類

本プロジェクトでは以下の9つのセグメントで金融・保険・経済領域をカバーします。

| # | セグメント | 英語名 |
|---|-----------|--------|
| 1 | 証券・投資銀行 | Securities & Investment Banking |
| 2 | 銀行・決済 | Banking & Payments |
| 3 | 資産運用 | Asset Management |
| 4 | 保険 | Insurance |
| 5 | リース・クレジット・ノンバンク | Leasing, Credit & Non-Banking |
| 6 | 経済学・金融政策 | Economics & Monetary Policy |
| 7 | 会計・税務・企業法務 | Accounting, Tax & Corporate Law |
| 8 | フィンテック・暗号資産 | Fintech, Crypto Assets & Web3 |
| 9 | 不動産・コモディティ | Real Estate, Commodities & Specialized Markets |

各セグメントのサブセグメント・トピック・検索キーワードの詳細は以下のファイルを参照してください：

- **詳細リスト（機械可読）**: [data/config/segment_topics.jsonl](data/config/segment_topics.jsonl)
- **詳細リスト（人間可読）**: [data/config/segment_topics.md](data/config/segment_topics.md)


## 推奨メタデータ項目（書籍レコード）
- record_id, title, subtitle, authors, language, publication_year, edition, publisher/imprint, isbn_or_issn
- segment, subsegment, perspective(法制度・理論・実務・事例・トレンド・資格), topics/keywords, instrument/asset_class, jurisdiction/region, audience(level)
- format(print/ebook), pages, series, official_status(公的/民間), access_type(有償/無償), source_url or catalog_url
- notes/summary, related_regulation/code, qualification_target, recommended_for, data_source(reviewer)
- dataset_status(draft/validated/rejected), last_reviewed_at

## 収集・登録ワークフロー
1) セグメントを選択し、観点ごとに必要な書籍タイプを定義（制度/理論/実務/事例/資格など）。
2) 信頼できるソース（出版社/規制当局/業界団体/大学/学会/専門メディア/書誌データベース）から候補を収集。
3) メタデータを上記フィールドで構造化入力し、ISBN/ISSNや出版年など必須項目を確認。
4) 重複排除（ISBN・タイトル・著者で正規化）し、既存レコードとの突合。
5) カバレッジチェック：セグメント×観点で件数を可視化し、未充足領域を優先補完。
6) 品質レビュー：出典信頼度、公的性、最新性、実務妥当性を評価し、`dataset_status`を更新。

## 信頼できるソース例（優先）
- 出版社・専門書: 日本経済新聞出版, 東洋経済新報社, 中央経済社, 金融財政事情研究会, 有斐閣, 中央法規, オーム社, ダイヤモンド社; 海外は Wiley, Pearson, FT Press, McGraw-Hill, OUP, CUP, Springer, Elsevier
- 規制当局・公的機関: 金融庁, 日本銀行, 公取委, 総務省, 経産省, 財務省, 金融庁EDINET/開示書類, 東証/JSDA, BIS, IOSCO, FSB, IMF, World Bank, SEC/FRB/FDIC, FCA/BoE/EBA
- 業界団体・学会: 日本証券業協会, 投資信託協会, 信託協会, 生命保険協会, 損害保険協会, ISDA, PRI/UNEP FI
- 研究・論文: SSRN, NBER, RePEc/IDEAS, JSTOR, CiNii, 大学紀要・商学部/経済学部の出版物
- データ・市場情報: JPX/東証データ, JSCC/JASDEC資料, IMF/BIS統計, OECD, S&P/Moody's/Fitchの年次報告（書籍扱い可否を確認）

## 品質チェックと正規化
- ISBN/ISSNのバリデーション、出版社名の正規化（出版社公式表記を採用）。
- タイトル・著者のローマ字表記/英訳を併記（可能な場合）。
- ジュリスディクション（JP/US/EU/Globalなど）を明示し、法制度書は発行時点の規制日付をメモ。
- 版の確認（改訂版/第○版）、最新年版の有無を記録。
- 資格向け書籍は対象資格名（例: 証券アナリストCMA、CFP、USCPA、アクチュアリー）をタグ付け。

## デリバラブルフォーマットの例
- CSV/Parquet/JSON Lines で保持。必須列例: `record_id,title,authors,language,publication_year,publisher,isbn,segment,subsegment,perspective,jurisdiction,access_type,source_url,dataset_status,last_reviewed_at`
- 付随資料: セグメント×観点のカバレッジ集計表（ピボット）、ソースリスト、レビュー履歴。

## 優先度の目安
- まず主要セグメント（証券・投資銀行、銀行・決済、資産運用、保険）で各1,000〜1,500件を目標に充填。
- 規制/法制度と実務ガイドは最新改訂版を優先し、理論書は古典と最新版をバランス配置。
- トレンド/フィンテック領域は刊行年が新しいものを優先し、毎年のアップデート枠を確保。
- 基本方針として刊行年2015〜2025の実務・制度・トレンド系を中心に収集（古い版は補足的に扱う）。

---

## 収集アプローチ（2段階統合方式）

本プロジェクトでは以下の2つのアプローチを並行して実行し、最終的に統合します。

### アプローチA: 出版社カタログからのクローリング
金融・保険・経済に強い出版社のWebサイトから目録をベースに書籍一覧を自動収集します。

対象出版社の詳細は以下のファイルを参照してください：

- **出版社リスト**: [data/config/publishers.jsonl](data/config/publishers.jsonl)

### アプローチB: セグメント×観点からの検索収集
セグメント分類と観点に基づき、キーワード検索でターゲットを絞った収集を行います。
検索キーワードは [data/config/segment_topics.jsonl](data/config/segment_topics.jsonl) を参照。

### 統合・重複排除プロセス
1. **版の統一**: 同一書籍の版違い（Edition違い）は最新版のみを採用
   - ISBN-13の先頭12桁（チェックディジット除く）で同一書籍を判定
   - タイトル＋著者の正規化マッチングで補完
   - publication_yearが最新のものを採用
2. **URL検証**: 収集した`source_url`/`catalog_url`の有効性をバリデーション
3. **重要度スコアリング**: 以下の要素でランク付け
   - 出版社の信頼度スコア（tier1/tier2/tier3）
   - 刊行年の新しさ（2020年以降を優先）
   - 被引用数・レビュー数（取得可能な場合）
   - セグメント×観点のカバレッジ貢献度
4. **マスターDBへの統合**: `data/master_books.jsonl`として一元管理

---

## 技術スタック・実行環境

### 環境設定
```bash
# 仮想環境の有効化
source .venv/bin/activate

# 依存パッケージのインストール
pip install -r requirements.txt
```

### 使用ライブラリ
- **クローリング**: `requests`, `beautifulsoup4`, `playwright` (JS必須サイト用)
- **データ処理**: `pandas`, `polars`
- **バリデーション**: `jsonschema`, `pydantic`
- **LLM API連携**: `openai`, `anthropic`, `google-generativeai`
- **非同期処理**: `asyncio`, `aiohttp`
- **レート制限**: `ratelimit`, `tenacity`

### LLM APIの活用
- **セグメント/観点の自動分類**: 書籍タイトル・説明文からセグメント・サブセグメント・観点を推定
- **重複判定の補助**: タイトルの類似度判定、同一書籍の版違い検出
- **メタデータ補完**: 不足フィールドの推定・補完
- **品質チェック**: 収集データの妥当性検証

### ディレクトリ構造
```
fin_llm_dataset/
├── .venv/                  # Python仮想環境
├── AGENTS.md               # 本ドキュメント
├── README.md
├── requirements.txt        # 依存パッケージ
├── schema/
│   └── book_record.schema.json
├── scripts/
│   ├── validate.py         # JSONLバリデーション
│   ├── crawlers/           # 出版社別クローラー
│   │   ├── base_crawler.py
│   │   ├── wiley_crawler.py
│   │   ├── nikkei_crawler.py
│   │   └── ...
│   ├── processors/         # データ処理
│   │   ├── build_master.py # マスターDB構築
│   │   ├── build_db.py     # SQLiteビルド
│   │   ├── book_matcher.py # 重複判定
│   │   └── migrate_v1_to_v2.py
│   └── classifiers/        # LLM分類
│       └── segment_classifier.py
├── data/
│   ├── config/             # 設定ファイル
│   │   ├── segment_topics.jsonl  # セグメント・トピック定義
│   │   ├── segment_topics.md     # 同上（人間可読版）
│   │   └── publishers.jsonl      # 出版社リスト
│   ├── raw/                # クローラー生データ（.gitignore）
│   ├── processed/          # 処理済みデータ
│   └── master/             # マスターDB
│       └── books.jsonl     # 統合マスター
├── samples/
│   └── books.sample.jsonl
└── reports/
    └── coverage_matrix.csv # セグメント×観点カバレッジ
```

---

## クローラー実行ガイド

### 基本コマンド
```bash
# 単一出版社のクロール
python scripts/crawlers/wiley_crawler.py --category finance --output data/raw/wiley/

# マスターDBの初期構築
python scripts/processors/build_master.py --init

# ソースの追加
python scripts/processors/build_master.py --add data/raw/publisher/books.jsonl

# 統計情報の確認
python scripts/processors/build_master.py --stats

# SQLiteデータベースのビルド
python scripts/processors/build_db.py
```

### レート制限・倫理的配慮
- 各サイトへのリクエスト間隔: 最低2秒
- robots.txtの尊重
- User-Agentの明示
- 過度な負荷をかけない（1日あたりの上限設定）
