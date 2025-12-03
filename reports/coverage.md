# カバレッジ計測ガイド

本ガイドは、セグメント×観点のカバレッジを定期的に可視化し、未充足領域を優先補完するためのテンプレートです。

## 集計の基本軸
- 行: セグメント（例: 証券・投資銀行, 銀行・決済, 資産運用, 保険, ほか）
- 列: 観点（法制度/規制, 理論, 実務, 事例, トレンド, 資格）
- 値: 書籍件数（`dataset_status != "rejected"`）
- フィルタ: 言語、刊行年レンジ、ジュリスディクション、アクセス種別（有償/無償）

## 推奨アウトプット
- ピボットテーブル（CSV/Markdown）: `reports/coverage_pivot.csv` / `reports/coverage_pivot.md`
- 未充足リスト: 閾値（例: 各セル 50 件）未達の組み合わせを抽出し `reports/gaps.csv` に出力
- トレンド: 刊行年ヒストグラム/タイムラインで更新頻度を確認

## サンプルSQL（DuckDB想定）
```sql
-- ピボット用集計（長表）
COPY (
  SELECT segment,
         perspective,
         COUNT(*) AS cnt
  FROM read_csv_auto('data/books.csv')
  WHERE dataset_status <> 'rejected'
  GROUP BY 1,2
) TO 'reports/coverage_long.csv' (HEADER, DELIMITER ',');
```

## 品質チェックの観点
- `dataset_status` が `validated` の比率をセグメントごとに算出し、レビューの進捗を確認
- `publication_year` が古いセルは最新改訂の有無を確認
- 観点「資格」は対象資格タグの分布を確認（CMA, CFP, USCPA, アクチュアリー等）

## 運用サイクル（例）
- 週次: 新規追加と重複排除の反映、ピボット更新
- 月次: 未充足セルのアクションプラン策定、古い版のアップデート確認
- 四半期: スキーマの見直しとメタデータ項目の追加検討
