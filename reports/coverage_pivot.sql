-- DuckDB SQL to produce segment x perspective pivot and gaps list.
-- Usage (example): duckdb -c "PRAGMA threads=4;" -s "$(cat reports/coverage_pivot.sql)"

-- Input CSV assumed at data/books.csv with header following schema/book_record.schema.json

-- Long form aggregate
COPY (
  SELECT segment,
         perspective,
         COUNT(*) AS cnt
  FROM read_csv_auto('data/books.csv')
  WHERE dataset_status <> 'rejected'
  GROUP BY 1,2
) TO 'reports/coverage_long.csv' (HEADER, DELIMITER ',');

-- Pivot to wide table
COPY (
  PIVOT (
    SELECT segment, perspective, cnt
    FROM read_csv_auto('reports/coverage_long.csv')
  ) ON perspective USING SUM(cnt)
) TO 'reports/coverage_pivot.csv' (HEADER, DELIMITER ',');

-- Gaps (example threshold = 50)
COPY (
  SELECT segment, perspective, cnt
  FROM read_csv_auto('reports/coverage_long.csv')
  WHERE cnt < 50
  ORDER BY cnt ASC
) TO 'reports/gaps.csv' (HEADER, DELIMITER ',');
