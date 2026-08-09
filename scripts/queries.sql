-- DuckDB queries for the WarpStream Tableflow ecommerce lab.
--
-- Do not hand-edit table UUIDs. After Tableflow creates local tables, run:
--   ./scripts/render-queries.sh
-- That rewrites this file with your session paths.
--
-- Then in DuckDB:
--   INSTALL iceberg; LOAD iceberg;
--   .read scripts/queries.sql
--
-- Placeholder paths below will fail until you render:

-- Query 1: row counts
SELECT 'clickstream' AS tbl, COUNT(*) AS rows
FROM iceberg_scan('/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/REPLACE_CLICKSTREAM')
UNION ALL
SELECT 'orders', COUNT(*)
FROM iceberg_scan('/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/REPLACE_ORDERS')
UNION ALL
SELECT 'inventory', COUNT(*)
FROM iceberg_scan('/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/REPLACE_INVENTORY');
