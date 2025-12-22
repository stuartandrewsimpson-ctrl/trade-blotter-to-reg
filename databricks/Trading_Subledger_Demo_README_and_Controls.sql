-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Trading Subledger demo (Databricks)
-- MAGIC
-- MAGIC ## What this is
-- MAGIC This workspace loads 4 CSVs into `workspace.demo` and builds a small “medallion” model:
-- MAGIC - **Bronze:** raw ingests
-- MAGIC - **Silver:** typed/standardised
-- MAGIC - **Gold:** reconciliations + reporting views
-- MAGIC
-- MAGIC The demo shows how to reconcile:
-- MAGIC 1) **Front-office MTM time series** (FO)
-- MAGIC → 2) **journal postings** (DR/CR)
-- MAGIC → 3) a **thin ledger** (daily balances)
-- MAGIC
-- MAGIC ## Data assets
-- MAGIC - `demo.fo_mtm_timeseries`
-- MAGIC - `demo.fo_sec_positions`
-- MAGIC - `demo.sec_positions`
-- MAGIC - `demo.sec_trades`
-- MAGIC - `demo.jnl_dr_cr_postings`
-- MAGIC - `demo.tb_thin_ledger`
-- MAGIC
-- MAGIC ## Quickstart (run top to bottom)
-- MAGIC 1) Validate row counts  
-- MAGIC 2) Build silver views  
-- MAGIC 3) Build gold reconciliation views  
-- MAGIC 4) Run example checks  
-- MAGIC
-- MAGIC ## Controls included
-- MAGIC **Control 1 — Thin ledger rebuild**  
-- MAGIC Rebuild balances as the cumulative sum of journal postings and compare to `tb_thin_ledger`.
-- MAGIC
-- MAGIC **Control 2 — FO MTM delta vs MTM journals**  
-- MAGIC Compare day-on-day FO MTM change to MTM journal postings (MTM accounts).
-- MAGIC
-- MAGIC ## Known breaks / demo limitations
-- MAGIC Known break (intentional demo): FO MTM deltas reconcile to MTM journal postings for most instruments, but one slice (`CIF002 / GB0000000003`) has FO MTM changes with no corresponding MTM journal postings, causing a thin-ledger vs journals break on MTM accounts (`400100/400200`).
-- MAGIC This is surfaced by **Control 2** and explains the **Control 1** variance.
-- MAGIC
-- MAGIC

-- COMMAND ----------

USE demo;

SELECT 'fo_mtm_timeseries' AS tbl, COUNT(*) AS n FROM demo.fo_mtm_timeseries
UNION ALL SELECT 'fo_sec_positions', COUNT(*) FROM demo.fo_sec_positions
UNION ALL SELECT 'sec_positions', COUNT(*) FROM demo.sec_positions
UNION ALL SELECT 'sec_trades', COUNT(*) FROM demo.sec_trades;


-- COMMAND ----------

-- Basic profiling for FO MTM
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT customer_id) AS customers,
  COUNT(DISTINCT isin) AS isins,
  COUNT(DISTINCT ccy) AS currencies,
  MIN(as_of_date) AS min_dt,
  MAX(as_of_date) AS max_dt
FROM demo.fo_mtm_timeseries;


-- COMMAND ----------

DESCRIBE demo.fo_sec_positions;
DESCRIBE demo.sec_positions;
DESCRIBE demo.sec_trades;


-- COMMAND ----------

USE demo;

SELECT
  posting_date,
  ccy,
  SUM(CASE WHEN dr_cr = 'DR' THEN amount ELSE 0 END) AS total_dr,
  SUM(CASE WHEN dr_cr = 'CR' THEN amount ELSE 0 END) AS total_cr,
  SUM(CASE WHEN dr_cr = 'DR' THEN amount ELSE -amount END) AS net_dr_minus_cr
FROM demo.jnl_dr_cr_postings
GROUP BY posting_date, ccy
ORDER BY posting_date, ccy;


-- COMMAND ----------

WITH j AS (
  SELECT
    posting_date,
    CAST(account_code AS STRING) AS account_code,
    ccy,
    SUM(CASE WHEN dr_cr='DR' THEN amount ELSE -amount END) AS delta
  FROM demo.jnl_dr_cr_postings
  GROUP BY posting_date, CAST(account_code AS STRING), ccy
),
j_cum AS (
  SELECT
    posting_date,
    account_code,
    ccy,
    SUM(delta) OVER (PARTITION BY account_code, ccy ORDER BY posting_date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance_from_journals
  FROM j
)
SELECT
  t.posting_date,
  t.account_code,
  t.ccy,
  t.balance AS balance_thin_ledger,
  j.balance_from_journals,
  (t.balance - j.balance_from_journals) AS diff
FROM demo.tb_thin_ledger t
JOIN j_cum j
  ON t.posting_date = j.posting_date
 AND CAST(t.account_code AS STRING) = j.account_code
 AND t.ccy = j.ccy
ORDER BY t.posting_date, t.account_code, t.ccy;


-- COMMAND ----------

WITH j_day AS (
  SELECT
    posting_date,
    account_code,
    ccy,
    SUM(CASE WHEN dr_cr='DR' THEN amount ELSE -amount END) AS net_journal
  FROM demo.jnl_dr_cr_postings
  WHERE account_code IN ('400100','400200')
  GROUP BY posting_date, account_code, ccy
),
t_day AS (
  SELECT
    posting_date,
    CAST(account_code AS STRING) AS account_code,
    ccy,
    balance
  FROM demo.tb_thin_ledger
  WHERE CAST(account_code AS STRING) IN ('400100','400200')
)
SELECT
  t.posting_date,
  t.account_code,
  t.ccy,
  t.balance,
  SUM(j.net_journal) OVER (
    PARTITION BY t.account_code, t.ccy ORDER BY t.posting_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS bal_from_journals,
  t.balance - SUM(j.net_journal) OVER (
    PARTITION BY t.account_code, t.ccy ORDER BY t.posting_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS diff
FROM t_day t
LEFT JOIN j_day j
  ON t.posting_date = j.posting_date
 AND t.account_code = j.account_code
 AND t.ccy = j.ccy
ORDER BY t.posting_date, t.account_code;


-- COMMAND ----------

WITH mtm AS (
  SELECT
    as_of_date AS posting_date,
    ccy,
    SUM(fo_mtm) AS fo_mtm_total
  FROM demo.fo_mtm_timeseries
  GROUP BY as_of_date, ccy
),
mtm_delta AS (
  SELECT
    posting_date,
    ccy,
    fo_mtm_total - LAG(fo_mtm_total) OVER (PARTITION BY ccy ORDER BY posting_date) AS fo_mtm_change
  FROM mtm
),
j_net AS (
  SELECT
    posting_date,
    ccy,
    SUM(CASE WHEN account_code='400200' AND dr_cr='DR' THEN amount
             WHEN account_code='400200' AND dr_cr='CR' THEN -amount
             ELSE 0 END) AS journal_mtm_change_400200
  FROM demo.jnl_dr_cr_postings
  GROUP BY posting_date, ccy
)
SELECT
  d.posting_date,
  d.ccy,
  d.fo_mtm_change,
  j.journal_mtm_change_400200,
  d.fo_mtm_change - j.journal_mtm_change_400200 AS diff
FROM mtm_delta d
LEFT JOIN j_net j
  ON d.posting_date = j.posting_date AND d.ccy = j.ccy
ORDER BY d.posting_date;


-- COMMAND ----------

WITH fo AS (
  SELECT
    customer_id,
    isin,
    ccy,
    as_of_date AS posting_date,
    fo_mtm
      - LAG(fo_mtm) OVER (PARTITION BY customer_id, isin, ccy ORDER BY as_of_date) AS fo_mtm_change
  FROM demo.fo_mtm_timeseries
),
j AS (
  SELECT
    customer_id,
    isin,
    ccy,
    posting_date,
    SUM(
      CASE
        WHEN CAST(account_code AS STRING) = '400200' AND dr_cr = 'DR' THEN amount
        WHEN CAST(account_code AS STRING) = '400200' AND dr_cr = 'CR' THEN -amount
        ELSE 0
      END
    ) AS journal_mtm_change
  FROM demo.jnl_dr_cr_postings
  GROUP BY customer_id, isin, ccy, posting_date
)
SELECT
  fo.posting_date,
  fo.customer_id,
  fo.isin,
  fo.ccy,
  fo.fo_mtm_change,
  COALESCE(j.journal_mtm_change, 0) AS journal_mtm_change,
  fo.fo_mtm_change - COALESCE(j.journal_mtm_change, 0) AS diff
FROM fo
LEFT JOIN j
  ON fo.customer_id = j.customer_id
 AND fo.isin = j.isin
 AND fo.ccy = j.ccy
 AND fo.posting_date = j.posting_date
WHERE fo.posting_date IN ('2025-01-06','2025-01-08','2025-01-10','2025-01-11','2025-01-12')
  AND fo.fo_mtm_change IS NOT NULL
  AND ABS(fo.fo_mtm_change - COALESCE(j.journal_mtm_change, 0)) > 0.000001
ORDER BY fo.posting_date, ABS(diff) DESC;


-- COMMAND ----------

SELECT
  posting_date,
  account_code,
  dr_cr,
  SUM(amount) AS amt
FROM demo.jnl_dr_cr_postings
WHERE customer_id = 'CIF002'
  AND isin = 'GB0000000003'
  AND posting_date IN ('2025-01-06','2025-01-08','2025-01-10','2025-01-11','2025-01-12')
GROUP BY posting_date, account_code, dr_cr
ORDER BY posting_date, account_code, dr_cr;
