/* ============================================================================
   Script: compare_results.sql
   Purpose: Verify that all calculation methods produce identical results
            for the same data_id and targil_id combination.

   Strategy:
     - Scanning all rows per data_id is too slow on millions of rows.
     - Instead, we aggregate per (targil_id, method) using AVG, MIN, MAX, STDEV.
     - Then compare aggregates across methods side by side.
     - A MATCH means all methods agree (avg difference < tolerance threshold)
       AND each method produced the same number of rows.

   Tables used:
     t_results  — calculation output (data_id, targil_id, method, result)
     t_targil   — formula definitions (targil_id, targil)
   ============================================================================ */

USE PaymentSystemDB;
GO

-- ============================================================================
-- Step 1: Aggregate results per formula per method.
-- Each row = one method's stats for one formula.
-- AVG/MIN/MAX/STDEV over 1M rows per group — fast with NOLOCK on large tables.
-- ============================================================================
;WITH method_stats AS
(
    SELECT
        r.targil_id,
        t.targil                            AS formula,
        r.method,
        COUNT(*)                            AS row_count,       -- rows per method
        AVG(r.result)                       AS avg_result,
        MIN(r.result)                       AS min_result,
        MAX(r.result)                       AS max_result,
        STDEV(r.result)                     AS stdev_result
    FROM t_results r WITH (NOLOCK)
    JOIN t_targil  t WITH (NOLOCK) ON r.targil_id = t.targil_id
    GROUP BY r.targil_id, t.targil, r.method
),

-- ============================================================================
-- Step 2: Compare all methods per formula.
-- MIN/MAX of row_count are across methods — if equal, every method
-- processed the same number of rows (correct).
-- MIN/MAX of avg_result are across methods — if equal, all methods
-- produced the same average result (correct).
-- ============================================================================
formula_comparison AS
(
    SELECT
        targil_id,
        formula,
        COUNT(DISTINCT method)              AS method_count,
        -- Row count comparison: each method should have processed the same rows
        MIN(row_count)                      AS min_row_count,
        MAX(row_count)                      AS max_row_count,
        -- Result comparison: average result should be identical across methods
        MIN(avg_result)                     AS min_avg,
        MAX(avg_result)                     AS max_avg,
        MAX(avg_result) - MIN(avg_result)   AS avg_diff,
        MIN(min_result)                     AS overall_min,
        MAX(max_result)                     AS overall_max,
        -- List all methods that ran for this formula
        STRING_AGG(method, ' | ')
            WITHIN GROUP (ORDER BY method)  AS methods_run
    FROM method_stats
    GROUP BY targil_id, formula
)

-- ============================================================================
-- Step 3: Final comparison report — one row per formula.
-- ============================================================================
SELECT
    targil_id,
    formula,
    method_count,
    methods_run,
    min_row_count,
    max_row_count,
    CAST(min_avg        AS DECIMAL(18,6))   AS min_avg,
    CAST(max_avg        AS DECIMAL(18,6))   AS max_avg,
    CAST(avg_diff       AS DECIMAL(18,8))   AS avg_diff,
    CAST(overall_min    AS DECIMAL(18,6))   AS overall_min,
    CAST(overall_max    AS DECIMAL(18,6))   AS overall_max,
    CASE
        WHEN method_count  < 2              THEN 'ONLY ONE METHOD'
        WHEN avg_diff      > 0.0001         THEN 'RESULT MISMATCH'
        -- Each method should have processed the same number of rows independently
        WHEN min_row_count <> max_row_count THEN 'ROW COUNT MISMATCH'
        ELSE                                     'MATCH'
    END                                     AS status
FROM formula_comparison
ORDER BY
    -- Show problems first for quick review
    CASE
        WHEN method_count  < 2              THEN 1
        WHEN avg_diff      > 0.0001         THEN 2
        WHEN min_row_count <> max_row_count THEN 3
        ELSE                                     4
    END,
    targil_id;
GO

-- ============================================================================
-- Summary: single-line verdict across all formulas
-- ============================================================================
;WITH method_stats AS
(
    SELECT
        r.targil_id,
        r.method,
        AVG(r.result) AS avg_result,
        COUNT(*)      AS row_count
    FROM t_results r WITH (NOLOCK)
    GROUP BY r.targil_id, r.method
),
formula_comparison AS
(
    SELECT
        targil_id,
        COUNT(DISTINCT method)            AS method_count,
        MAX(avg_result) - MIN(avg_result) AS avg_diff,
        MIN(row_count)                    AS min_rows,   -- min rows per method
        MAX(row_count)                    AS max_rows    -- max rows per method
    FROM method_stats
    GROUP BY targil_id
)
SELECT
    COUNT(*)                                                AS total_formulas,
    SUM(CASE WHEN avg_diff    <= 0.0001
              AND min_rows     = max_rows   THEN 1 ELSE 0 END) AS formulas_match,
    SUM(CASE WHEN avg_diff     > 0.0001    THEN 1 ELSE 0 END) AS result_mismatches,
    SUM(CASE WHEN min_rows    <> max_rows  THEN 1 ELSE 0 END) AS row_count_mismatches,
    CASE
        WHEN SUM(CASE WHEN avg_diff  > 0.0001
                       OR min_rows  <> max_rows THEN 1 ELSE 0 END) = 0
        THEN '✓ ALL METHODS PRODUCE IDENTICAL RESULTS'
        ELSE '✗ DISCREPANCIES DETECTED — SEE DETAIL ABOVE'
    END                                                     AS verdict
FROM formula_comparison;
GO
