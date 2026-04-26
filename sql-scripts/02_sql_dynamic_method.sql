USE PaymentSystemDB;
GO

-- ============================================================================
-- Stored Procedure: sp_CalculateDynamicFormula
--
-- Dynamically evaluates a single formula from t_targil against all rows
-- in t_data, then writes results to t_results and logs runtime to t_log.
--
-- Formula types supported:
--   Simple      : tnai IS NULL
--                 targil = full expression (e.g. "a + b")
--                 → evaluated directly as SQL expression
--
--   Conditional : tnai IS NOT NULL
--                 targil       = full if(...) string (e.g. "if(a > 5, b * 2, b / 2)")
--                 tnai         = condition           (e.g. "a > 5")
--                 targil_false = ELSE branch         (e.g. "b / 2")
--                 → THEN branch extracted from targil between first and second comma
--                 → Builds: CASE WHEN (tnai) THEN (then_branch) ELSE (targil_false) END
-- ============================================================================
CREATE OR ALTER PROCEDURE sp_CalculateDynamicFormula
    @TargilId INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Condition    NVARCHAR(MAX),
            @FullFormula  NVARCHAR(MAX),
            @FormulaFalse NVARCHAR(MAX),
            @ThenBranch   NVARCHAR(MAX),
            @FinalExpr    NVARCHAR(MAX),
            @SQL          NVARCHAR(MAX),
            @StartTime    DATETIME2(7) = SYSDATETIME();

    -- Fetch formula definition from t_targil
    SELECT
        @Condition    = tnai,
        @FullFormula  = targil,
        @FormulaFalse = targil_false
    FROM t_targil
    WHERE targil_id = @TargilId;

    -- Exit gracefully if formula does not exist
    IF @FullFormula IS NULL RETURN;

    -- Normalize comparison operators to valid T-SQL syntax.
    -- ISNULL guards against NULL before REPLACE.
    -- Order matters: != must be replaced before = to avoid partial matches.
    SET @Condition    = REPLACE(REPLACE(ISNULL(@Condition,    ''), '!=', '<>'), '==', '=');
    SET @FormulaFalse = REPLACE(REPLACE(ISNULL(@FormulaFalse, ''), '!=', '<>'), '==', '=');

    -- Build the final SQL expression
    IF NULLIF(LTRIM(RTRIM(@Condition)), '') IS NOT NULL
    BEGIN
        -- Conditional formula: tnai IS NOT NULL
        -- targil contains the full if(...) string — extract the THEN branch
        -- by finding the text between the first and second comma.
        -- Example: "if(a > 5, b * 2, b / 2)"
        --           first comma at pos 9, second comma at pos 15
        --           THEN branch = "b * 2"
        DECLARE @FirstComma  INT = CHARINDEX(',', @FullFormula);
        DECLARE @SecondComma INT = CHARINDEX(',', @FullFormula, @FirstComma + 1);

        SET @ThenBranch = LTRIM(RTRIM(
            SUBSTRING(@FullFormula, @FirstComma + 1, @SecondComma - @FirstComma - 1)
        ));

        SET @FinalExpr =
            N'CASE WHEN (' + @Condition  + N') ' +
            N'THEN ('      + @ThenBranch + N') ' +
            N'ELSE ('      + @FormulaFalse + N') END';
    END
    ELSE
    BEGIN
        -- Simple formula — use targil directly as a SQL expression
        SET @FinalExpr = @FullFormula;
    END

    -- Build the dynamic INSERT statement.
    -- @p_targil_id passed as parameter to prevent SQL injection.
    -- NOLOCK on t_data avoids shared locks during the read.
    SET @SQL =
        N'INSERT INTO t_results (data_id, targil_id, method, result) ' +
        N'SELECT d.data_id, @p_targil_id, ''SQL'', CAST((' + @FinalExpr + N') AS FLOAT) ' +
        N'FROM t_data d WITH (NOLOCK);';

    BEGIN TRY
        EXEC sp_executesql
            @SQL,
            N'@p_targil_id INT',
            @p_targil_id = @TargilId;

        -- Log execution time in seconds with microsecond precision.
        -- DATEDIFF_BIG prevents integer overflow on long-running queries.
        INSERT INTO t_log (targil_id, method, run_time)
        VALUES (
            @TargilId,
            'SQL',
            CAST(DATEDIFF_BIG(MICROSECOND, @StartTime, SYSDATETIME()) AS FLOAT) / 1000000.0
        );
    END TRY
    BEGIN CATCH
        -- Re-raise with context about which formula failed.
        -- THROW preserves the original error state (preferred over RAISERROR).
        DECLARE @Msg NVARCHAR(MAX) =
            N'sp_CalculateDynamicFormula failed for targil_id=' +
            CAST(@TargilId AS NVARCHAR(10)) + N': ' + ERROR_MESSAGE();
        THROW 50001, @Msg, 1;
    END CATCH
END
GO
-- ============================================================================
-- Clean previous run — delete only SQL data before re-running.
-- Other methods' data is preserved for cross-method comparison.
-- ============================================================================
DELETE FROM t_results WHERE method = 'SQL';
DELETE FROM t_log     WHERE method = 'SQL';
GO
-- ============================================================================
-- Execution Block
-- Iterates over all formulas in t_targil and calls the procedure for each.
--
-- CURSOR options:
--   LOCAL        — scoped to this batch only, not visible to other sessions
--   FAST_FORWARD — read-only, forward-only: the fastest cursor type in SQL Server
--   READ_ONLY    — explicitly prevents accidental updates through the cursor
-- ============================================================================
DECLARE @Id INT;

DECLARE cur CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
    SELECT targil_id
    FROM t_targil
    ORDER BY targil_id;

OPEN cur;
FETCH NEXT FROM cur INTO @Id;

WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC sp_CalculateDynamicFormula @TargilId = @Id;
    FETCH NEXT FROM cur INTO @Id;
END

CLOSE cur;
DEALLOCATE cur;
GO

-- ============================================================================
-- Verification
-- ============================================================================
SELECT method, COUNT(*) AS total_rows
FROM t_results
GROUP BY method;

SELECT targil_id, method, run_time
FROM t_log
WHERE method = 'SQL'
ORDER BY targil_id;
GO
