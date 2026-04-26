/* ============================================================================
   PROJECT:     Payment Calculation System
   PURPOSE:     Schema creation, data seeding, and formula population
   TARGET DB:   SQL Server (.\SQLEXPRESS)
   NOTES:       - Follows exact spec from task requirements
                - All PKs use IDENTITY — no manual key management
                - Random data seeded with +1.x offset to prevent zero values,
                  which would cause errors in log() and division formulas
   ============================================================================ */

-- ============================================================================
-- 1. DATABASE INITIALIZATION
-- Creates the database only if it does not already exist
-- ============================================================================
IF DB_ID('PaymentSystemDB') IS NULL
BEGIN
    CREATE DATABASE PaymentSystemDB;
END
GO

USE PaymentSystemDB;
GO

-- ============================================================================
-- 2. CLEAN SLATE
-- Drop tables in reverse FK dependency order to avoid constraint violations.
-- Safe to run multiple times — IF OBJECT_ID guards against missing objects.
-- ============================================================================
IF OBJECT_ID('dbo.t_results', 'U') IS NOT NULL DROP TABLE dbo.t_results;
IF OBJECT_ID('dbo.t_log',     'U') IS NOT NULL DROP TABLE dbo.t_log;
IF OBJECT_ID('dbo.t_targil',  'U') IS NOT NULL DROP TABLE dbo.t_targil;
IF OBJECT_ID('dbo.t_data',    'U') IS NOT NULL DROP TABLE dbo.t_data;
GO

-- ============================================================================
-- 3. TABLE DEFINITIONS
-- Exact structure as specified in the task requirements document.
-- ============================================================================

-- t_data: source data table — 1M rows of random float values
CREATE TABLE t_data
(
    data_id INT   IDENTITY(1,1) NOT NULL PRIMARY KEY,  -- unique row identifier
    a       FLOAT NOT NULL,                             -- numeric field 1
    b       FLOAT NOT NULL,                             -- numeric field 2
    c       FLOAT NOT NULL,                             -- numeric field 3
    d       FLOAT NOT NULL                              -- numeric field 4
);
GO

-- t_targil: formula definitions table
-- Supports both simple expressions and conditional (IF) formulas
CREATE TABLE t_targil
(
    targil_id    INT          IDENTITY(1,1) NOT NULL PRIMARY KEY,  -- unique formula identifier
    targil       VARCHAR(500) NOT NULL,   -- main formula expression (e.g. "a + b", "if(a > 5, b * 2, b / 2)")
    tnai         VARCHAR(500) NULL,       -- condition for IF formulas (e.g. "a > 5")
    targil_false VARCHAR(500) NULL        -- ELSE branch for IF formulas (e.g. "b / 2")
);
GO

-- t_results: stores the calculated result for every data row × formula × method
-- resultsl_id name preserved exactly as written in the task spec
CREATE TABLE t_results
(
    results_id INT          IDENTITY(1,1) NOT NULL PRIMARY KEY,  -- unique result identifier
    data_id     INT          NOT NULL,                             -- FK → t_data
    targil_id   INT          NOT NULL,                             -- FK → t_targil
    method      VARCHAR(100) NOT NULL,                             -- calculation method (SQL_DB / CSharp_File / Python_Bulk)
    result      FLOAT        NULL,                                 -- calculated value
    CONSTRAINT FK_t_results_data   FOREIGN KEY (data_id)   REFERENCES dbo.t_data(data_id),
    CONSTRAINT FK_t_results_targil FOREIGN KEY (targil_id) REFERENCES dbo.t_targil(targil_id)
);
GO

-- t_log: execution time log — one row per formula per method run
CREATE TABLE t_log
(
    log_id    INT          IDENTITY(1,1) NOT NULL PRIMARY KEY,  -- unique log entry identifier
    targil_id INT          NOT NULL,                             -- FK → t_targil
    method    VARCHAR(100) NOT NULL,                             -- calculation method
    run_time  FLOAT        NULL,                                 -- execution duration in seconds
    CONSTRAINT FK_t_log_targil FOREIGN KEY (targil_id) REFERENCES dbo.t_targil(targil_id)
);
GO

-- ============================================================================
-- 4. DATA POPULATION — 1,000,000 rows in t_data
--
-- Strategy: CTE generates exactly 1M row numbers via ROW_NUMBER().
-- RAND(CHECKSUM(NEWID())) produces a different random value per row.
-- +1.x offset ensures values are never zero, preventing:
--   - log(0) = -infinity errors
--   - division-by-zero in formulas like "a / b"
-- Each column gets a different offset (1.1–1.4) to ensure distinct distributions.
-- ============================================================================
;WITH N AS
(
    SELECT TOP (1000000)
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.all_objects a
    CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.t_data (a, b, c, d)
SELECT
    CAST(RAND(CHECKSUM(NEWID())) * 100 + 1.1 AS FLOAT),  -- range: [1.1, 101.1)
    CAST(RAND(CHECKSUM(NEWID())) * 100 + 1.2 AS FLOAT),  -- range: [1.2, 101.2)
    CAST(RAND(CHECKSUM(NEWID())) * 100 + 1.3 AS FLOAT),  -- range: [1.3, 101.3)
    CAST(RAND(CHECKSUM(NEWID())) * 100 + 1.4 AS FLOAT)   -- range: [1.4, 101.4)
FROM N;
GO

-- ============================================================================
-- 5. FORMULA SEEDING — t_targil
--
-- Three categories as required by the task:
--   I.  Simple    — basic arithmetic operations
--   II. Complex   — mathematical functions (sqrt, log, sin, etc.)
--   III.Conditional — IF formulas using the if(condition, then, else) syntax
--
-- Conditional formulas store:
--   targil       = full if(...) expression (used by C# and Python)
--   tnai         = condition only          (used by SQL stored procedure)
--   targil_false = ELSE branch only        (used by SQL stored procedure)
-- ============================================================================
INSERT INTO dbo.t_targil (targil, tnai, targil_false)
VALUES
    -- I. Simple arithmetic (baseline performance reference)
    ('a + b',           NULL, NULL),   -- addition of two fields
    ('c * 2',           NULL, NULL),   -- multiplication by constant
    ('b - a',           NULL, NULL),   -- subtraction of two fields
    ('d / 4',           NULL, NULL),   -- division by constant
    ('a + b + c + d',   NULL, NULL),   -- sum of all fields
    ('100 - d',         NULL, NULL),   -- subtraction from constant

    -- II. Complex mathematical functions (CPU-intensive)
    ('(a + b) * 8',              NULL, NULL),  -- grouped arithmetic
    ('sqrt(c*c + d*d)',          NULL, NULL),  -- Euclidean distance (Pythagorean theorem)
    ('log(b + 1) + c',           NULL, NULL),  -- natural log — +1 guards against log(0)
    ('abs(d - b)',                NULL, NULL),  -- absolute difference
    ('sin(a) + cos(b)',           NULL, NULL),  -- trigonometric combination
    ('exp(a / 10)',               NULL, NULL),  -- exponential growth
    ('power(a, 3) + (b * c)',    NULL, NULL),  -- polynomial expression
    ('floor(d) + 0.5',           NULL, NULL),  -- rounding with offset

    -- III. Conditional formulas — if(condition, then, else)
    -- tnai and targil_false are stored separately for the SQL stored procedure
    ('if(a > 5, b * 2, b / 2)',       'a > 5',       'b / 2'),   -- scale b based on a
    ('if(b < 10, a + 1, d - 1)',      'b < 10',      'd - 1'),   -- branch on b threshold
    ('if(a == c, 1, 0)',              'a == c',      '0'),        -- equality check → binary flag
    ('if(d >= 50, a * b, c + d)',     'd >= 50',     'c + d'),   -- threshold-based product
    ('if(b != 0, a / b, 0)',          'b != 0',      '0'),        -- safe division guard
    ('if(a + b > 100, 100, a + b)',   'a + b > 100', 'a + b'),   -- value clamping
    ('if(c > d, abs(c-d), abs(d-c))', 'c > d',       'abs(d-c)'); -- absolute difference with condition
GO

-- ============================================================================
-- 6. VERIFICATION
-- Quick sanity checks after seeding
-- ============================================================================
SELECT COUNT(*) AS total_data_rows  FROM dbo.t_data;
SELECT COUNT(*) AS total_formulas   FROM dbo.t_targil;

-- View all seeded formulas
SELECT targil_id, targil, tnai, targil_false
FROM dbo.t_targil
ORDER BY targil_id;
GO
