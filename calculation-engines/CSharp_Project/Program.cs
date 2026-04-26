using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;

namespace EducationProject
{
    // -------------------------------------------------------------------------
    // Dynamic Formula Calculator — C# Implementation
    //
    // Strategy:
    //   1. Load all 1M rows from t_data into memory once (avoids repeated I/O).
    //   2. Compile ALL formulas in parallel using Roslyn — each formula becomes
    //      a native .NET delegate (real IL, not interpreted).
    //   3. Evaluate each delegate across all rows using Parallel.For,
    //      utilizing all available CPU cores.
    //   4. Stream results into SQL Server via SqlBulkCopy + custom IDataReader,
    //      bypassing row-by-row INSERT overhead and avoiding DataTable allocation.
    //   5. Record execution time per formula in t_log.
    // -------------------------------------------------------------------------

    class Program
    {
        const string ConnectionString =
            @"Server=.\SQLEXPRESS;Database=PaymentSystemDB;" +
             "Trusted_Connection=True;Encrypt=False;";

        // Single source of truth for the method name —
        // referenced by both BulkDataReader and WriteLog
        internal const string Method = "CSharp";

        static void Main()
        {
            Console.WriteLine("=== Dynamic Formula Calculator — C# ===\n");

            try
            {
                // --- Clean previous run ---
                // Delete only this method's data to allow clean re-runs
                // without affecting other methods' results for comparison.
                Console.WriteLine("Cleaning previous run data...");
                using (var conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    using var cmd = new SqlCommand(
                        "DELETE FROM t_results WHERE method = @m; " +
                        "DELETE FROM t_log     WHERE method = @m", conn);
                    cmd.CommandTimeout = 0;
                    cmd.Parameters.Add("@m", SqlDbType.VarChar, 100).Value = Method;
                    cmd.ExecuteNonQuery();
                }
                Console.WriteLine("  Done.\n");

                var formulas = DbHelper.GetFormulas(ConnectionString);

                // Load data without NOLOCK — ensures consistent reads
                // so results are comparable across methods
                var data = DataLoader.LoadAll(ConnectionString);

                Console.WriteLine($"Loaded {data.Count:N0} data rows and " +
                                  $"{formulas.Count} formulas.");
                Console.WriteLine("Pre-compiling all formulas in parallel...\n");

                // Compile all formulas in parallel before processing.
                // Roslyn compilation (~200ms each) is CPU-bound and parallelizes well.
                var compiled = formulas
                    .AsParallel()
                    .AsOrdered()
                    .Select(f =>
                    {
                        bool ok = FormulaCompiler.TryCompile(f.Expression, out var fn);
                        return (Formula: f, Ok: ok, Func: fn);
                    })
                    .ToList();

                // Open a single shared connection for all bulk inserts and logs.
                // Avoids the overhead of opening/closing a connection per formula.
                using var sharedConn = new SqlConnection(ConnectionString);
                sharedConn.Open();

                // Track total invalid values across all formulas for final report
                int totalInvalidCount = 0;

                foreach (var (formula, ok, func) in compiled)
                {
                    Console.WriteLine($"[Formula {formula.Id:D2}] {formula.Expression}");

                    if (!ok)
                    {
                        Console.WriteLine("  [SKIP] Compilation failed.\n");
                        continue;
                    }

                    int invalidCount = ProcessFormula(formula, data, func, sharedConn);
                    totalInvalidCount += invalidCount;
                }

                // Final report — explicitly states NaN/Infinity replacement policy
                // so cross-method comparison accuracy is transparent
                Console.WriteLine("\n" + new string('=', 50));
                Console.WriteLine("FINAL REPORT");
                Console.WriteLine(new string('=', 50));
                Console.WriteLine($"  Formulas processed : {compiled.Count(c => c.Ok)}");
                Console.WriteLine($"  Total data rows    : {data.Count:N0}");
                if (totalInvalidCount > 0)
                    Console.WriteLine($"  [NOTE] {totalInvalidCount:N0} NaN/Infinity values were " +
                                      $"replaced with 0.0 — these rows may affect " +
                                      $"cross-method comparison accuracy.");
                else
                    Console.WriteLine("  All calculated values are finite — " +
                                      "results are fully comparable across methods.");
                Console.WriteLine(new string('=', 50));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nFATAL: {ex.Message}");
            }

            Console.WriteLine("\nAll formulas processed. Press any key to exit...");
            Console.ReadKey();
        }

        // Returns count of invalid (NaN/Infinity) values for final report.
        // Allows Main to aggregate totals across all formulas.
        static int ProcessFormula(
            FormulaModel formula,
            List<DataRow> data,
            Func<double, double, double, double, double> func,
            SqlConnection conn)
        {
            int rowCount = data.Count;
            int invalidCount = 0;

            // --- Phase 1: Parallel evaluation across all CPU cores ---
            // Parallel.For splits the 1M rows across available cores.
            // Each thread writes to its own index — no locking needed.
            // Interlocked.Increment used for thread-safe counter update.
            var sw = Stopwatch.StartNew();
            var results = new double[rowCount];

            Parallel.For(0, rowCount, i =>
            {
                double val = func(data[i].A, data[i].B, data[i].C, data[i].D);

                // Guard against NaN and Infinity — these cannot be stored as FLOAT
                // and would cause bulk insert failures or corrupt comparisons.
                // Store as 0.0 and track count for diagnostics.
                if (double.IsFinite(val))
                    results[i] = val;
                else
                {
                    results[i] = 0.0;
                    System.Threading.Interlocked.Increment(ref invalidCount);
                }
            });

            double calcSec = sw.Elapsed.TotalSeconds;

            // Warn immediately if this formula produced invalid values
            if (invalidCount > 0)
                Console.WriteLine($"  [WARNING] {invalidCount:N0} NaN/Infinity values " +
                                  $"replaced with 0.0");

            // --- Phase 2: Bulk insert via SqlBulkCopy ---
            sw.Restart();
            BulkInsert(data, results, formula.Id, rowCount, conn);
            double loadSec = sw.Elapsed.TotalSeconds;

            // --- Phase 3: Log total execution time ---
            double totalSec = calcSec + loadSec;
            DbHelper.WriteLog(formula.Id, Method, totalSec, conn);

            Console.WriteLine($"  Calc: {calcSec:F3}s | Load: {loadSec:F3}s " +
                              $"| Total: {totalSec:F3}s\n");

            return invalidCount;
        }

        // Streams calculated results into t_results using SqlBulkCopy.
        // Reuses the shared connection — no open/close overhead per formula.
        // Column mapping skips resultsl_id (IDENTITY) — matches exact schema name.
        static void BulkInsert(List<DataRow> data, double[] results,
                               int targilId, int rowCount, SqlConnection conn)
        {
            using var bulk = new SqlBulkCopy(conn)
            {
                DestinationTableName = "t_results",
                BatchSize = 500_000,
                BulkCopyTimeout = 0
            };

            // Column names match the exact schema — resultsl_id is IDENTITY, excluded
            bulk.ColumnMappings.Add("data_id", "data_id");
            bulk.ColumnMappings.Add("targil_id", "targil_id");
            bulk.ColumnMappings.Add("method", "method");
            bulk.ColumnMappings.Add("result", "result");

            // BulkDataReader streams rows from memory — no DataTable allocation
            bulk.WriteToServer(new BulkDataReader(data, results, targilId, rowCount));
        }
    }

    // -------------------------------------------------------------------------
    // BulkDataReader
    // Wraps two parallel arrays as an IDataReader for SqlBulkCopy.
    // Avoids allocating a DataTable (~400MB for 1M rows).
    // Method name sourced from Program.Method — single source of truth.
    // -------------------------------------------------------------------------
    public sealed class BulkDataReader : IDataReader
    {
        private readonly List<DataRow> _data;
        private readonly double[] _results;
        private readonly int _targilId;
        private readonly int _count;
        private int _index = -1;

        public BulkDataReader(List<DataRow> data, double[] results,
                              int targilId, int count)
        {
            _data = data;
            _results = results;
            _targilId = targilId;
            _count = count;
        }

        public bool Read() => ++_index < _count;

        public int FieldCount => 4;
        public bool IsClosed => false;
        public int Depth => 0;
        public int RecordsAffected => -1;

        public object GetValue(int i) => i switch
        {
            0 => _data[_index].DataId,
            1 => _targilId,
            2 => Program.Method,   // single source of truth — no hardcoded string
            3 => _results[_index],
            _ => throw new IndexOutOfRangeException($"Column index {i} out of range.")
        };

        public int GetValues(object[] values)
        {
            int count = Math.Min(values.Length, FieldCount);
            for (int i = 0; i < count; i++)
                values[i] = GetValue(i);
            return count;
        }

        public string GetName(int i) => i switch
        {
            0 => "data_id",
            1 => "targil_id",
            2 => "method",
            3 => "result",
            _ => throw new IndexOutOfRangeException()
        };

        public int GetOrdinal(string name)
        {
            for (int i = 0; i < FieldCount; i++)
                if (GetName(i) == name) return i;
            throw new IndexOutOfRangeException(name);
        }

        public Type GetFieldType(int i) => i switch
        {
            0 => typeof(int),
            1 => typeof(int),
            2 => typeof(string),
            3 => typeof(double),
            _ => throw new IndexOutOfRangeException()
        };

        public string GetDataTypeName(int i) => GetFieldType(i).Name;
        public bool IsDBNull(int i) => false;
        public object this[int i] => GetValue(i);
        public object this[string name] => GetValue(GetOrdinal(name));
        public void Dispose() { }
        public void Close() { }
        public bool NextResult() => false;
        public DataTable GetSchemaTable() => null;
        public int GetInt32(int i) => (int)GetValue(i);
        public double GetDouble(int i) => (double)GetValue(i);
        public string GetString(int i) => (string)GetValue(i);
        public bool GetBoolean(int i) => throw new NotSupportedException();
        public byte GetByte(int i) => throw new NotSupportedException();
        public long GetBytes(int i, long fo, byte[] b, int bo, int l) => throw new NotSupportedException();
        public char GetChar(int i) => throw new NotSupportedException();
        public long GetChars(int i, long fo, char[] b, int bo, int l) => throw new NotSupportedException();
        public Guid GetGuid(int i) => throw new NotSupportedException();
        public short GetInt16(int i) => throw new NotSupportedException();
        public long GetInt64(int i) => throw new NotSupportedException();
        public float GetFloat(int i) => throw new NotSupportedException();
        public decimal GetDecimal(int i) => throw new NotSupportedException();
        public DateTime GetDateTime(int i) => throw new NotSupportedException();
        public IDataReader GetData(int i) => throw new NotSupportedException();
    }

    public sealed class DataRow
    {
        public int DataId;
        public double A, B, C, D;
    }

    public sealed class FormulaModel
    {
        public int Id;
        public string Expression;
    }

    // -------------------------------------------------------------------------
    // DataLoader — loads t_data without NOLOCK to ensure consistent reads.
    // Consistent reads are required for valid cross-method result comparison.
    // -------------------------------------------------------------------------
    public static class DataLoader
    {
        public static List<DataRow> LoadAll(string connStr)
        {
            var list = new List<DataRow>(1_000_000);

            using var conn = new SqlConnection(connStr);
            conn.Open();

            using var cmd = new SqlCommand(
                // No NOLOCK — consistent reads required for benchmark accuracy
                "SELECT data_id, a, b, c, d FROM t_data", conn)
            {
                CommandTimeout = 0
            };

            using var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess);

            while (reader.Read())
                list.Add(new DataRow
                {
                    DataId = reader.GetInt32(0),
                    A = reader.GetDouble(1),
                    B = reader.GetDouble(2),
                    C = reader.GetDouble(3),
                    D = reader.GetDouble(4)
                });

            return list;
        }
    }

    // -------------------------------------------------------------------------
    // DbHelper — data access helpers.
    // All methods accept an existing SqlConnection to avoid repeated open/close.
    // -------------------------------------------------------------------------
    public static class DbHelper
    {
        public static List<FormulaModel> GetFormulas(string connStr)
        {
            var list = new List<FormulaModel>();

            using var conn = new SqlConnection(connStr);
            conn.Open();

            using var cmd = new SqlCommand(
                "SELECT targil_id, targil FROM t_targil ORDER BY targil_id", conn);
            using var r = cmd.ExecuteReader();

            while (r.Read())
                list.Add(new FormulaModel
                {
                    Id = r.GetInt32(0),
                    Expression = r.GetString(1)
                });

            return list;
        }

        // Accepts shared connection — no new connection opened per formula.
        // Uses explicit SqlDbType with size to match schema exactly.
        public static void WriteLog(int targilId, string method,
                                    double totalSec, SqlConnection conn)
        {
            using var cmd = new SqlCommand(
                "INSERT INTO t_log (targil_id, method, run_time) " +
                "VALUES (@t, @m, @r)", conn);

            // Explicit SqlDbType with size — avoids type inference issues
            cmd.Parameters.Add("@t", SqlDbType.Int).Value = targilId;
            cmd.Parameters.Add("@m", SqlDbType.VarChar, 100).Value = method;
            cmd.Parameters.Add("@r", SqlDbType.Float).Value = totalSec;

            cmd.ExecuteNonQuery();
        }
    }

    // -------------------------------------------------------------------------
    // FormulaCompiler
    //
    // Rewrites formula strings into valid C# and compiles to native delegates.
    //
    // if() rewriting uses a character-level parser instead of a simple regex —
    // correctly handles nested calls and expressions with commas (e.g. power(a, 2)).
    //
    // Thread-safe: ConcurrentDictionary cache for parallel compilation.
    // -------------------------------------------------------------------------
    public static class FormulaCompiler
    {
        private static readonly System.Collections.Concurrent.ConcurrentDictionary
            <string, Func<double, double, double, double, double>>
            _cache = new(StringComparer.Ordinal);

        private static readonly Regex FunctionRegex = new(
            @"\b(log10|log2|atan2|asin|acos|atan|sinh|cosh|tanh|" +
            @"sin|cos|tan|exp|sqrt|cbrt|log|ln|pow|power|abs|sign|" +
            @"floor|ceil|ceiling|round|truncate|trunc|min|max|clamp)\b",
            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        // Word-boundary aware if() detection — prevents false matches like "diff("
        private static readonly Regex IfPattern = new(
            @"\bif\s*\(",
            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private static string MapFunction(Match m) => m.Value.ToLowerInvariant() switch
        {
            "sin" => "Math.Sin",
            "cos" => "Math.Cos",
            "tan" => "Math.Tan",
            "asin" => "Math.Asin",
            "acos" => "Math.Acos",
            "atan" => "Math.Atan",
            "atan2" => "Math.Atan2",
            "sinh" => "Math.Sinh",
            "cosh" => "Math.Cosh",
            "tanh" => "Math.Tanh",
            "exp" => "Math.Exp",
            "sqrt" => "Math.Sqrt",
            "cbrt" => "Math.Cbrt",
            "log" => "Math.Log",
            "ln" => "Math.Log",
            "log2" => "Math.Log2",
            "log10" => "Math.Log10",
            "pow" => "Math.Pow",
            "power" => "Math.Pow",
            "floor" => "Math.Floor",
            "ceil" => "Math.Ceiling",
            "ceiling" => "Math.Ceiling",
            "round" => "Math.Round",
            "truncate" => "Math.Truncate",
            "trunc" => "Math.Truncate",
            "abs" => "Math.Abs",
            "sign" => "Math.Sign",
            "min" => "Math.Min",
            "max" => "Math.Max",
            "clamp" => "Math.Clamp",
            _ => m.Value
        };

        // Rewrites if(condition, then, else) → (condition ? then : else).
        //
        // Uses a character-level parser to correctly handle:
        //   - Commas inside nested function calls: power(a, 2)
        //   - Nested if() expressions
        //   - Arbitrary whitespace
        //
        // Returns the rewritten expression, or the original with a warning
        // if parsing fails — allows the caller to surface the issue clearly.
        private static string RewriteIf(string expr)
        {
            var match = IfPattern.Match(expr);
            if (!match.Success) return expr;

            int start = match.Index;

            // Find the matching closing parenthesis by tracking depth
            int depth = 0;
            int argsStart = start + match.Length;
            int end = -1;

            for (int i = argsStart - 1; i < expr.Length; i++)
            {
                if (expr[i] == '(') depth++;
                else if (expr[i] == ')') { depth--; if (depth == 0) { end = i; break; } }
            }

            if (end < 0)
            {
                Console.WriteLine("  [PARSE WARNING] Malformed if() — missing closing paren");
                return expr;
            }

            string inner = expr.Substring(argsStart, end - argsStart);
            var parts = SplitTopLevel(inner);

            if (parts.Count != 3)
            {
                Console.WriteLine($"  [PARSE WARNING] if() expects 3 arguments, got {parts.Count}");
                return expr;
            }

            string condition = RewriteIf(parts[0].Trim());
            string thenExpr = RewriteIf(parts[1].Trim());
            string elseExpr = RewriteIf(parts[2].Trim());

            string rewritten = $"({condition} ? {thenExpr} : {elseExpr})";

            // Reconstruct: prefix + rewritten + suffix (handles if() not at root)
            return expr.Substring(0, start) + rewritten + RewriteIf(expr.Substring(end + 1));
        }

        // Splits a string by commas that are not inside parentheses.
        // Ensures "power(a, 2)" is treated as one argument, not two.
        private static List<string> SplitTopLevel(string s)
        {
            var parts = new List<string>();
            int depth = 0;
            int start = 0;

            for (int i = 0; i < s.Length; i++)
            {
                if (s[i] == '(') depth++;
                else if (s[i] == ')') depth--;
                else if (s[i] == ',' && depth == 0)
                {
                    parts.Add(s.Substring(start, i - start));
                    start = i + 1;
                }
            }

            parts.Add(s.Substring(start));
            return parts;
        }

        public static bool TryCompile(string expression,
            out Func<double, double, double, double, double> func)
        {
            func = null;

            if (_cache.TryGetValue(expression, out func))
                return true;

            try
            {
                // Step 1: Rewrite if(...) using character-level parser
                string expr = RewriteIf(expression);

                // Step 2: Map math function names to System.Math equivalents
                expr = FunctionRegex.Replace(expr, MapFunction);

                string code =
                    $"(double a, double b, double c, double d) => (double)({expr})";

                var options = ScriptOptions.Default
                .AddReferences(typeof(Math).Assembly)
                .AddImports("System", "System.Math");
                func = CSharpScript
                    .EvaluateAsync<Func<double, double, double, double, double>>(code, options)
                    .GetAwaiter().GetResult();

                _cache.TryAdd(expression, func);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [COMPILE ERROR] {ex.Message}");
                return false;
            }
        }
    }
}

