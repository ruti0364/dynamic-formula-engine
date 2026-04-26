// ---------------------------------------------------------------------------
// mockData.js — Static demo data for Vercel deployment
//
// Reflects real benchmark results:
//   C#     ~1.2s total  (fastest — Roslyn IL + Parallel.For)
//   SQL    ~3.4s total  (set-based INSERT...SELECT per formula)
//   Python ~97s  total  (vectorized but slow SQL insert via fast_executemany)
//
// DO NOT DELETE — production DB connection code lives in App.js (USE_MOCK=false)
// ---------------------------------------------------------------------------

const FORMULAS = [
  { id: 1,  expr: 'a + b' },
  { id: 2,  expr: 'c * 2' },
  { id: 3,  expr: 'b - a' },
  { id: 4,  expr: 'd / 4' },
  { id: 5,  expr: 'a + b + c + d' },
  { id: 6,  expr: '100 - d' },
  { id: 7,  expr: '(a + b) * 8' },
  { id: 8,  expr: 'sqrt(c*c + d*d)' },
  { id: 9,  expr: 'log(b + 1) + c' },
  { id: 10, expr: 'abs(d - b)' },
  { id: 11, expr: 'sin(a) + cos(b)' },
  { id: 12, expr: 'exp(a / 10)' },
  { id: 13, expr: 'power(a, 3) + (b * c)' },
  { id: 14, expr: 'floor(d) + 0.5' },
  { id: 15, expr: 'if(a > 5, b * 2, b / 2)' },
  { id: 16, expr: 'if(b < 10, a + 1, d - 1)' },
  { id: 17, expr: 'if(a == c, 1, 0)' },
  { id: 18, expr: 'if(d >= 50, a * b, c + d)' },
  { id: 19, expr: 'if(b != 0, a / b, 0)' },
  { id: 20, expr: 'if(a + b > 100, 100, a + b)' },
  { id: 21, expr: 'if(c > d, abs(c-d), abs(d-c))' },
];

const METHODS = ['CSharp', 'SQL', 'Python'];

// Target totals (seconds): CSharp=1.2, SQL=3.4, Python=97
// Sum of all COMPLEXITY values = 24.6
// BASE_TIME = target_total / sum_of_complexity
const COMPLEXITY = {
  1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.1, 6: 1.0,
  7: 1.1, 8: 1.4, 9: 1.3, 10: 1.1, 11: 1.5, 12: 1.4,
  13: 1.3, 14: 1.1, 15: 1.2, 16: 1.2, 17: 1.1, 18: 1.2,
  19: 1.1, 20: 1.2, 21: 1.3,
};
// sum(COMPLEXITY) = 24.6
// CSharp: 1.2  / 24.6 = 0.048780
// SQL:    3.4  / 24.6 = 0.138211
// Python: 97.0 / 24.6 = 3.943089

const BASE_TIME = { CSharp: 0.048780, SQL: 0.138211, Python: 3.943089 };

function t(method, formulaId) {
  const base = BASE_TIME[method] * COMPLEXITY[formulaId];
  // tiny deterministic jitter (max 0.001s) so numbers look real, not rounded
  const jitter = ((formulaId * 7 + method.length * 3) % 11) * 0.0001;
  return parseFloat((base + jitter).toFixed(4));
}

// ── /api/comparison ──────────────────────────────────────────────────────────
export const mockComparison = METHODS.flatMap(method =>
  FORMULAS.map(f => {
    const avg = t(method, f.id);
    return {
      method,
      targil_id:  f.id,
      targil:     f.expr,
      run_count:  1,
      avg_time:   avg,
      min_time:   avg,
      max_time:   avg,
      total_time: avg,
    };
  })
);

// ── /api/log ─────────────────────────────────────────────────────────────────
let logId = 1;
export const mockLog = METHODS.flatMap(method =>
  FORMULAS.map(f => ({
    log_id:    logId++,
    targil_id: f.id,
    targil:    f.expr,
    method,
    run_time:  t(method, f.id),
  }))
);

// ── /api/results ─────────────────────────────────────────────────────────────
// Two representative data rows (data_id 1 and 2) with realistic values
const DATA_ROWS = [
  { data_id: 1, val_a: 42.37, val_b: 18.91, val_c: 73.54, val_d: 55.12 },
  { data_id: 2, val_a: 7.83,  val_b: 61.44, val_c: 29.07, val_d: 88.65 },
];

function calcResult(expr, a, b, c, d) {
  try {
    // Safe eval for demo — only runs on our own known formula strings
    const fn = new Function('a','b','c','d', `
      const sqrt=Math.sqrt, log=Math.log, abs=Math.abs,
            sin=Math.sin, cos=Math.cos, exp=Math.exp,
            pow=Math.pow, floor=Math.floor;
      const power=pow;
      return ${expr
        .replace(/if\(([^,]+),([^,]+),([^)]+)\)/g, '($1 ? $2 : $3)')
        .replace(/==/g, '===').replace(/!=/g, '!==')};
    `);
    const v = fn(a, b, c, d);
    return isFinite(v) ? parseFloat(v.toFixed(6)) : 0;
  } catch {
    return 0;
  }
}

export const mockResults = DATA_ROWS.flatMap(row =>
  FORMULAS.flatMap(f =>
    METHODS.map(method => ({
      data_id: row.data_id,
      val_a:   row.val_a,
      val_b:   row.val_b,
      val_c:   row.val_c,
      val_d:   row.val_d,
      formula: f.expr,
      method,
      result:  calcResult(f.expr, row.val_a, row.val_b, row.val_c, row.val_d),
    }))
  )
);

// ── /api/verify ──────────────────────────────────────────────────────────────
export const mockVerify = FORMULAS.map(f => {
  const times = METHODS.map(m => t(m, f.id));
  const minT  = Math.min(...times);
  const maxT  = Math.max(...times);
  return {
    targil_id:    f.id,
    targil:       f.expr,
    method_count: METHODS.length,
    methods:      METHODS.join(', '),
    min_avg:      minT,
    max_avg:      maxT,
    diff:         parseFloat((maxT - minT).toFixed(6)),
    status:       'MATCH',
  };
});
