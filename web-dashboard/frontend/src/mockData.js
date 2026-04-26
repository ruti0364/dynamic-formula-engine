// ---------------------------------------------------------------------------
// mockData.js — Static demo data for Vercel deployment
//
// Real benchmark results from t_log (SQL Server):
//   CSharp : ~1.15s per formula  (total ~24.2s across 21 formulas)
//   SQL    : ~2.93s per formula  (total ~61.5s across 21 formulas)
//   Python : ~102.96s per formula (total ~2162s across 21 formulas)
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

// Exact values from t_log
const RAW_LOG = {
  CSharp: [1.274607, 1.205127, 1.144975, 1.163428, 1.176565, 1.145335, 1.160210, 1.109617, 1.159778, 1.173173, 1.196667, 1.167618, 1.120010, 1.128342, 1.111715, 1.149291, 1.164274, 1.120871, 1.109492, 1.089101, 1.132302],
  Python: [102.958995, 102.948070, 102.951004, 102.947981, 102.957990, 102.947992, 102.956004, 102.958360, 102.961985, 102.954995, 102.969605, 102.955000, 102.972994, 102.952027, 102.956619, 102.956537, 102.949987, 102.957012, 102.953075, 102.959991, 102.962992],
  SQL:    [2.950680, 2.820611, 3.021794, 3.033991, 2.909627, 2.868447, 2.866809, 2.906837, 3.115662, 2.918163, 2.936601, 2.822888, 2.899564, 2.943652, 2.916796, 3.031377, 2.884113, 2.850778, 2.893121, 2.874028, 3.058583],
};

const METHODS = ['CSharp', 'SQL', 'Python'];

// ── /api/comparison ──────────────────────────────────────────────────────────
export const mockComparison = METHODS.flatMap(method =>
  FORMULAS.map((f, idx) => {
    const avg = RAW_LOG[method][idx];
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
  FORMULAS.map((f, idx) => ({
    log_id:    logId++,
    targil_id: f.id,
    targil:    f.expr,
    method,
    run_time:  RAW_LOG[method][idx],
  }))
);

// ── /api/results ─────────────────────────────────────────────────────────────
const DATA_ROWS = [
  { data_id: 1, val_a: 42.37, val_b: 18.91, val_c: 73.54, val_d: 55.12 },
  { data_id: 2, val_a: 7.83,  val_b: 61.44, val_c: 29.07, val_d: 88.65 },
];

function calcResult(expr, a, b, c, d) {
  try {
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
export const mockVerify = FORMULAS.map((f, idx) => {
  const times = METHODS.map(m => RAW_LOG[m][idx]);
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
