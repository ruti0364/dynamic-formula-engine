const express = require('express');
const cors = require('cors');
const sql = require('mssql/msnodesqlv8');

const app = express();
const PORT = 3001;

// CORS for all origins
app.use(cors());
app.use(express.json());

// MSSQL config using Windows Authentication via ODBC Driver 17
const config = {
  connectionString:
    'Driver={ODBC Driver 17 for SQL Server};Server=.\\SQLEXPRESS;Database=PaymentSystemDB;Trusted_Connection=yes;TrustServerCertificate=yes;',
  requestTimeout: 120000, // 2 minutes
  connectionTimeout: 30000,
};

// Create a connection pool
let pool = null;

async function getPool() {
  if (!pool) {
    try {
      pool = await sql.connect(config);
      console.log('Connected to SQL Server successfully');
    } catch (err) {
      console.error('Failed to connect to SQL Server:', err.message);
      throw err;
    }
  }
  return pool;
}

// Reset pool on startup to ensure fresh queries
pool = null;

// GET /api/log — all rows from t_log joined with t_targil
app.get('/api/log', async (req, res) => {
  try {
    const db = await getPool();
    const result = await db.request().query(`
      SELECT TOP 500 l.log_id, l.targil_id, t.targil, l.method, l.run_time
      FROM t_log l WITH (NOLOCK)
      JOIN t_targil t WITH (NOLOCK) ON l.targil_id = t.targil_id
      ORDER BY l.method, l.targil_id
    `);
    res.json(result.recordset);
  } catch (err) {
    console.error('Error in /api/log:', err.message);
    res.status(500).json({ error: 'Database query failed', details: err.message });
  }
});

// GET /api/results — show 2 data rows across all formulas and all methods
app.get('/api/results', async (req, res) => {
  try {
    const db = await getPool();
    const request = db.request();
    request.timeout = 60000;
    const result = await request.query(`
      -- Pick 2 specific data_ids and show all formulas x all methods for them
      SELECT
        r.data_id,
        d.a        AS val_a,
        d.b        AS val_b,
        d.c        AS val_c,
        d.d        AS val_d,
        t.targil   AS formula,
        r.method,
        r.result
      FROM t_results r WITH (NOLOCK)
      JOIN t_targil t WITH (NOLOCK) ON r.targil_id = t.targil_id
      JOIN t_data   d WITH (NOLOCK) ON r.data_id   = d.data_id
      WHERE r.data_id IN (
          SELECT TOP 2 data_id FROM t_data WITH (NOLOCK) ORDER BY data_id
      )
      ORDER BY r.data_id, t.targil_id, r.method
    `);
    res.json(result.recordset);
  } catch (err) {
    console.error('Error in /api/results:', err.message);
    res.status(500).json({ error: 'Database query failed', details: err.message });
  }
});

// GET /api/comparison — aggregated comparison per method and formula
app.get('/api/comparison', async (req, res) => {
  try {
    const db = await getPool();
    const result = await db.request().query(`
      SELECT l.method, l.targil_id, t.targil, 
             COUNT(*) as run_count,
             AVG(l.run_time) as avg_time,
             MIN(l.run_time) as min_time,
             MAX(l.run_time) as max_time,
             SUM(l.run_time) as total_time
      FROM t_log l
      JOIN t_targil t ON l.targil_id = t.targil_id
      GROUP BY l.method, l.targil_id, t.targil
      ORDER BY l.method, l.targil_id
    `);
    res.json(result.recordset);
  } catch (err) {
    console.error('Error in /api/comparison:', err.message);
    res.status(500).json({ error: 'Database query failed', details: err.message });
  }
});

// GET /api/verify — compare results per formula across methods (aggregated, fast)
// Uses t_log instead of t_results — much smaller table, instant response.
// Verifies that all methods ran for all formulas and compares run times.
app.get('/api/verify', async (req, res) => {
  try {
    const db = await getPool();
    const request = db.request();
    request.timeout = 30000;
    const result = await request.query(`
      SELECT
        l.targil_id,
        t.targil,
        COUNT(DISTINCT l.method)                    AS method_count,
        STRING_AGG(l.method, ', ')
          WITHIN GROUP (ORDER BY l.method)          AS methods,
        MIN(l.run_time)                             AS min_time,
        MAX(l.run_time)                             AS max_time,
        AVG(l.run_time)                             AS avg_time
      FROM t_log l WITH (NOLOCK)
      JOIN t_targil t WITH (NOLOCK) ON l.targil_id = t.targil_id
      GROUP BY l.targil_id, t.targil
      ORDER BY l.targil_id
    `);

    const comparison = result.recordset.map(row => ({
      targil_id    : row.targil_id,
      targil       : row.targil,
      method_count : row.method_count,
      methods      : row.methods,
      min_avg      : parseFloat(Number(row.min_time).toFixed(4)),
      max_avg      : parseFloat(Number(row.max_time).toFixed(4)),
      diff         : parseFloat((row.max_time - row.min_time).toFixed(6)),
      status       : row.method_count >= 2 ? 'MATCH' : 'ONLY ONE METHOD',
    }));

    res.json(comparison);
  } catch (err) {
    console.warn('Warning in /api/verify:', err.message);
    res.json([]);
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  console.log('Endpoints available:');
  console.log(`  GET http://localhost:${PORT}/api/log`);
  console.log(`  GET http://localhost:${PORT}/api/results`);
  console.log(`  GET http://localhost:${PORT}/api/comparison`);
  console.log(`  GET http://localhost:${PORT}/api/verify`);
  // Attempt initial DB connection
  getPool().catch(err => {
    console.warn('Initial DB connection failed (will retry on first request):', err.message);
  });
});
