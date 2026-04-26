import React, { useMemo, useState } from 'react';
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts';

const METHOD_COLORS = {
  CSharp:  '#2c4a8a',
  SQL:     '#2e7d32',
  Python:  '#e65100',
};
const DEFAULT_COLORS = ['#6a1b9a', '#00838f', '#c62828', '#f57f17'];

function getColor(method, idx) {
  return METHOD_COLORS[method] ?? DEFAULT_COLORS[idx % DEFAULT_COLORS.length];
}

// Recharts doesn't support log scale natively — we apply log10 transform manually
// and format the tick labels back to real values.
function logTick(value) {
  const real = Math.pow(10, value);
  if (real < 0.01) return real.toExponential(1);
  if (real < 1)    return real.toFixed(3);
  if (real < 10)   return real.toFixed(2);
  return real.toFixed(1);
}

function ComparisonChart({ data }) {
  const [viewMode, setViewMode] = useState('log'); // 'log' | 'split'

  const methods  = useMemo(() => [...new Set(data.map(d => d.method))], [data]);
  const formulas = useMemo(() => [...new Set(data.map(d => d.targil))], [data]);

  // Chart data with log10-transformed values for the log view
  const chartDataLog = useMemo(() => {
    return formulas.map(formula => {
      const entry = { formula };
      methods.forEach(method => {
        const row = data.find(d => d.targil === formula && d.method === method);
        const raw = row ? parseFloat(Number(row.avg_time).toFixed(4)) : 0;
        // log10(0) = -Infinity — clamp to a small positive floor
        entry[method]        = raw > 0 ? Math.log10(raw) : Math.log10(0.0001);
        entry[`${method}_raw`] = raw;
      });
      return entry;
    });
  }, [data, methods, formulas]);

  // Per-method chart data for the split view
  const splitData = useMemo(() => {
    return methods.map(method => ({
      method,
      color: getColor(method, methods.indexOf(method)),
      rows: formulas.map(formula => {
        const row = data.find(d => d.targil === formula && d.method === method);
        return {
          formula,
          time: row ? parseFloat(Number(row.avg_time).toFixed(4)) : 0,
        };
      }),
    }));
  }, [data, methods, formulas]);

  // Method totals for recommendation
  const methodTotals = useMemo(() => {
    return methods.map(method => ({
      method,
      total: data
        .filter(d => d.method === method)
        .reduce((sum, d) => sum + Number(d.total_time), 0),
    }));
  }, [data, methods]);

  const bestMethod = useMemo(() => {
    if (!methodTotals.length) return null;
    return methodTotals.reduce((best, cur) => (cur.total < best.total ? cur : best));
  }, [methodTotals]);

  if (!data || data.length === 0) {
    return (
      <div className="card">
        <div className="card-title">השוואת ביצועים</div>
        <div className="empty-state">אין נתונים להצגה</div>
      </div>
    );
  }

  const formatTime = val => (val != null ? Number(val).toFixed(4) : '—');

  // Custom tooltip for log chart — shows real value, not log-transformed
  const LogTooltip = ({ active, payload, label }) => {
    if (!active || !payload?.length) return null;
    return (
      <div style={{
        background: '#fff', border: '1px solid #ddd', borderRadius: 6,
        padding: '8px 12px', fontSize: '0.85rem', direction: 'rtl',
      }}>
        <p style={{ margin: '0 0 4px', fontWeight: 600 }}>{label}</p>
        {payload.map(p => (
          <p key={p.dataKey} style={{ margin: '2px 0', color: p.fill }}>
            {p.name}: {Number(p.payload[`${p.dataKey}_raw`]).toFixed(4)} שנ'
          </p>
        ))}
      </div>
    );
  };

  return (
    <div>
      {/* View toggle */}
      <div className="card" style={{ paddingBottom: 8 }}>
        <div className="card-title">גרף השוואת זמן ריצה</div>
        <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
          <button
            onClick={() => setViewMode('log')}
            style={{
              padding: '6px 16px', borderRadius: 6, border: 'none', cursor: 'pointer',
              background: viewMode === 'log' ? '#2c4a8a' : '#e8edf5',
              color: viewMode === 'log' ? '#fff' : '#333',
              fontWeight: viewMode === 'log' ? 600 : 400,
            }}
          >
            סקאלה לוגריתמית
          </button>
          <button
            onClick={() => setViewMode('split')}
            style={{
              padding: '6px 16px', borderRadius: 6, border: 'none', cursor: 'pointer',
              background: viewMode === 'split' ? '#2c4a8a' : '#e8edf5',
              color: viewMode === 'split' ? '#fff' : '#333',
              fontWeight: viewMode === 'split' ? 600 : 400,
            }}
          >
            גרפים נפרדים לכל שיטה
          </button>
        </div>

        {/* ── LOG SCALE VIEW ── */}
        {viewMode === 'log' && (
          <>
            <p style={{ fontSize: '0.82rem', color: '#666', marginBottom: 12, marginTop: 0 }}>
              ציר Y בסקאלה לוגריתמית (log₁₀) — מאפשר השוואה בין שיטות עם פערי זמן גדולים.
              ריחוף על נקודה מציג את הזמן האמיתי בשניות.
            </p>
            <div className="chart-container">
              <ResponsiveContainer width="100%" height={400}>
                <LineChart
                  data={chartDataLog}
                  margin={{ top: 10, right: 20, left: 20, bottom: 70 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#e0e5f0" />
                  <XAxis
                    dataKey="formula"
                    tick={{ fontSize: 11, fill: '#444' }}
                    angle={-35}
                    textAnchor="end"
                    interval={0}
                  />
                  <YAxis
                    tickFormatter={logTick}
                    tick={{ fontSize: 11, fill: '#444' }}
                    domain={['auto', 'auto']}
                    label={{
                      value: "זמן — log₁₀(שנ')",
                      angle: 90,
                      position: 'insideLeft',
                      offset: -5,
                      style: { fontSize: 11, fill: '#666' },
                    }}
                  />
                  {/* Reference lines for clean log scale markers */}
                  <ReferenceLine y={Math.log10(0.1)}  stroke="#ccc" strokeDasharray="4 2" label={{ value: '0.1s',  position: 'right', fontSize: 10, fill: '#999' }} />
                  <ReferenceLine y={Math.log10(1)}    stroke="#ccc" strokeDasharray="4 2" label={{ value: '1s',    position: 'right', fontSize: 10, fill: '#999' }} />
                  <ReferenceLine y={Math.log10(10)}   stroke="#ccc" strokeDasharray="4 2" label={{ value: '10s',   position: 'right', fontSize: 10, fill: '#999' }} />
                  <ReferenceLine y={Math.log10(100)}  stroke="#ccc" strokeDasharray="4 2" label={{ value: '100s',  position: 'right', fontSize: 10, fill: '#999' }} />
                  <Tooltip content={<LogTooltip />} />
                  <Legend wrapperStyle={{ paddingTop: 20, fontSize: '0.88rem' }} />
                  {methods.map((method, idx) => (
                    <Line
                      key={method}
                      type="monotone"
                      dataKey={method}
                      name={method}
                      stroke={getColor(method, idx)}
                      strokeWidth={method === bestMethod?.method ? 3 : 2}
                      dot={{ r: 4, fill: getColor(method, idx) }}
                      activeDot={{ r: 6 }}
                      opacity={method === bestMethod?.method ? 1 : 0.85}
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </div>
          </>
        )}

        {/* ── SPLIT VIEW ── */}
        {viewMode === 'split' && (
          <>
            <p style={{ fontSize: '0.82rem', color: '#666', marginBottom: 16, marginTop: 0 }}>
              כל שיטה בגרף נפרד עם ציר Y מותאם — מאפשר לראות את הפרופיל הפנימי של כל שיטה.
            </p>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
              {splitData.map(({ method, color, rows }) => {
                const total = methodTotals.find(m => m.method === method)?.total ?? 0;
                const isBest = method === bestMethod?.method;
                return (
                  <div key={method} style={{
                    border: `2px solid ${isBest ? color : '#e0e5f0'}`,
                    borderRadius: 10,
                    padding: '16px 16px 8px',
                    background: isBest ? '#f7f9ff' : '#fff',
                  }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
                      <span style={{
                        fontWeight: 700, fontSize: '1rem', color,
                      }}>
                        {method}
                      </span>
                      <span style={{ fontSize: '0.82rem', color: '#666' }}>
                        זמן כולל: <strong>{total.toFixed(3)} שנ'</strong>
                      </span>
                      {isBest && (
                        <span style={{
                          background: color, color: '#fff',
                          borderRadius: 12, padding: '2px 10px',
                          fontSize: '0.75rem', fontWeight: 600,
                        }}>
                          ⚡ מהיר ביותר
                        </span>
                      )}
                    </div>
                    <ResponsiveContainer width="100%" height={220}>
                      <BarChart
                        data={rows}
                        margin={{ top: 4, right: 16, left: 10, bottom: 60 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" stroke="#e8ecf4" />
                        <XAxis
                          dataKey="formula"
                          tick={{ fontSize: 10, fill: '#555' }}
                          angle={-35}
                          textAnchor="end"
                          interval={0}
                        />
                        <YAxis
                          tick={{ fontSize: 10, fill: '#555' }}
                          tickFormatter={v => v.toFixed(2)}
                          label={{
                            value: "שנ'",
                            angle: 90,
                            position: 'insideLeft',
                            style: { fontSize: 10, fill: '#888' },
                          }}
                        />
                        <Tooltip
                          formatter={v => [`${Number(v).toFixed(4)} שנ'`, 'זמן ריצה']}
                          contentStyle={{ fontSize: '0.82rem', direction: 'rtl' }}
                        />
                        <Bar dataKey="time" fill={color} radius={[3, 3, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                );
              })}
            </div>
          </>
        )}
      </div>

      {/* Summary Table */}
      <div className="card">
        <div className="card-title">טבלת סיכום ביצועים</div>
        <div className="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>שיטה</th>
                <th>נוסחה</th>
                <th>מס' ריצות</th>
                <th>זמן (שנ')</th>
                <th>זמן מינימלי</th>
                <th>זמן מקסימלי</th>
                <th>זמן כולל</th>
              </tr>
            </thead>
            <tbody>
              {data.map((row, idx) => {
                const isBest = row.method === bestMethod?.method;
                return (
                  <tr key={idx} className={isBest ? 'row-best' : ''}>
                    <td>
                      {row.method}
                      {isBest && (
                        <span className="badge badge-best" style={{ marginRight: 8 }}>
                          מהיר ביותר
                        </span>
                      )}
                    </td>
                    <td>{row.targil}</td>
                    <td>{row.run_count}</td>
                    <td>{formatTime(row.avg_time)}</td>
                    <td>{formatTime(row.min_time)}</td>
                    <td>{formatTime(row.max_time)}</td>
                    <td>{formatTime(row.total_time)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {bestMethod && (
          <div className="recommendation-box">
            <h3>המלצה</h3>
            <p>
              השיטה המהירה ביותר היא <strong>{bestMethod.method}</strong> עם זמן כולל של{' '}
              <strong>{formatTime(bestMethod.total)}</strong> שניות.
              מומלץ להשתמש בשיטה זו לביצועים מיטביים.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

export default ComparisonChart;
