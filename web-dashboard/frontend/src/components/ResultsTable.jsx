import React, { useState, useMemo } from 'react';

function ResultsTable({ data }) {
  const [methodFilter, setMethodFilter] = useState('');
  const [targilFilter, setTargilFilter] = useState('');

  const methods  = useMemo(() => [...new Set(data.map(d => d.method))].sort(), [data]);
  const formulas = useMemo(() => [...new Set(data.map(d => d.formula))].sort(), [data]);

  const filtered = useMemo(() => {
    return data.filter(d => {
      const matchMethod = !methodFilter || d.method === methodFilter;
      const matchTargil = !targilFilter || d.formula === targilFilter;
      return matchMethod && matchTargil;
    });
  }, [data, methodFilter, targilFilter]);

  if (!data || data.length === 0) {
    return (
      <div className="card">
        <div className="card-title">תוצאות חישוב</div>
        <div className="empty-state">אין נתונים להצגה</div>
      </div>
    );
  }

  const fmt = v => (v != null ? Number(v).toFixed(4) : '—');

  return (
    <div className="card">
      <div className="card-title">תוצאות חישוב</div>

      <div className="note-banner">
        מוצגות 2 שורות נתונים עם כל הנוסחאות וכל השיטות — לצורך השוואה ואימות.
      </div>

      <div className="filters">
        <div className="filter-group">
          <label htmlFor="method-filter-results">סינון לפי שיטה:</label>
          <select
            id="method-filter-results"
            value={methodFilter}
            onChange={e => setMethodFilter(e.target.value)}
          >
            <option value="">כל השיטות</option>
            {methods.map(m => <option key={m} value={m}>{m}</option>)}
          </select>
        </div>

        <div className="filter-group">
          <label htmlFor="targil-filter-results">סינון לפי נוסחה:</label>
          <select
            id="targil-filter-results"
            value={targilFilter}
            onChange={e => setTargilFilter(e.target.value)}
          >
            <option value="">כל הנוסחאות</option>
            {formulas.map(f => <option key={f} value={f}>{f}</option>)}
          </select>
        </div>

        <span className="pagination-info">מוצגות {filtered.length} רשומות</span>
      </div>

      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              <th style={{ maxWidth: '180px', width: '180px' }}>נוסחה</th>
              <th>a</th>
              <th>b</th>
              <th>c</th>
              <th>d</th>
              <th>שיטה</th>
              <th>תוצאה</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 ? (
              <tr>
                <td colSpan={7} style={{ textAlign: 'center', padding: '24px', color: '#888' }}>
                  לא נמצאו רשומות
                </td>
              </tr>
            ) : (
              filtered.map((row, idx) => (
                <tr key={idx}>
                  <td style={{ maxWidth: '180px', wordBreak: 'break-all', fontSize: '0.82rem' }}>{row.formula}</td>
                  <td>{fmt(row.val_a)}</td>
                  <td>{fmt(row.val_b)}</td>
                  <td>{fmt(row.val_c)}</td>
                  <td>{fmt(row.val_d)}</td>
                  <td>{row.method}</td>
                  <td><strong>{fmt(row.result)}</strong></td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default ResultsTable;
