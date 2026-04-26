import React, { useState, useMemo } from 'react';

const PAGE_SIZE = 50;

function LogTable({ data }) {
  const [methodFilter, setMethodFilter] = useState('');
  const [formulaFilter, setFormulaFilter] = useState('');
  const [page, setPage] = useState(1);

  const methods  = useMemo(() => [...new Set(data.map(d => d.method))].sort(), [data]);
  const formulas = useMemo(() => [...new Set(data.map(d => d.targil))].sort(), [data]);

  const filtered = useMemo(() => {
    return data.filter(d => {
      const matchMethod  = !methodFilter  || d.method  === methodFilter;
      const matchFormula = !formulaFilter || d.targil  === formulaFilter;
      return matchMethod && matchFormula;
    });
  }, [data, methodFilter, formulaFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const paginated = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  const handleMethodChange = e => {
    setMethodFilter(e.target.value);
    setPage(1);
  };

  const handleFormulaChange = e => {
    setFormulaFilter(e.target.value);
    setPage(1);
  };

  // Build page buttons (show max 7 around current)
  const pageButtons = useMemo(() => {
    const pages = [];
    const delta = 3;
    const start = Math.max(1, page - delta);
    const end = Math.min(totalPages, page + delta);
    for (let i = start; i <= end; i++) pages.push(i);
    return pages;
  }, [page, totalPages]);

  if (!data || data.length === 0) {
    return (
      <div className="card">
        <div className="card-title">לוג ריצות</div>
        <div className="empty-state">אין נתונים להצגה</div>
      </div>
    );
  }

  return (
    <div className="card">
      <div className="card-title">לוג ריצות</div>

      <div className="filters">
        <div className="filter-group">
          <label htmlFor="method-filter-log">סינון לפי שיטה:</label>
          <select
            id="method-filter-log"
            value={methodFilter}
            onChange={handleMethodChange}
          >
            <option value="">כל השיטות</option>
            {methods.map(m => (
              <option key={m} value={m}>{m}</option>
            ))}
          </select>
        </div>
        <div className="filter-group">
          <label htmlFor="formula-filter-log">סינון לפי נוסחה:</label>
          <select
            id="formula-filter-log"
            value={formulaFilter}
            onChange={handleFormulaChange}
          >
            <option value="">כל הנוסחאות</option>
            {formulas.map(f => (
              <option key={f} value={f}>{f}</option>
            ))}
          </select>
        </div>
        <span className="pagination-info">
          סה"כ {filtered.length} רשומות
        </span>
      </div>

      <div className="table-wrapper" style={{ maxWidth: '600px' }}>
        <table style={{ tableLayout: 'fixed', width: '100%' }}>
          <thead>
            <tr>
              <th style={{ width: '45%' }}>נוסחה</th>
              <th style={{ width: '30%' }}>שיטה</th>
              <th style={{ width: '25%' }}>זמן ריצה (שנ')</th>
            </tr>
          </thead>
          <tbody>
            {paginated.length === 0 ? (
              <tr>
                <td colSpan={3} style={{ textAlign: 'center', padding: '24px', color: '#888' }}>
                  לא נמצאו רשומות
                </td>
              </tr>
            ) : (
              paginated.map((row, idx) => (
                <tr key={row.log_id ?? idx}>
                  <td>{row.targil}</td>
                  <td>{row.method}</td>
                  <td>{row.run_time != null ? Number(row.run_time).toFixed(4) : '—'}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div className="pagination">
          <button onClick={() => setPage(1)} disabled={page === 1}>«</button>
          <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1}>‹</button>

          {pageButtons[0] > 1 && <span className="pagination-info">...</span>}
          {pageButtons.map(p => (
            <button
              key={p}
              onClick={() => setPage(p)}
              className={p === page ? 'active-page' : ''}
            >
              {p}
            </button>
          ))}
          {pageButtons[pageButtons.length - 1] < totalPages && (
            <span className="pagination-info">...</span>
          )}

          <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page === totalPages}>›</button>
          <button onClick={() => setPage(totalPages)} disabled={page === totalPages}>»</button>

          <span className="pagination-info">
            עמוד {page} מתוך {totalPages}
          </span>
        </div>
      )}
    </div>
  );
}

export default LogTable;
