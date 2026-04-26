import React, { useMemo } from 'react';

function Summary({ verifyData }) {
  const stats = useMemo(() => {
    const total   = verifyData.length;
    const matched = verifyData.filter(d => d.method_count >= 2).length;
    return { total, matched, missing: total - matched };
  }, [verifyData]);

  if (!verifyData || verifyData.length === 0) {
    return (
      <div className="card">
        <div className="card-title">אימות תוצאות</div>
        <div className="empty-state">אין נתונים להצגה</div>
      </div>
    );
  }

  return (
    <div>
      {/* Stats */}
      <div className="stats-grid">
        <div className="stat-card stat-total">
          <div className="stat-value">{stats.total}</div>
          <div className="stat-label">סה"כ נוסחאות</div>
        </div>
        <div className="stat-card stat-match">
          <div className="stat-value">{stats.matched}</div>
          <div className="stat-label">ביצועים שווים</div>
        </div>
        <div className="stat-card stat-mismatch">
          <div className="stat-value">{stats.missing}</div>
          <div className="stat-label">ביצועים שונים</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {stats.total > 0
              ? `${((stats.matched / stats.total) * 100).toFixed(0)}%`
              : '—'}
          </div>
          <div className="stat-label">התאמה</div>
        </div>
      </div>

      {/* Detail Table */}
      <div className="card">
        <div className="card-title">פירוט — זמני ריצה לפי נוסחה ושיטה</div>
        <div className="table-wrapper">
          <table>
            <thead>
              <tr>
                <th>נוסחה</th>
                <th>שיטות שרצו</th>
                <th>מס' שיטות</th>
                <th>זמן מינימלי (שנ')</th>
                <th>זמן מקסימלי (שנ')</th>
                <th>הפרש</th>
                <th>סטטוס</th>
              </tr>
            </thead>
            <tbody>
              {verifyData.map((row, idx) => {
                const isOk = row.method_count >= 2;
                return (
                  <tr key={idx} className={isOk ? 'row-match' : 'row-mismatch'}>
                    <td>{row.targil}</td>
                    <td style={{ fontSize: '0.82rem', color: '#555' }}>{row.methods}</td>
                    <td>{row.method_count}</td>
                    <td>{row.min_avg != null ? Number(row.min_avg).toFixed(4) : '—'}</td>
                    <td>{row.max_avg != null ? Number(row.max_avg).toFixed(4) : '—'}</td>
                    <td>{row.diff != null ? Number(row.diff).toFixed(6) : '—'}</td>
                    <td>
                      <span className={`badge ${isOk ? 'badge-match' : 'badge-mismatch'}`}>
                        {isOk ? '✓ תקין' : '✗ שגוי'}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

export default Summary;
