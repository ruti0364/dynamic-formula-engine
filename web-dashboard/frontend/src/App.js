import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';
import ComparisonChart from './components/ComparisonChart';
import LogTable from './components/LogTable';
import ResultsTable from './components/ResultsTable';
import Summary from './components/Summary';
import { mockComparison, mockLog, mockResults, mockVerify } from './mockData';

// ---------------------------------------------------------------------------
// USE_MOCK — controls data source:
//   true  → static mockData.js  (Vercel / demo deployment, no backend needed)
//   false → live Node.js API    (local dev with SQL Server)
//
// Set via .env:  REACT_APP_USE_MOCK=true
// Default: false (production DB mode)
// ---------------------------------------------------------------------------
const USE_MOCK = process.env.REACT_APP_USE_MOCK === 'true';

const API_BASE = 'http://localhost:3001/api';

const TABS = [
  { id: 'comparison', label: 'השוואת ביצועים' },
  { id: 'log',        label: 'לוג ריצות' },
  { id: 'results',    label: 'תוצאות חישוב' },
  { id: 'verify',     label: 'אימות תוצאות' },
];

function App() {
  const [activeTab, setActiveTab] = useState('comparison');
  const [logData, setLogData] = useState([]);
  const [resultsData, setResultsData] = useState([]);
  const [comparisonData, setComparisonData] = useState([]);
  const [verifyData, setVerifyData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [errors, setErrors] = useState({});

  useEffect(() => {
    const fetchAll = async () => {
      setLoading(true);
      const newErrors = {};

      // ── MOCK MODE (Vercel / demo) ──────────────────────────────────────────
      if (USE_MOCK) {
        setComparisonData(mockComparison);
        setLogData(mockLog);
        setResultsData(mockResults);
        setVerifyData(mockVerify);
        setLoading(false);
        return;
      }

      // ── LIVE MODE (local dev with SQL Server) ─────────────────────────────
      const endpoints = [
        { key: 'log',        url: `${API_BASE}/log`,        setter: setLogData },
        { key: 'results',    url: `${API_BASE}/results`,    setter: setResultsData },
        { key: 'comparison', url: `${API_BASE}/comparison`, setter: setComparisonData },
        { key: 'verify',     url: `${API_BASE}/verify`,     setter: setVerifyData, silent: true },
      ];

      await Promise.all(
        endpoints.map(async ({ key, url, setter, silent }) => {
          try {
            const res = await axios.get(url);
            setter(res.data);
          } catch (err) {
            console.error(`Failed to fetch ${key}:`, err.message);
            if (!silent) {
              newErrors[key] = err.response?.data?.details || err.message;
            }
          }
        })
      );

      setErrors(newErrors);
      setLoading(false);
    };

    fetchAll();
  }, []);

  const hasError = Object.keys(errors).length > 0;

  return (
    <div className="app" dir="rtl">
      <header className="app-header">
        <h1>מערכת תשלומים - דוח ביצועים</h1>
        <p className="header-subtitle">ניתוח ביצועים והשוואת שיטות חישוב</p>
      </header>

      {hasError && (
        <div className="error-banner">
          <strong>שגיאה בטעינת נתונים:</strong>
          <ul>
            {Object.entries(errors).map(([key, msg]) => (
              <li key={key}>
                <strong>{key}:</strong> {msg}
              </li>
            ))}
          </ul>
        </div>
      )}

      <nav className="tab-nav">
        {TABS.map(tab => (
          <button
            key={tab.id}
            className={`tab-btn${activeTab === tab.id ? ' active' : ''}`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      <main className="app-main">
        {loading ? (
          <div className="loading-container">
            <div className="spinner" />
            <p>טוען נתונים מהשרת...</p>
          </div>
        ) : (
          <>
            {activeTab === 'comparison' && <ComparisonChart data={comparisonData} />}
            {activeTab === 'log'        && <LogTable data={logData} />}
            {activeTab === 'results'    && <ResultsTable data={resultsData} />}
            {activeTab === 'verify'     && <Summary verifyData={verifyData} />}
          </>
        )}
      </main>

      <footer className="app-footer">
        <p>מערכת תשלומים © {new Date().getFullYear()} | דוח ביצועים</p>
      </footer>
    </div>
  );
}

export default App;
