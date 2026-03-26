import { useState, useEffect, useRef } from "react";

const API = "/api";

function StatCard({ label, value, color }) {
  return (
    <div style={{
      background: "#1a1a2e", border: `1px solid ${color}`,
      borderRadius: 8, padding: "16px 24px", minWidth: 160
    }}>
      <div style={{ color: "#aaa", fontSize: 12 }}>{label}</div>
      <div style={{ color, fontSize: 28, fontWeight: "bold" }}>{value ?? "—"}</div>
    </div>
  );
}

function FraudTable({ scores }) {
  return (
    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
      <thead>
        <tr style={{ background: "#0f3460", color: "#eee" }}>
          {["Transaction ID", "User", "Score", "Reasons", "Flagged At"].map(h => (
            <th key={h} style={{ padding: "8px 12px", textAlign: "left" }}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {scores.map((s, i) => (
          <tr key={s.transaction_id} style={{ background: i % 2 ? "#16213e" : "#0f3460" }}>
            <td style={{ padding: "6px 12px", fontFamily: "monospace", fontSize: 11 }}>
              {s.transaction_id?.slice(0, 8)}…
            </td>
            <td style={{ padding: "6px 12px" }}>{s.user_id}</td>
            <td style={{ padding: "6px 12px", color: s.fraud_score >= 60 ? "#ff4444" : "#ffaa00",
                         fontWeight: "bold" }}>
              {s.fraud_score}
            </td>
            <td style={{ padding: "6px 12px", color: "#aaa" }}>{s.reasons}</td>
            <td style={{ padding: "6px 12px", color: "#888", fontSize: 11 }}>
              {s.flagged_at}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default function App() {
  const [stats,  setStats]  = useState(null);
  const [scores, setScores] = useState([]);
  const [error,  setError]  = useState(null);
  const backoffRef  = useRef(0);
  const errorCount  = useRef(0);

  const fetchData = async () => {
    if (backoffRef.current > 0) { backoffRef.current--; return; }
    try {
      const [s, f] = await Promise.all([
        fetch(`${API}/stats`).then(r => r.json()),
        fetch(`${API}/fraud-scores?limit=20`).then(r => r.json()),
      ]);
      errorCount.current = 0;
      backoffRef.current = 0;
      setError(null);
      setStats(s);
      setScores(f.fraud_scores || []);
    } catch (e) {
      errorCount.current++;
      backoffRef.current = Math.min(Math.pow(2, errorCount.current - 1), 32);
      setError(e.message);
    }
  };

  useEffect(() => { fetchData(); const id = setInterval(fetchData, 5000); return () => clearInterval(id); }, []);

  return (
    <div style={{ background: "#0a0a1a", minHeight: "100vh", color: "#eee",
                  fontFamily: "system-ui, sans-serif", padding: 24 }}>
      <h1 style={{ color: "#e94560", marginBottom: 8 }}>
        Real-Time Fraud Detection
      </h1>
      <p style={{ color: "#666", marginBottom: 24 }}>
        Redpanda → Spark → StarRocks | Live updates every 5s
      </p>

      {error && <div style={{ color: "#ff4444", marginBottom: 16 }}>Error: {error}</div>}

      <div style={{ display: "flex", gap: 16, marginBottom: 32, flexWrap: "wrap" }}>
        <StatCard label="Total Txns (1h)"    value={stats?.total_transactions} color="#00b4d8" />
        <StatCard label="Flagged (1h)"       value={stats?.flagged_count}      color="#ff4444" />
        <StatCard label="Avg Fraud Score"    value={stats?.avg_fraud_score?.toFixed(1)} color="#ffaa00" />
        <StatCard label="Last Ingest"        value={stats?.last_ingest?.slice(11,19)} color="#06d6a0" />
      </div>

      <h2 style={{ color: "#e94560", marginBottom: 12 }}>Recent Fraud Alerts</h2>
      {scores.length === 0
        ? <p style={{ color: "#666" }}>No fraud alerts yet. Send some transactions!</p>
        : <FraudTable scores={scores} />
      }
    </div>
  );
}
