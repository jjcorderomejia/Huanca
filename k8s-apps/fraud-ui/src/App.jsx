import { useState, useEffect, useRef } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine
} from "recharts";

const API = "/api";

const SPINNER = (
  <div style={{
    width: 40, height: 40, margin: "80px auto",
    border: "4px solid #e8e8e8",
    borderTop: "4px solid #e94560",
    borderRadius: "50%",
    animation: "spin 0.8s linear infinite"
  }} />
);

function StatCard({ label, value, color }) {
  return (
    <div style={{
      background: "#fff", border: "1px solid #e8e8e8",
      borderTop: `3px solid ${color}`,
      borderRadius: 8, padding: "16px 24px", minWidth: 160, flex: "1 1 160px",
      boxShadow: "0 1px 4px rgba(0,0,0,0.06)"
    }}>
      <div style={{ color: "#999", fontSize: 11, textTransform: "uppercase", letterSpacing: 1 }}>{label}</div>
      <div style={{ color: "#1a1a2e", fontSize: 32, fontWeight: "bold", marginTop: 4 }}>{value ?? "—"}</div>
    </div>
  );
}

function ScoreBadge({ score }) {
  const color = score >= 80 ? "#e53e3e" : score >= 60 ? "#dd6b20" : "#d69e2e";
  const bg    = score >= 80 ? "#fff5f5" : score >= 60 ? "#fffaf0" : "#fffff0";
  return (
    <span style={{
      background: bg, color, border: `1px solid ${color}`,
      borderRadius: 4, padding: "2px 8px", fontWeight: "bold", fontSize: 12
    }}>
      {score}
    </span>
  );
}

function FraudTable({ scores }) {
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ background: "#f7f8fa", color: "#999", fontSize: 11, textTransform: "uppercase", letterSpacing: 1 }}>
            {["Transaction ID", "User", "Score", "Reasons", "Flagged At"].map(h => (
              <th key={h} style={{ padding: "10px 14px", textAlign: "left", fontWeight: 600, borderBottom: "1px solid #e8e8e8" }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {scores.map((s, i) => (
            <tr key={s.transaction_id}
              style={{ background: i % 2 ? "#fafafa" : "#fff", borderBottom: "1px solid #f0f0f0" }}>
              <td style={{ padding: "8px 14px", fontFamily: "monospace", fontSize: 11, color: "#aaa" }}>
                {s.transaction_id?.slice(0, 8)}…
              </td>
              <td style={{ padding: "8px 14px", color: "#1a1a2e", fontWeight: 500 }}>{s.user_id}</td>
              <td style={{ padding: "8px 14px" }}><ScoreBadge score={s.fraud_score} /></td>
              <td style={{ padding: "8px 14px", color: "#666", fontSize: 12 }}>{s.reasons}</td>
              <td style={{ padding: "8px 14px", color: "#aaa", fontSize: 11 }}>
                {s.flagged_at?.slice(0, 19).replace("T", " ")}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <div style={{ color: "#bbb", fontSize: 11, padding: "6px 14px" }}>
        Showing {scores.length} most recent alerts
      </div>
    </div>
  );
}

function ScoreChart({ scores }) {
  const data = [...scores].reverse().map(s => ({
    time: s.flagged_at?.slice(11, 19),
    score: s.fraud_score,
    user: s.user_id,
  }));
  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
        <XAxis dataKey="time" tick={{ fill: "#bbb", fontSize: 10 }} />
        <YAxis domain={[0, 100]} tick={{ fill: "#bbb", fontSize: 10 }} />
        <Tooltip
          contentStyle={{ background: "#fff", border: "1px solid #e8e8e8", borderRadius: 6, boxShadow: "0 2px 8px rgba(0,0,0,0.1)" }}
          labelStyle={{ color: "#666" }}
          itemStyle={{ color: "#e94560" }}
          formatter={(v, _, p) => [`${v} pts`, p.payload.user]}
        />
        <ReferenceLine y={60} stroke="#e53e3e" strokeDasharray="4 2" label={{ value: "Threshold", fill: "#e53e3e", fontSize: 10 }} />
        <Bar dataKey="score" fill="#e94560" radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}

export default function App() {
  const [stats,      setStats]      = useState(null);
  const [scores,     setScores]     = useState([]);
  const [topUsers,   setTopUsers]   = useState([]);
  const [loading,    setLoading]    = useState(true);
  const [error,      setError]      = useState(null);
  const backoffRef  = useRef(0);
  const errorCount  = useRef(0);

  const fetchData = async () => {
    if (backoffRef.current > 0) { backoffRef.current--; return; }
    try {
      const [s, f, t] = await Promise.all([
        fetch(`${API}/stats`).then(r => r.json()),
        fetch(`${API}/fraud-scores?limit=20`).then(r => r.json()),
        fetch(`${API}/top-risky-users`).then(r => r.json()),
      ]);
      errorCount.current = 0;
      backoffRef.current = 0;
      setError(null);
      setStats(s);
      setScores(f.fraud_scores || []);
      setTopUsers(t.top_risky_users || []);
    } catch (e) {
      errorCount.current++;
      backoffRef.current = Math.min(Math.pow(2, errorCount.current - 1), 32);
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const id = setInterval(fetchData, 5000);
    return () => clearInterval(id);
  }, []);

  return (
    <>
      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        * { box-sizing: border-box; }
        body { margin: 0; background: #f5f6fa; }
      `}</style>
      <div style={{ minHeight: "100vh", color: "#1a1a2e", fontFamily: "system-ui, sans-serif" }}>

        {/* Header */}
        <div style={{ background: "#fff", borderBottom: "1px solid #e8e8e8", padding: "16px 32px",
                      display: "flex", justifyContent: "space-between", alignItems: "center",
                      boxShadow: "0 1px 4px rgba(0,0,0,0.06)" }}>
          <div>
            <h1 style={{ color: "#e94560", margin: 0, fontSize: 20, fontWeight: 700, letterSpacing: 1 }}>
              FRAUD DETECTION
            </h1>
            <div style={{ color: "#bbb", fontSize: 11, marginTop: 2 }}>
              Redpanda → Spark → StarRocks
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ width: 8, height: 8, borderRadius: "50%",
                           background: error ? "#e53e3e" : "#38a169", display: "inline-block" }} />
            <span style={{ color: error ? "#e53e3e" : "#38a169", fontSize: 12, fontWeight: 500 }}>
              {error ? "Disconnected" : "Live"}
            </span>
            <span style={{ color: "#ccc", fontSize: 12, marginLeft: 8 }}>
              {stats?.last_ingest?.slice(0, 19).replace("T", " ")} UTC
            </span>
          </div>
        </div>

        <div style={{ padding: "24px 32px" }}>
          {loading ? SPINNER : (
            <>
              {/* Stat cards */}
              <div style={{ display: "flex", gap: 16, marginBottom: 32, flexWrap: "wrap" }}>
                <StatCard label="Total Transactions (1h)" value={stats?.total_transactions} color="#3182ce" />
                <StatCard label="Flagged (1h)"            value={stats?.flagged_count}      color="#e53e3e" />
                <StatCard label="Avg Fraud Score (1h)"    value={stats?.avg_fraud_score?.toFixed(1)} color="#dd6b20" />
                <StatCard label="Flag Rate (1h)"
                  value={stats?.total_transactions
                    ? ((stats.flagged_count / stats.total_transactions) * 100).toFixed(1) + "%"
                    : "—"}
                  color="#e94560" />
              </div>

              {/* Chart + Top Users side by side */}
              <div style={{ display: "flex", gap: 24, marginBottom: 32, flexWrap: "wrap" }}>
                <div style={{ flex: "2 1 400px", background: "#fff", border: "1px solid #e8e8e8",
                              borderRadius: 8, padding: 20, boxShadow: "0 1px 4px rgba(0,0,0,0.06)" }}>
                  <div style={{ color: "#999", fontSize: 11, textTransform: "uppercase",
                                letterSpacing: 1, marginBottom: 16 }}>
                    Recent Fraud Scores
                  </div>
                  {scores.length === 0
                    ? <div style={{ color: "#ccc", textAlign: "center", padding: 40 }}>No alerts yet</div>
                    : <ScoreChart scores={scores} />
                  }
                </div>

                <div style={{ flex: "1 1 200px", background: "#fff", border: "1px solid #e8e8e8",
                              borderRadius: 8, padding: 20, boxShadow: "0 1px 4px rgba(0,0,0,0.06)" }}>
                  <div style={{ color: "#999", fontSize: 11, textTransform: "uppercase",
                                letterSpacing: 1, marginBottom: 16 }}>
                    Top Risky Users (24h)
                  </div>
                  {topUsers.length === 0
                    ? <div style={{ color: "#ccc", fontSize: 13 }}>No data</div>
                    : topUsers.map((u, i) => (
                      <div key={u.user_id} style={{
                        display: "flex", justifyContent: "space-between", alignItems: "center",
                        padding: "8px 0", borderBottom: i < topUsers.length - 1 ? "1px solid #f0f0f0" : "none"
                      }}>
                        <span style={{ color: "#1a1a2e", fontSize: 13, fontWeight: 500 }}>{u.user_id}</span>
                        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                          <span style={{ color: "#aaa", fontSize: 11 }}>{u.flagged_count} flags</span>
                          <ScoreBadge score={u.max_score} />
                        </div>
                      </div>
                    ))
                  }
                </div>
              </div>

              {/* Fraud table */}
              <div style={{ background: "#fff", border: "1px solid #e8e8e8", borderRadius: 8,
                            boxShadow: "0 1px 4px rgba(0,0,0,0.06)" }}>
                <div style={{ padding: "16px 20px", borderBottom: "1px solid #f0f0f0",
                              display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={{ color: "#999", fontSize: 11, textTransform: "uppercase", letterSpacing: 1 }}>
                    Recent Fraud Alerts
                  </span>
                  <span style={{ color: "#ccc", fontSize: 11 }}>Auto-refreshes every 5s</span>
                </div>
                {scores.length === 0
                  ? <div style={{ color: "#ccc", textAlign: "center", padding: 40, fontSize: 13 }}>
                      No fraud alerts detected yet
                    </div>
                  : <FraudTable scores={scores} />
                }
              </div>
            </>
          )}
        </div>
      </div>
    </>
  );
}
