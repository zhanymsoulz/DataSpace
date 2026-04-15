import { useState } from "react";

export default function SmartDataApp() {
  const [text, setText] = useState("");
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async () => {
    setLoading(true);
    setError(null);
    setData(null);
    try {
      const response = await fetch(
        `http://localhost:8080/merged-python-data?text=${encodeURIComponent(text)}`
      );
      const result = await response.json();
if (result.status === "error") throw new Error(result.message);
if (result.status === "empty") {
  setData([]); // optional: show a message to user
  setError(result.message);
  return;
}
setData(result.data);

    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ maxWidth: "720px", margin: "2rem auto", padding: "1rem" }}>
      <div style={{ border: "1px solid #ccc", borderRadius: "8px", padding: "1rem" }}>
        <h2 style={{ fontSize: "1.2rem", marginBottom: "0.5rem" }}>Ask about the weather</h2>
        <input
          type="text"
          placeholder="e.g., Show me weather for Jan 1st 2014"
          value={text}
          onChange={(e) => setText(e.target.value)}
          style={{ width: "100%", padding: "0.5rem", marginBottom: "0.5rem", borderRadius: "4px", border: "1px solid #ccc" }}
        />
        <button
          onClick={handleSubmit}
          disabled={loading}
          style={{ padding: "0.5rem 1rem", borderRadius: "4px", backgroundColor: "#2563eb", color: "white", border: "none" }}
        >
          {loading ? "Fetching..." : "Get Data"}
        </button>
        
      </div>
      

      {error && (
        <div style={{ color: "red", marginTop: "1rem" }}>
          Error: {error}
        </div>
      )}

      {data && Array.isArray(data) && data.length > 0 && (
        <div style={{ marginTop: "1rem", overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.9rem" }}>
            <thead>
              <tr>
                {Object.keys(data[0]).map((key) => (
  <th key={key} style={{ borderBottom: "1px solid #ccc", textAlign: "left", padding: "0.5rem" }}>
    {key === "_source" ? "Source" : key}
  </th>
))}

              </tr>
            </thead>
            <tbody>
              {data.map((row, i) => (
                <tr key={i}>
                  {Object.values(row).map((val, j) => (
                    <td key={j} style={{ padding: "0.5rem", borderBottom: "1px solid #eee" }}>{val}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {data && typeof data === "string" && (
        <div style={{ marginTop: "1rem", textAlign: "center", color: "#555" }}>
          {data}
        </div>
      )}
    
    </div>
  );
}
