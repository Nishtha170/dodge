from pathlib import Path
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.db import get_connection
from query import answer_with_llm, execute_sql, generate_sql, get_schema_summary, is_in_scope, safe_sql

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="O2C Context Graph")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/api/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/node/{node_id}")
def get_node(node_id: str) -> Dict[str, Any]:
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Node not found")
        return dict(row)
    finally:
        conn.close()


@app.get("/api/graph/overview")
def graph_overview(limit: int = 200) -> Dict[str, Any]:
    conn = get_connection()
    try:
        nodes = [dict(row) for row in conn.execute("SELECT * FROM nodes LIMIT ?", (limit,))]
        node_ids = [row["id"] for row in nodes]
        if not node_ids:
            return {"nodes": [], "edges": []}
        placeholders = ", ".join(["?" for _ in node_ids])
        edges = [
            dict(row)
            for row in conn.execute(
                f"SELECT * FROM edges WHERE source_id IN ({placeholders}) OR target_id IN ({placeholders})",
                tuple(node_ids + node_ids),
            )
        ]
        return {"nodes": nodes, "edges": edges}
    finally:
        conn.close()


@app.post("/api/graph/expand")
def graph_expand(payload: Dict[str, Any]) -> Dict[str, Any]:
    node_id = payload.get("node_id")
    limit = int(payload.get("limit", 200))
    if not node_id:
        raise HTTPException(status_code=400, detail="node_id required")
    conn = get_connection()
    try:
        edges = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM edges WHERE source_id = ? OR target_id = ? LIMIT ?",
                (node_id, node_id, limit),
            )
        ]
        node_ids = {node_id}
        for edge in edges:
            node_ids.add(edge["source_id"])
            node_ids.add(edge["target_id"])
        placeholders = ", ".join(["?" for _ in node_ids])
        nodes = [
            dict(row)
            for row in conn.execute(f"SELECT * FROM nodes WHERE id IN ({placeholders})", tuple(node_ids))
        ]
        return {"nodes": nodes, "edges": edges}
    finally:
        conn.close()


@app.post("/api/search")
def search_nodes(payload: Dict[str, Any]) -> Dict[str, Any]:
    query = (payload.get("query") or "").strip()
    if not query:
        return {"nodes": []}
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM nodes WHERE label LIKE ? OR id LIKE ? LIMIT 50",
            (f"%{query}%", f"%{query}%"),
        ).fetchall()
        return {"nodes": [dict(row) for row in rows]}
    finally:
        conn.close()


@app.post("/api/chat")
def chat(payload: Dict[str, Any]) -> Dict[str, Any]:
    question = (payload.get("message") or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="message required")

    def _normalize_search(q: str) -> str:
        lower = q.lower().strip()
        for prefix in ("where is", "find", "search", "lookup", "locate"):
            if lower.startswith(prefix):
                return q[len(prefix):].strip()
        return q.strip()

    def _search_nodes_like(q: str, limit: int = 10):
        conn = get_connection()
        try:
            rows = conn.execute(
                "SELECT * FROM nodes WHERE label LIKE ? OR id LIKE ? LIMIT ?",
                (f"%{q}%", f"%{q}%", limit),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    if not is_in_scope(question):
        lookup = _normalize_search(question)
        if lookup:
            matches = _search_nodes_like(lookup)
            if matches:
                lines = ["Found matching nodes:"]
                for node in matches:
                    lines.append(f"- {node.get('id')} | {node.get('label')} ({node.get('type')})")
                return {
                    "in_scope": True,
                    "answer": "\n".join(lines),
                    "sql": "",
                    "rows": matches,
                    "columns": [],
                }
            return {
                "in_scope": True,
                "answer": "No matching nodes found in the dataset.",
                "sql": "",
                "rows": [],
                "columns": [],
            }
        return {
            "in_scope": False,
            "answer": "This system is designed to answer questions related to the provided dataset only.",
            "sql": "",
            "rows": [],
        }

    schema_summary = get_schema_summary()
    try:
        llm_result = generate_sql(question, schema_summary)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"LLM error: {exc}")

    if not llm_result.get("in_scope", False):
        return {
            "in_scope": False,
            "answer": "This system is designed to answer questions related to the provided dataset only.",
            "sql": "",
            "rows": [],
        }

    sql = (llm_result.get("sql") or "").strip()
    ok, reason = safe_sql(sql)
    if not ok:
        raise HTTPException(status_code=400, detail=f"Unsafe SQL: {reason}")

    rows, columns = execute_sql(sql)
    try:
        answer = answer_with_llm(question, sql, rows)
    except Exception:
        answer = "Unable to generate a natural language answer. Showing raw results instead."

    return {
        "in_scope": True,
        "answer": answer,
        "sql": sql,
        "rows": rows,
        "columns": columns,
    }
