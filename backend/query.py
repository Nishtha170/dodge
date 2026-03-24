import json
import os
import re
from typing import Any, Dict, List, Tuple

try:
    from .db import get_connection
    from .llm import chat_completion
except ImportError:
    from db import get_connection
    from llm import chat_completion

SCOPE_KEYWORDS = [
    "order",
    "sales order",
    "delivery",
    "billing",
    "invoice",
    "payment",
    "customer",
    "product",
    "plant",
    "journal",
    "accounting",
    "business partner",
    "schedule line",
    "document",
]

FORBIDDEN_SQL = ["insert", "update", "delete", "drop", "alter", "pragma", "attach", "detach"]


def is_in_scope(question: str) -> bool:
    lower = question.lower()
    return any(term in lower for term in SCOPE_KEYWORDS)


def get_schema_summary() -> str:
    conn = get_connection()
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        lines: List[str] = []
        for table_row in tables:
            table = table_row[0]
            cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
            col_names = ", ".join([col[1] for col in cols])
            lines.append(f"{table}({col_names})")
        return "\n".join(lines)
    finally:
        conn.close()


def safe_sql(sql: str) -> Tuple[bool, str]:
    if not sql:
        return False, "Empty SQL"
    compact = sql.strip().lower()
    if not compact.startswith("select"):
        return False, "Only SELECT statements are allowed"
    if ";" in compact[:-1]:
        return False, "Multiple statements are not allowed"
    for keyword in FORBIDDEN_SQL:
        if re.search(rf"\b{keyword}\b", compact):
            return False, f"Forbidden keyword: {keyword}"
    return True, ""


def generate_sql(question: str, schema_summary: str) -> Dict[str, Any]:
    system = (
        "You are a data analyst for an SAP order-to-cash dataset. "
        "Decide if the question is in scope. If in scope, produce a single SQLite SELECT query. "
        "If out of scope, set in_scope=false and leave sql empty. "
        "Return strictly valid JSON with keys: in_scope (boolean), sql (string), reason (string)."
    )
    user = (
        "Schema:\n"
        f"{schema_summary}\n\n"
        "Question:\n"
        f"{question}\n\n"
        "Rules:\n"
        "- Use only the tables and columns listed.\n"
        "- Use SQLite syntax.\n"
        "- Prefer joins on matching id fields.\n"
        "- Do not use write operations.\n"
    )

    content = chat_completion(
        [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        response_format={"type": "json_object"},
    )
    return json.loads(content)


def execute_sql(sql: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    conn = get_connection()
    try:
        cursor = conn.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return rows, columns
    finally:
        conn.close()


def summarize_rows(rows: List[Dict[str, Any]], columns: List[str]) -> str:
    if not rows:
        return "No rows returned."
    if len(rows) == 1 and len(columns) == 1:
        return f"Result: {rows[0][columns[0]]}"
    preview = rows[:10]
    lines = ["Top rows:"]
    lines.append(", ".join(columns))
    for row in preview:
        lines.append(", ".join([str(row.get(col, "")) for col in columns]))
    if len(rows) > 10:
        lines.append(f"... and {len(rows) - 10} more")
    return "\n".join(lines)


def answer_with_llm(question: str, sql: str, rows: List[Dict[str, Any]]) -> str:
    mode = os.environ.get("LLM_ANSWER_MODE", "template").lower()
    if mode != "llm":
        return summarize_rows(rows, list(rows[0].keys()) if rows else [])

    system = (
        "You answer questions strictly from the provided SQL results. "
        "If the result is empty, say so and avoid speculation."
    )
    user = json.dumps({"question": question, "sql": sql, "rows": rows})
    content = chat_completion(
        [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
    )
    return content.strip()
