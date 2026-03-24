# O2C Context Graph System

This project ingests the SAP order-to-cash dataset, builds a context graph, visualizes it, and provides a guardrailed natural language query interface that generates SQL dynamically.

## Architecture
- **Storage**: SQLite (`backend/data/o2c.db`) with raw tables plus graph tables (`nodes`, `edges`).
- **Graph modeling**: Entities such as Sales Orders, Deliveries, Billing, Payments, Products, Plants, and Business Partners are modeled as nodes. Relationships across documents and reference fields are modeled as edges.
- **LLM flow**: Natural language -> SQL (guardrailed) -> execute -> data-backed answer. Optional LLM summarization for the answer.
- **UI**: Cytoscape.js for graph visualization + chat panel for querying.

## Setup

### 1) Install dependencies
```bash
cd backend
pip install -r requirements.txt
```

### 2) Load the dataset into SQLite
```bash
python data_loader.py
```
If the dataset is not under `sap-o2c-data` in the repo root, set:
```bash
set O2C_DATASET_DIR=C:\path\to\sap-o2c-data
```

### 3) Configure LLM
Choose a provider and set env vars.

Example for **OpenRouter**:
```bash
set LLM_PROVIDER=openrouter
set LLM_API_KEY=YOUR_KEY
set LLM_MODEL=meta-llama/llama-3.1-8b-instruct
set LLM_BASE_URL=https://openrouter.ai/api/v1
```

Optional: enable LLM-written answers (default is template summary):
```bash
set LLM_ANSWER_MODE=llm
```

### 4) Run the server
```bash
uvicorn app:app --reload
```
Open http://localhost:8000

## Example Queries
- Which products are associated with the highest number of billing documents?
- Trace the full flow of a given billing document.
- Identify sales orders that have broken flows (delivered but not billed, billed without delivery).

## Guardrails
- Queries are restricted to dataset tables only.
- Non-domain prompts are rejected with a fixed response.
- Only `SELECT` statements are allowed.

## Submission Checklist
- Working demo link: https://dodge-o6x2.onrender.com/
- Public GitHub repository: https://github.com/Nishtha170/dodge
- README with architecture and LLM prompting strategy: included
- AI coding session logs: `ai_sessions/codex_session_log.md`

