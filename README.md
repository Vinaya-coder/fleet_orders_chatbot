# Fleet Orders Chatbot

A conversational AI assistant for querying and analysing fleet order data using natural language. Built on a true ReAct agent architecture where the LLM decides what to do at every step — no hardcoded routing logic.

## What it does

Ask questions about your fleet operations in plain English and get instant answers with visualizations:

- "Show me all failed orders from last week"
- "Compare processed orders across January, February and March"
- "What is the total token consumption this week?"
- "Which email requests have failed to complete?"

## Architecture

The system is built on three layers:

**Tools layer** (`tools.py`) — Two plain Python functions that the agent can call. `get_schema()` fetches the live database structure and `run_query()` executes SQL after validating it through the existing SQL analyst.

**Agent layer** (`assistant.py`) — A ReAct agent loop where Gemini decides which tools to call, in what order, and when to stop. Three tools are available: `get_schema_tool`, `run_query_tool`, and `create_chart_tool`. The agent self-corrects on SQL errors and auto-generates visualizations for multi-row results.

**API layer** (`api.py`) — A FastAPI backend that receives queries from the frontend, runs the agent, and returns the answer along with optional chart data.

```
Frontend (React)
      ↓
   api.py  (FastAPI)
      ↓
assistant.py  (ReAct Agent)
      ↓
   tools.py
   ↙        ↘
db_engine   sql_analyst
(schema +   (SQL validation
 queries)    before execution)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| LLM | Google Gemini 2.0 Flash |
| Agent Framework | LangChain |
| Backend | FastAPI + Uvicorn |
| Database | PostgreSQL |
| ORM / DB access | SQLAlchemy |
| Visualizations | Plotly |
| Frontend | React + Tailwind CSS |
| Observability | LangSmith |
| Data processing | Pandas |
