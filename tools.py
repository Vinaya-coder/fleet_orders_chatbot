"""
tools.py  —  The only two tools the agent gets.

get_schema()   → returns table/column/enum/FK info as a string
run_query(sql) → executes SQL and returns rows as a list of dicts

Both wrap your existing FleetDB and engine exactly as-is.
Nothing new here — just your existing code exposed as LangChain tools.
"""

from langchain_core.tools import tool
from sqlalchemy import text
from sqlalchemy import text
from src.utils.db_engine import FleetDB
from src.agents.sql_analyst import SQLAnalyst
import json
from sqlalchemy import text
from src.utils.db_engine import FleetDB
from src.agents.sql_analyst import SQLAnalyst
_db = FleetDB()
_analyst = SQLAnalyst()
 
def _get_db():
    """Lazy import so tools.py can be imported without a live DB connection."""
    from src.utils.db_engine import FleetDB
    return FleetDB()




def get_schema() -> str:
    schema = _db.get_live_schema()
    joins = _db.get_live_joins()
    return f"{schema}\n\n--- Relationships ---\n{joins}"
 
def run_query(sql: str) -> list:
    # Run your existing SQL fixes before hitting the DB
    clean_sql = _analyst._validate_sql(sql)
    if clean_sql != sql:
        print(f"  SQL was fixed before execution")
 
    with _db.engine.connect() as conn:
        conn.execute(text("SET statement_timeout TO '30s';"))
        result = conn.execute(text(clean_sql))
        rows = result.fetchall()
        col_names = list(result.keys())
        return [dict(zip(col_names, row)) for row in rows]


def run_query(sql: str) -> list:
    """
    Executes a PostgreSQL SELECT query and returns results as a
    list of dicts (one dict per row).

    Rules you must follow before calling this tool:
    - Only SELECT statements are allowed.
    - Cast enum/text columns with ::text when filtering
      e.g. status::text ILIKE 'completed'
    - Use ILIKE instead of = for string comparisons.
    - Date arithmetic: (NOW() AT TIME ZONE 'UTC' - INTERVAL '7 days')
      The INTERVAL must be INSIDE the parentheses.
    - Add LIMIT 100 unless the query is an aggregate (COUNT/SUM/AVG).
    - A 30-second statement timeout is enforced server-side.

    Args:
        sql: A valid PostgreSQL SELECT statement.

    Returns:
        List of dicts, one per row. Empty list if no rows match.
        Single-item list with an 'error' key if the query fails —
        read the error and try a corrected query.
    """
    if not sql or not sql.strip():
        return [{"error": "Empty SQL string provided."}]

    # Only allow read queries
    normalized = sql.strip().lstrip("(").upper()
    if not normalized.startswith("SELECT") and not normalized.startswith("WITH"):
        return [{"error": f"Only SELECT/WITH queries allowed. Got: {sql[:80]}"}]

    db = _get_db()
    try:
        with db.engine.connect() as conn:
            conn.execute(text("SET statement_timeout TO '30s';"))
            result = conn.execute(text(sql))
            rows   = result.fetchall()
            cols   = list(result.keys())
            return [dict(zip(cols, row)) for row in rows]
    except Exception as e:
        # Return the error as data so the LLM can read it and self-correct
        return [{"error": str(e)}]
@tool
def create_chart_tool(data: list, chart_type: str, x_axis: str, y_axis: str, title: str) -> str:
    """
    Create a visualization from query results. Call this when the user
    explicitly asks for a chart, graph, table, or visualization — or when
    the data would be clearer as a visual (e.g. comparisons, trends, distributions).

    Args:
        data: The rows returned from run_query_tool
        chart_type: one of 'bar', 'line', 'pie', 'table'
        x_axis: column name for x axis
        y_axis: column name for y axis
        title: chart title
    """
    from src.reporting.visualizer import create_visualization
    result = create_visualization(data, chart_type, x_axis, y_axis, title)
    return json.dumps(result, default=str)