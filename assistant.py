import json
from multiprocessing import context
import re
import plotly.io as pio
from dotenv import load_dotenv
load_dotenv()
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.rate_limiters import InMemoryRateLimiter
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage, ToolMessage
from langchain_core.tools import tool
from tools import get_schema, run_query
from src.reporting.visualizer import create_visualization
from config.prompts import AGENT_SYSTEM_PROMPT



MAX_ITERATIONS = 8
MAX_SQL_RETRIES = 2
ALLOWED_TOOLS = {"get_schema_tool", "run_query_tool", "create_chart_tool"}

pacer = InMemoryRateLimiter(
    requests_per_second=2,
    check_every_n_seconds=0.5,
    max_bucket_size=5,
)

llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash",
    temperature=0,
    rate_limiter=pacer,
    max_retries=5,
    timeout=30,
)

_WRITE_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|REPLACE)\b",
    re.IGNORECASE,
)

def _is_safe_sql(sql: str) -> bool:
    return not _WRITE_PATTERN.search(sql)


@tool
def get_schema_tool() -> str:
    """Fetch the full Fleet database schema including tables, columns, enums and foreign keys."""
    return get_schema()


@tool
def run_query_tool(sql: str) -> str:
    """
    Execute a PostgreSQL SELECT query and return results as JSON.
    If the query fails, returns the error message so you can fix and retry.

    Args:
        sql: A valid PostgreSQL SELECT statement.
    """
    if not _is_safe_sql(sql):
        return "ERROR: Write operations are not permitted."
    try:
        rows = run_query(sql)
        return json.dumps(rows, default=str)
    except Exception as e:
        return f"ERROR: {str(e)}"


@tool
def create_chart_tool(data: str, chart_type: str, x_axis: str, y_axis: str, title: str) -> str:
    """
    Create a visualization from query results. Call this when:
    - User says 'show me', 'list', 'display', 'table' -> use chart_type='table'
    - User asks for a chart, graph, plot, or visualization -> use appropriate chart type
    - Results have more than 5 rows -> always use chart_type='table' for readability
    - Data shows trends or comparisons -> use 'bar' or 'line'
    - Data shows proportions -> use 'pie'

    Do NOT skip this tool when results have many rows.

    Args:
        data: JSON string of rows returned from run_query_tool
        chart_type: one of 'bar', 'line', 'pie', 'table'
        x_axis: column name for x axis
        y_axis: column name for y axis (use any numeric column, or first column if none)
        title: short descriptive title
    """
    try:
        actual_data = json.loads(data) if isinstance(data, str) else data
        print(f"  CHART TOOL - type: {chart_type}, rows: {len(actual_data)}")
        result = create_visualization(
            data=actual_data,
            chart_type=chart_type,
            x_axis=x_axis,
            y_axis=y_axis,
            title=title,
        )
        if result.get("figure"):
            result["spec"] = pio.to_json(result["figure"])
            del result["figure"]
        return json.dumps(result, default=str)
    except Exception as e:
        print(f"  CHART TOOL ERROR: {e}")
        return f"ERROR creating chart: {str(e)}"


TOOLS = [get_schema_tool, run_query_tool, create_chart_tool]
llm_with_tools = llm.bind_tools(TOOLS)

TOOL_MAP = {
    "get_schema_tool": get_schema_tool,
    "run_query_tool": run_query_tool,
    "create_chart_tool": create_chart_tool,
}

def run_agent(user_query: str, chat_history: list = None, previous_rows: list = None,context: dict = None) -> dict:
    if not user_query or not user_query.strip():
        return {
            "answer": "Please ask a question.",
            "sql": None, "rows": None,
            "chart": None, "steps": [], "error": None
        }

    messages = [SystemMessage(content=AGENT_SYSTEM_PROMPT)]
    messages.append(HumanMessage(content=user_query))
    if context and context.get("prev_question"):
        messages.append(SystemMessage(
           content=f"Previous question: {context['prev_question']}\nPrevious answer: {context['prev_answer']}\nUse this ONLY if the current question is a direct follow-up."
    ))

    steps = []
    last_sql = None
    last_rows = None
    last_chart = None
    sql_error_count = 0
    iteration = 0
    hard_error = None

    while iteration < MAX_ITERATIONS:
        iteration += 1
        print(f"  Iteration {iteration}/{MAX_ITERATIONS}")

        response = llm_with_tools.invoke(messages)
        messages.append(response)
        print(f"  Tool calls: {[tc['name'] for tc in response.tool_calls] if response.tool_calls else 'NONE - final answer'}")

        if not response.tool_calls:
            break

        for tc in response.tool_calls:
            tool_name = tc["name"]
            tool_args = tc["args"]

            if tool_name not in ALLOWED_TOOLS:
                tool_result = f"ERROR: Tool '{tool_name}' is not permitted."
                print(f"  Blocked disallowed tool: {tool_name}")
            else:
                print(f"  -> Agent calling: {tool_name}")
                tool_fn = TOOL_MAP[tool_name]
                tool_result = tool_fn.invoke(tool_args)

            steps.append({
                "tool": tool_name,
                "args": tool_args,
                "result_preview": str(tool_result)[:200],
            })

            if tool_name == "run_query_tool":
                last_sql = tool_args.get("sql")
                if str(tool_result).startswith("ERROR"):
                    sql_error_count += 1
                    print(f"  SQL error {sql_error_count}/{MAX_SQL_RETRIES}")
                    if sql_error_count >= MAX_SQL_RETRIES:
                        hard_error = f"SQL failed after {MAX_SQL_RETRIES} attempts."
                        messages.append(ToolMessage(
                            content=str(tool_result) + "\nMax retries reached. Explain to the user what went wrong.",
                            tool_call_id=tc["id"]
                        ))
                        continue
                else:
                    sql_error_count = 0
                    try:
                        last_rows = json.loads(tool_result)
                    except Exception:
                        last_rows = None

            elif tool_name == "create_chart_tool":
                if not str(tool_result).startswith("ERROR"):
                    try:
                        last_chart = json.loads(tool_result)
                    except Exception:
                        last_chart = None

            messages.append(ToolMessage(
                content=str(tool_result),
                tool_call_id=tc["id"]
            ))

    else:
        return {
            "answer": "Agent stopped — query required too many steps. Try rephrasing.",
            "sql": last_sql,
            "rows": last_rows,
            "chart": None,
            "steps": steps,
            "error": f"Hit max {MAX_ITERATIONS} iterations.",
        }

    # Auto-create visualization if LLM skipped it and we have multiple rows
    if last_rows and len(last_rows) > 3 and last_chart is None:
        print("  -> Auto-creating visualization (LLM skipped create_chart_tool)")
        try:
            q = user_query.lower()
            chart_type = "bar" if "bar" in q else \
                         "line" if ("line" in q or "trend" in q) else \
                         "pie" if "pie" in q else \
                         "table"
            cols = list(last_rows[0].keys())
            numeric_cols = [c for c in cols if isinstance(last_rows[0][c], (int, float))]
            x_axis = cols[0]
            y_axis = numeric_cols[0] if numeric_cols else cols[1] if len(cols) > 1 else cols[0]
            result = create_visualization(
                data=last_rows,
                chart_type=chart_type,
                x_axis=x_axis,
                y_axis=y_axis,
                title=user_query[:60],
            )
            if result.get("figure"):
                result["spec"] = pio.to_json(result["figure"])
                del result["figure"]
                last_chart = result
        except Exception as e:
            print(f"  Auto-chart failed: {e}")

    return {
        "answer": response.content,
        "sql": last_sql,
        "rows": last_rows,
        "chart": last_chart,
        "steps": steps,
        "error": hard_error,
    }