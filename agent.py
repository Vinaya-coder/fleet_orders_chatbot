import json
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.rate_limiters import InMemoryRateLimiter
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage, ToolMessage
from langchain_core.tools import tool
from tools import get_schema, run_query
from config.prompts import AGENT_SYSTEM_PROMPT

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
    try:
        rows = run_query(sql)
        return json.dumps(rows, default=str)
    except Exception as e:
        return f"ERROR: {str(e)}"

TOOLS = [get_schema_tool, run_query_tool]
llm_with_tools = llm.bind_tools(TOOLS)

TOOL_MAP = {
    "get_schema_tool": get_schema_tool,
    "run_query_tool": run_query_tool,
}

def run_agent(user_query: str, chat_history: list = None) -> dict:
    messages = [SystemMessage(content=AGENT_SYSTEM_PROMPT)]

    if chat_history:
        for msg in chat_history:
            if msg["role"] == "user":
                messages.append(HumanMessage(content=msg["content"]))
            else:
                messages.append(AIMessage(content=msg["content"]))

    messages.append(HumanMessage(content=user_query))

    steps = []
    last_sql = None
    last_rows = None

    while True:
        response = llm_with_tools.invoke(messages)
        messages.append(response)

        if not response.tool_calls:
            break

        for tc in response.tool_calls:
            tool_name = tc["name"]
            tool_args = tc["args"]

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
                if not str(tool_result).startswith("ERROR"):
                    try:
                        last_rows = json.loads(tool_result)
                    except Exception:
                        last_rows = None

            messages.append(ToolMessage(
                content=str(tool_result),
                tool_call_id=tc["id"]
            ))

    return {
        "answer": response.content,
        "sql": last_sql,
        "rows": last_rows,
        "steps": steps,
        "error": None,
    }