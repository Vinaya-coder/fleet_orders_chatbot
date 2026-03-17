from unittest import result

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from assistant import run_agent
import json
import time
import plotly.io as pio

api = FastAPI()

api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def serialize_chart(chart_info: dict):
    if not chart_info or chart_info.get("chart_type") == "none":
        return {"chart_type": "none"}
    try:
        figure = chart_info.get("figure")
        if figure:
            fig_json = pio.to_json(figure)
            return {
                "chart_type": chart_info.get("chart_type"),
                "spec": fig_json
            }
        spec = chart_info.get("spec")
        if spec:
            return {
                "chart_type": chart_info.get("chart_type"),
                "spec": spec if isinstance(spec, str) else json.dumps(spec)
            }
    except Exception as e:
        print(f"Chart serialization failed: {e}")
    return {"chart_type": "none"}


@api.post("/chat")
async def chat_endpoint(request: Request):
    payload = await request.json()
    start_time = time.time()

    query = payload.get("query", "")
    messages_history = payload.get("messages", [])

    try:
        clean_context = payload.get("context")
        result = run_agent(
            user_query=query,
            chat_history=[],
            context=clean_context
        )
    

        # Add this exchange to message history
        updated_messages = messages_history + [
            {"role": "user", "content": query},
            {"role": "assistant", "content": result["answer"]},
        ]

        chart_serialized = serialize_chart(result.get("chart") or {})
        print("CHART DEBUG:", chart_serialized.get("chart_type"), "has spec:", "spec" in chart_serialized)
        elapsed = time.time() - start_time
        print(f"Total API response time: {elapsed:.2f}s")

        return {
            "summary": result["answer"],
            "sql": result.get("sql", ""),
            "chart": chart_serialized,
            "messages": updated_messages,
            "error": result.get("error"),
            "responseTime": f"{elapsed:.2f}s",
            "previous_rows": result.get("rows"),
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        elapsed = time.time() - start_time
        return {
            "summary": f"Error: {str(e)}",
            "sql": "",
            "chart": {"chart_type": "none"},
            "error": str(e),
            "responseTime": f"{elapsed:.2f}s",
        }


@api.post("/refactor")
async def refactor_response(request: Request):
    payload = await request.json()
    start_time = time.time()

    messages = payload.get("messages", [])
    refactor_instruction = payload.get("instruction", "Make it clearer and more concise")

    if not messages:
        return {"error": "No previous message to refactor", "summary": "", "messages": messages}

    try:
        from langchain_google_genai import ChatGoogleGenerativeAI
        from langchain_core.prompts import ChatPromptTemplate

        refactor_llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)

        last_assistant_msg = None
        for msg in reversed(messages):
            if msg.get("role") == "assistant":
                last_assistant_msg = msg.get("content", "")
                break

        if not last_assistant_msg:
            return {"error": "No previous assistant response to refactor", "summary": "", "messages": messages}

        refactor_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a response refinement expert.
Original Response: {previous_response}
User's Refactoring Request: {instruction}
Generate an improved version. Respond ONLY with the refined response, no explanation."""),
            ("human", "Refactor the response above.")
        ])

        response = refactor_llm.invoke(refactor_prompt.format_prompt(
            previous_response=last_assistant_msg,
            instruction=refactor_instruction
        ).to_messages())

        refined_response = response.content.strip()

        updated_messages = messages.copy()
        for i in range(len(updated_messages) - 1, -1, -1):
            if updated_messages[i].get("role") == "assistant":
                updated_messages[i] = {"role": "assistant", "content": refined_response}
                break

        elapsed = time.time() - start_time
        return {
            "summary": refined_response,
            "messages": updated_messages,
            "responseTime": f"{elapsed:.2f}s",
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        elapsed = time.time() - start_time
        return {
            "error": f"Refactoring failed: {str(e)}",
            "summary": "",
            "messages": messages,
            "responseTime": f"{elapsed:.2f}s",
        }


if __name__ == "__main__":
    import uvicorn
    print("Fleet Orders API starting on http://localhost:8000")
    uvicorn.run("api:api", host="0.0.0.0", port=8000, reload=True)