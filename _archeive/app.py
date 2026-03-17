import streamlit as st
import pandas as pd
import time
from _archeive.main import app

st.set_page_config(page_title="Fleet Ops AI", layout="wide")

# --- SIDEBAR: SETTINGS & STATUS ---
with st.sidebar:
    st.title("⚙️ Pipeline Settings")
    st.info("Connected to DigitalOcean Stage DB")

    show_sql = st.checkbox("Show Analysis Logic", value=False)
    show_raw_data = st.checkbox("Show Raw Data (Dev Mode)", value=False)

    if st.button("🗑️ Clear Chat History"):
        st.session_state.messages = []
        st.session_state.last_query = None
        st.rerun()
    
    # Refactor button - only show if we have a last query with issues
    if st.session_state.get("last_query") and st.session_state.get("last_error"):
        if st.button("🔄 Try Rephrasing Query"):
            st.session_state.needs_refactor = True
            st.session_state.refactor_trigger = True
            st.rerun()

    st.markdown("---")
    st.caption("Engine: Gemini 2.0 Flash")
    st.caption("Mode: Read-Only Access")

# --- MAIN AREA: CHAT INTERFACE ---
st.title("🚛 Fleet Operations Intelligence")

# Show refactoring indicator if it's happening
if st.session_state.get("refactor_trigger"):
    st.info("🔄 Rephrasing your question for better understanding...")

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "last_query" not in st.session_state:
    st.session_state.last_query = None
if "last_error" not in st.session_state:
    st.session_state.last_error = None
if "needs_refactor" not in st.session_state:
    st.session_state.needs_refactor = False
if "refactor_trigger" not in st.session_state:
    st.session_state.refactor_trigger = False

# Display chat history (messages only, no SQL/data expansion)
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Handle New User Input or Refactor Trigger
query = st.chat_input("Ask about orders, fleet status, or any data insight...")
refactoring = st.session_state.get("refactor_trigger", False)

if query or refactoring:
    if refactoring and st.session_state.get("last_query"):
        # Use the last query for refactoring
        query = st.session_state.last_query
        st.session_state.refactor_trigger = False
        st.session_state.last_error = None
        # Show that we're retrying with refactoring
        st.chat_message("assistant").markdown("🔄 *Rephrasing your question for better understanding...*")
    
    # 1. Show user message (only if it's a new query, not refactoring)
    if not refactoring:
        st.chat_message("user").markdown(query)
        st.session_state.messages.append({"role": "user", "content": query})
        st.session_state.last_query = query

    # 2. Generate response
    with st.chat_message("assistant"):
        with st.spinner("✨ Analyzing your query..."):
            initial_state = {
                "query": query.strip(),
                "results": [],
                "error": None,
                "sql": "",
                "intent": "",
                "intent_type": "",
                "chart": {"chart_type": "none"},
                "messages": st.session_state.messages,
                "conversation_context": None,
                "pending_questions": None,
                "question_index": 0,
                "original_query": None,
                "refactor_attempts": 0,
                "needs_refactor": st.session_state.get("needs_refactor", False)
            }

            start_time = time.time()
            result_state = app.invoke(initial_state)
            elapsed = time.time() - start_time
            
            # Display timing
            st.caption(f"⏱️ Processed in {elapsed:.2f}s")

            if result_state.get("error"):
                error_msg = f"❌ {result_state['error']}"
                st.error(error_msg)
                st.session_state.last_error = result_state.get("error")
                st.session_state.messages.append({"role": "assistant", "content": error_msg})
            
            elif result_state.get("intent_type") == "chat":
                # Chat response - display LLM output
                if result_state.get("results"):
                    response_content = result_state["results"][0].get("response", "No response generated")
                    st.markdown(response_content)
                    st.session_state.messages.append({"role": "assistant", "content": response_content})
            
            else:
                # SQL Query Response - Display formatted summary + chart
                formatted_results = result_state.get("results", [])
                chart_info = result_state.get("chart", {})
                sql_code = result_state.get("sql", "")
                
                # --- Display the formatted summary (always do this) ---
                if formatted_results and len(formatted_results) > 0:
                    response_data = formatted_results[0]
                    
                    if "summary" in response_data:
                        summary = response_data["summary"]
                        st.markdown(summary)
                        
                        # Add to chat history
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": summary
                        })
                
                # --- Display SQL if requested ---
                if show_sql and sql_code:
                    with st.expander("🔍 View Generated SQL"):
                        st.code(sql_code, language="sql")
                
                # --- Display visualization if available ---
                if chart_info and chart_info.get("chart_type") != "none":
                    try:
                        figure = chart_info.get("figure")
                        if figure:
                            st.plotly_chart(figure, use_container_width=True)
                    except Exception as e:
                        pass  # Silently skip if chart not available
                
                # --- Show raw data in dev mode ---
                if show_raw_data and chart_info.get("raw_data"):
                    with st.expander("📋 Raw Data (Dev Mode)"):
                        df = pd.DataFrame(chart_info.get("raw_data", []))
                        st.dataframe(df, use_container_width=True)
                        st.caption(f"📊 {len(df)} records retrieved")
                
                # --- Show if there are more questions pending ---
                pending = result_state.get("pending_questions")
                if pending and len(pending) > 0:
                    st.info(f"📋 Processing {len(pending)} more questions from your query...")

st.markdown("---")
st.caption("Gemini 2.0 Flash | Read-Only Database Access")