import os
import time
import json
import csv
import csv
from io import StringIO
from typing import TypedDict, List, Optional, Any
from langgraph.graph import StateGraph, END
from src.reporting.visualizer import create_visualizer
#from src.core.router import create_router
from _archeive.schema_linker import SchemaLinker
from src.agents.sql_analyst import SQLAnalyst
from src.utils.db_engine import get_db_instance, FleetDB

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.rate_limiters import InMemoryRateLimiter

# Conservative rate limiting to avoid 429 errors
pacer = InMemoryRateLimiter(
    requests_per_second=2,  
    check_every_n_seconds=0.5,
    max_bucket_size=5
)

shared_llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash",
    rate_limiter=pacer,  
    max_retries=5,  
    temperature=0,
    timeout=30  
)

linker_instance = SchemaLinker(llm=shared_llm)
analyst_instance = SQLAnalyst(llm=shared_llm)
db_manager = FleetDB()




def parse_sql_results(raw_data: Any, sql: str = None) -> List[dict]:
    """
    Convert executor.run() output into structured list of dicts.
    Handles multiple formats: string representation, list of tuples, list of dicts, or CSV string.
    """
    print(f"\n🔍 PARSE DEBUG:")
    print(f"   Type: {type(raw_data)}")
    print(f"   Length: {len(raw_data) if hasattr(raw_data, '__len__') else 'N/A'}")
    if isinstance(raw_data, str):
        print(f"   Preview: {raw_data[:150]}")
    elif isinstance(raw_data, list) and len(raw_data) > 0:
        print(f"   First item type: {type(raw_data[0])}")
        print(f"   First item: {raw_data[0]}")
    
    if not raw_data:
        print(f"   → Empty/None, returning []")
        return []
    
    # STRING that looks like Python list - need to parse it!
    if isinstance(raw_data, str):
        if raw_data.strip().startswith('[') and raw_data.strip().endswith(']'):
            print(f"   → Detected string repr of list, parsing with ast.literal_eval()...")
            try:
                import ast
                actual_list = ast.literal_eval(raw_data)
                print(f"   → Successfully parsed to {len(actual_list)} items")
                return parse_sql_results(actual_list, sql)
            except (ValueError, SyntaxError) as e:
                print(f"   → ast.literal_eval failed: {e}")
                pass
        
        # CSV STRING FORMAT
        print(f"   → Trying CSV format...")
        lines = raw_data.strip().split('\n')
        if not lines or len(lines) < 2:
            print(f"   → Not CSV (< 2 lines)")
            return []
        
        try:
            reader = csv.DictReader(StringIO(raw_data))
            result_list = list(reader)
            
            if result_list:
                print(f"   → CSV parsed: {len(result_list)} rows")
                for row in result_list:
                    for key in row:
                        try:
                            if '.' in str(row[key]):
                                row[key] = float(row[key])
                            else:
                                row[key] = int(row[key])
                        except (ValueError, TypeError):
                            pass
                
                return result_list
        except Exception as e:
            print(f"   → CSV parsing failed: {e}")
            pass
        
        return []
    
    # Already a list of dicts
    if isinstance(raw_data, list) and len(raw_data) > 0 and isinstance(raw_data[0], dict):
        print(f"   → Already list of dicts: {len(raw_data)} rows")
        return raw_data
    
    # LIST OF TUPLES or unknown iterables
    if isinstance(raw_data, list) and len(raw_data) > 0:
        print(f"   → Processing as list...")
        try:
            if isinstance(raw_data[0], tuple):
                print(f"   → List of tuples detected")
                result_list = []
                num_cols = len(raw_data[0])
                
                col_names = extract_column_names_from_sql(sql) if sql else None
                
                if not col_names:
                    col_names = [f"col_{i}" for i in range(num_cols)]
                    print(f"   → Using generic col names: {col_names}")
                else:
                    print(f"   → Extracted col names: {col_names}")
                
                for row_tuple in raw_data:
                    row_dict = dict(zip(col_names, row_tuple))
                    result_list.append(row_dict)
                
                print(f"   → Converted {len(result_list)} tuples to dicts")
                return result_list
            
            elif isinstance(raw_data[0], dict):
                print(f"   → Already dicts")
                return raw_data
            
            else:
                print(f"   → Unknown items in list, wrapping: {type(raw_data[0])}")
                return [{"value": item} for item in raw_data]
                
        except Exception as e:
            print(f"   → List processing failed: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    print(f"   → No handler matched, returning []")
    return []


def extract_column_names_from_sql(sql: str) -> List[str]:
    """
    Extract column names/aliases from SELECT clause.
    Works for simple queries like: SELECT col1, col2 AS alias, SUM(...) AS total
    """
    try:
        import re
        # Simple regex to extract SELECT columns
        # Matches: SELECT ... FROM (until FROM keyword)
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return None
        
        select_clause = select_match.group(1)
        # Split by comma, but handle nested parentheses
        cols = []
        current_col = ""
        paren_depth = 0
        
        for char in select_clause + ",":
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                col = current_col.strip()
                if col:
                    # Extract alias if exists (AS keyword), otherwise use last word
                    if ' AS ' in col.upper():
                        alias = col.split(' AS ')[-1].strip()
                        cols.append(alias)
                    else:
                        # Get last identifier (after dots/spaces)
                        parts = col.split()[-1].strip()
                        # Remove table alias prefix (e.g., "du.document_filename" -> "document_filename")
                        if '.' in parts:
                            parts = parts.split('.')[-1]
                        cols.append(parts)
                current_col = ""
                continue
            current_col += char
        
        return cols if cols else None
    except Exception as e:
        print(f"⚠️ Could not extract column names from SQL: {e}")
        return None


class AgentState(TypedDict):
    query: str
    intent: str
    intent_type: str  # "chat", "sql", "multi_question", "followup"
    schema_info: str
    sql: str
    results: List
    chart: dict
    error: Optional[str]
    messages: List[dict]  # Chat history
    conversation_context: Optional[dict]  # Previous query/result/sql for follow-ups
    pending_questions: Optional[List[str]]  # Remaining questions to process
    question_index: int  # Track which question in multi-question we're on
    original_query: Optional[str]  # Keep original for refactoring
    refactor_attempts: int  # Counter to avoid infinite refactor loops
    needs_refactor: bool  # Flag set by user or system to trigger refactoring
    is_viz_request: bool  # Flag set by classifier for visualization requests

def call_schema_fetcher(state: AgentState):
    query = state.get("query", "").strip()
    messages = state.get("messages", [])
    
    # TIMER: Start tracking
    start_time = time.time()
    
    # 1. Get cached schema (skips DB introspection on cache hit)
    schema = db_manager.get_live_schema()
    joins = db_manager.get_live_joins()
    db_context = f"{schema}\n\n{joins}"

    elapsed = time.time() - start_time
    print(f"⏱️ Schema Fetcher: {elapsed:.2f}s")
    
    return {
        "intent_type": "sql",
        "schema_info": db_context,
        "messages": messages
    }

def call_analyst(state: AgentState):
    query = state.get("query", "").strip()
    messages = state.get("messages", [])
    
    try:
        start_time = time.time()
        generated_sql = analyst_instance.generate(
            query=query, 
            schema_info=state["schema_info"],
            messages=messages,
            conversation_context=state.get("conversation_context")
        )
        elapsed = time.time() - start_time
        print(f"⏱️ SQL Analyst: {elapsed:.2f}s")
        return {"sql": generated_sql, "messages": messages}
    except Exception as e:
        print(f"❌ SQL generation error: {e}")
        return {"sql": "", "error": f"Failed to generate SQL: {str(e)}", "messages": messages}


def call_validator_executor(state: AgentState):
    sql = state["sql"].strip()
    messages = state.get("messages", [])
    
    # Skip execution if this is a visualization-only request (just re-display previous results)
    if state.get("skip_execution"):
        print(f"📊 Skipping SQL execution - using previous results for visualization")
        return {"results": state.get("results", []), "messages": messages}
    
    try:
        # Use the engine directly instead of executor.run() to avoid string representation issues
        from sqlalchemy import text
        start = time.time()
        
        with db_manager.engine.connect() as conn:
            # Use PostgreSQL statement_timeout instead of conn.settimeout()
            # Set 30 second timeout at SQL level
            conn.execute(text("SET statement_timeout TO '30s';"))
            result = conn.execute(text(sql))
            rows = result.fetchall()
            col_names = result.keys()
            
            # Convert to list of dicts
            parsed_results = []
            for row in rows:
                parsed_results.append(dict(zip(col_names, row)))
        
        elapsed = time.time() - start
        print(f"✅ DB Query: {elapsed:.2f}s | {len(parsed_results)} rows")
        
        return {"results": parsed_results, "messages": messages}
    except Exception as e:
        print(f"❌ Query error (after {time.time() - start:.1f}s): {e}")
        return {"error": str(e), "results": [], "messages": messages}


def call_visualizer(state: AgentState):
    """Create visualizations if appropriate for the data."""
    error = state.get("error")
    chart_info = state.get("chart", {})
    messages = state.get("messages", [])

    if error or chart_info.get("chart_type") == "none":
        return {"chart": {"chart_type": "none"}, "messages": messages}
    
    try:
        raw_data = chart_info.get("raw_data", [])
        chart_type = chart_info.get("chart_type", "none")
        x_axis = chart_info.get("x_axis")
        y_axis = chart_info.get("y_axis")
        
        viz_tool = create_visualizer()
        chart = viz_tool(
            data=raw_data,
            chart_type=chart_type,
            x_axis=x_axis,
            y_axis=y_axis,
            title=state.get("query", "Query Results")[:50]
        )
        
        return {"chart": chart, "messages": messages}
    except Exception as e:
        print(f"❌ Visualizer error: {e}")
        return {"chart": {"chart_type": "none"}, "messages": messages}


def classify_intent(state: AgentState):
    """Use LLM to intelligently classify query intent (no hardcoded patterns)."""
    from langchain_core.prompts import ChatPromptTemplate
    
    query = state["query"].strip()
    previous_context = state.get("conversation_context")
    
    # Fast Path removed as per USER request - now using semantic classification




    # Build context-aware prompt
    context_info = ""
    if previous_context:
        context_info = f"\nPrevious question: {previous_context.get('prev_query', '')}\nPrevious result: {previous_context.get('prev_result_summary', '')}"
    
    classify_prompt = ChatPromptTemplate.from_messages([
        ("system", """Classify the user query into ONE of these categories:
- chat: Greetings or non-data conversation.
- followup: Queries about previous results.
- multi_question: Multiple distinct questions.
- sql: Data/analytic questions.
- reset_context: Clearing history/topics.

CRITICAL: If the query is NOT about fleet orders, database stats, or greetings, classify it as 'chat'.

Also detect if the user explicitly wants a chart/graph/visualization.

Respond in JSON format: {{"intent": "category", "is_viz": true/false}}"""),
        ("human", "Query: {query_text}")
    ])
    
    response = shared_llm.invoke(classify_prompt.format_prompt(
        query_text=f"{query}{context_info}"
    ).to_messages())
    content = response.content.strip()
    
    # Robust JSON extraction
    try:
        if "```json" in content:
            content = content.split("```json")[-1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[-1].split("```")[0].strip()
            
        data = json.loads(content)
        intent = data.get("intent", "sql").lower()
        is_viz = data.get("is_viz", False)
    except Exception as e:
        print(f"⚠️ Classifier JSON parse error: {e} | Content: {content}")
        # Fallback to simple keyword check if JSON fails
        intent = "sql"
        if "chat" in content.lower() or "hello" in content.lower(): intent = "chat"
        elif "followup" in content.lower(): intent = "followup"
        is_viz = any(k in query.lower() for k in ["chart", "plot", "graph", "visualize"])
    
    return {
        "intent_type": intent,
        "is_viz_request": is_viz,
        "question_index": 0
    }


def handle_followup(state: AgentState):
    """Handle follow-up questions that reference previous results."""
    from langchain_core.prompts import ChatPromptTemplate
    
    query = state["query"].strip()
    prev_context = state.get("conversation_context", {})
    
    if not prev_context:
        return {"intent_type": "sql"}
    
    is_viz_request = state.get("is_viz_request", False)
    
    if is_viz_request:
        print(f"📊 Detected visualization request: {query}")
        # Return the previous results so they can be visualized with the proper chart
        return {
            "intent_type": "sql",
            "query": prev_context.get("prev_query", ""),
            "results": state.get("results", []),  # Use existing results
            "skip_execution": True,  # Don't re-execute the SQL
            "conversation_context": prev_context
        }
    
    # Use LLM to understand what the follow-up means
    followup_prompt = ChatPromptTemplate.from_messages([
        ("system", """You are a query refinement assistant. The user is asking a follow-up question about previous database results.

Previous Query: {prev_query}
Previous Results Summary: {prev_summary}
User Follow-up: {followup_query}

Determine what the user wants:
1. "expand" - More rows of the same data (show more results, all results)
2. "drill_down" - More details of specific result (show breakdown, details)
3. "filter" - Apply additional filters or conditions
4. "new_question" - Actually a new question using previous context
5. "cancel" - User wants to stop, leave it, nevermind, or go back

Respond with ONLY the action type in lowercase."""),
        ("human", "Previous Query: {prev_query}\nPrevious Results: {prev_summary}\nFollow-up: {followup_query}")
    ])
    
    response = shared_llm.invoke(followup_prompt.format_prompt(
        prev_query=prev_context.get('prev_query', ''),
        prev_summary=prev_context.get('prev_result_summary', ''),
        followup_query=query
    ).to_messages())
    
    action = response.content.strip().lower()

    # Handle cancellation
    if "cancel" in action or any(cancel_word in query.lower() for cancel_word in ["leave it", "nevermind", "stop", "forget it"]):
        content = "Okay, I've canceled that follow-up. What else can I help you with?"
        messages = state.get("messages", [])
        messages.append({"role": "user", "content": query})
        messages.append({"role": "assistant", "content": content})
        return {
            "intent_type": "chat",
            "results": [{"response": content}],
            "messages": messages
        }
    
    # Build context instruction to preserve previous query's intent
    context_instruction = f"\n[FOLLOWUP CONTEXT: Apply the SAME filters/scope as the previous question: '{prev_context.get('prev_query', '')}'. The user is asking about results WITHIN that context, not a completely different dataset.]"
    
    # Based on action, refine the query
    if action == "expand":
        refined_query = f"{prev_context.get('prev_query', '')} - show all results, remove any limits{context_instruction}"
    elif action == "drill_down":
        refined_query = f"Provide detailed information about: {query} (for the same scope/filters as: {prev_context.get('prev_query', '')}) {context_instruction}"
    elif action == "filter":
        # Make filter requests EXPLICIT - tell analyst to add WHERE clause
        refined_query = f"Answer this: {query} (BUT ONLY for the results from: {prev_context.get('prev_query', '')}) - This is a FILTER request, so add the appropriate WHERE clause to the previous query{context_instruction}"
    else:
        refined_query = f"{query} (within the dataset from: {prev_context.get('prev_query', '')}){context_instruction}"
    
    return {
        "intent_type": "sql",
        "query": refined_query,
        "conversation_context": prev_context
    }


def refactor_query(state: AgentState):
    """Intelligently rephrase query if it failed or needs clarification."""
    from langchain_core.prompts import ChatPromptTemplate
    
    original = state.get("original_query") or state["query"]
    current = state["query"]
    error = state.get("error", "")
    results = state.get("results", [])
    
    # Prepare context about what went wrong
    issue_context = ""
    if error:
        issue_context = f"\nError received: {error}"
    elif not results:
        issue_context = "\nQuery returned no results - may need clearer phrasing"
    elif len(results) > 1000:
        issue_context = "\nQuery returned too many results - may be too broad"
    
    refactor_prompt = ChatPromptTemplate.from_messages([
        ("system", """You are a query refinement expert. The user's database question didn't get the desired results.

Original Query: {original}
{issue_context}

Your task: Rephrase the query to be clearer, more specific, or use different terminology that might help the database understand it better.
Guidelines:
- Be specific about what tables/data you're looking for
- Use explicit column names or descriptions
- Add clarity about time periods if relevant
- Change phrasing to be more database-friendly

Respond with ONLY the refined query, nothing else. No quotes or explanation."""),
        ("human", "Refactor this query given the context above.")
    ])
    
    response = shared_llm.invoke(refactor_prompt.format_prompt(
        original=original,
        issue_context=issue_context
    ).to_messages())
    
    refactored = response.content.strip().strip('"\'')
    attempts = state.get("refactor_attempts", 0)
    
    print(f"\n🔄 QUERY REFACTOR (Attempt #{attempts + 1})")
    print(f"   Original: {original}")
    print(f"   Refactored: {refactored}")
    
    # Prevent infinite refactoring loops
    if attempts >= 2:
        return {
            "error": "Multiple refactoring attempts failed. Please try a different question.",
            "needs_refactor": False
        }
    
    return {
        "query": refactored,
        "original_query": original,
        "refactor_attempts": attempts + 1,
        "needs_refactor": False,
        "results": [],  # Clear results so we process the refactored query
        "error": None,
        "sql": ""
    }


def handle_chat(state: AgentState):
    """Handle general chat queries."""
    from langchain_core.prompts import ChatPromptTemplate
    
    # Fast Path for greetings
    greetings = ["hello", "hi", "hey", "hola", "greetings", "good morning", "good afternoon", "good evening", "howdy"]
    if state["query"].strip().lower() in greetings:
        content = f"Hello! I'm your Fleet Orders assistant. How can I help you today?"
    else:
        chat_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a Fleet Orders assistant. You ONLY answer questions about the Fleet Orders database, fleet management, and order processing workflows.
            
If the user asks about anything else (e.g., general knowledge, celebrities, other businesses, politics, etc.), you MUST respond with EXACTLY this message:
"My data and knowledge is limited to my database "

Do not provide ANY other information for out-of-scope questions.
For on-scope conversational questions, answer briefly and naturally."""),
            ("human", "{query_text}")
        ])
        
        response = shared_llm.invoke(chat_prompt.format_prompt(query_text=state["query"]).to_messages())
        content = response.content
    
    # Add to messages (chat history)
    messages = state.get("messages", [])
    messages.append({"role": "user", "content": state["query"]})
    messages.append({"role": "assistant", "content": content})
    
    return {
        "intent_type": "chat",
        "results": [{"response": content}],
        "messages": messages
    }


def handle_context_reset(state: AgentState):
    """Handle explicit context reset user queries."""
    messages = state.get("messages", [])
    
    response = "Okay, I've cleared the previous conversation context. What would you like to talk about next?"
    
    messages.append({"role": "user", "content": state["query"]})
    messages.append({"role": "assistant", "content": response})
    
    return {
        "intent_type": "reset_context",
        "results": [{"response": response}],
        "conversation_context": None,  # Explicitly clear context
        "messages": messages
    }


def split_questions(state: AgentState):
    """Use LLM to intelligently split multi-part questions."""
    from langchain_core.prompts import ChatPromptTemplate
    
    query = state["query"]
    
    split_prompt = ChatPromptTemplate.from_messages([
        ("system", """You are a question analyzer. Break down multi-part questions into separate, independent questions.

Respond with a JSON array of questions. Example:
["First question here?", "Second question here?", "Third question here?"]

If it's a single question, return: ["The question here?"]

Respond ONLY with valid JSON, nothing else."""),
        ("human", "Break down: {query_text}")
    ])
    
    response = shared_llm.invoke(split_prompt.format_prompt(query_text=query).to_messages())
    
    try:
        questions = json.loads(response.content)
        if not isinstance(questions, list) or len(questions) == 0:
            questions = [query]
    except:
        questions = [query]
    
    # If only one question, proceed as normal SQL
    if len(questions) == 1:
        return {
            "intent_type": "sql",
            "query": questions[0],
            "pending_questions": None,
            "question_index": 0
        }
    
    # Multiple questions detected
    return {
        "intent_type": "sql",
        "query": questions[0],
        "pending_questions": questions[1:],
        "question_index": 0
    }


def process_pending_questions(state: AgentState):
    """Process the next pending question in a multi-question sequence."""
    pending = state.get("pending_questions", [])
    
    if not pending or len(pending) == 0:
        return {"pending_questions": None, "question_index": 0}
    
    # Get next question
    next_question = pending[0]
    remaining = pending[1:]
    
    return {
        "query": next_question,
        "pending_questions": remaining if remaining else None,
        "question_index": state.get("question_index", 0) + 1
    }


def analyze_result_structure(results: List[dict]) -> dict:
    """
    Analyze result data structure to understand column types and patterns.
    Returns insights without any hardcoding - everything data-driven.
    """
    if not results:
        return {"column_types": {}, "distinct_values": {}, "patterns": []}
    
    analysis = {
        "column_types": {},      # numeric, string, date, other
        "distinct_values": {},   # count of unique values per column
        "patterns": []           # detected patterns like "categorical"
    }
    
    # Analyze each column based on actual data
    all_columns = set()
    for row in results:
        all_columns.update(row.keys())
    
    for col in all_columns:
        values = [row.get(col) for row in results if col in row]
        if not values:
            continue
        
        # Determine column type from actual values - no hardcoding
        first_val = values[0]
        if isinstance(first_val, (int, float)):
            analysis["column_types"][col] = "numeric"
        elif hasattr(first_val, 'year'):  # Actual datetime/date object from PostgreSQL
            analysis["column_types"][col] = "date"
        elif isinstance(first_val, str):
            # Check if it looks like a date/timestamp by looking at actual content
            val_lower = str(first_val).lower()
            is_date = (
                any(year in val_lower for year in ['2024', '2025', '2026', '2023', '2022']) or
                any(marker in val_lower for marker in ['-', 'T', ':', 'Z']) or
                any(period in val_lower for period in ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 
                                                        'jul', 'aug', 'sep', 'oct', 'nov', 'dec'])
            )
            analysis["column_types"][col] = "date" if is_date else "string"
        else:
            analysis["column_types"][col] = "other"
        
        # Count unique values - if very few, it's categorical
        distinct_count = len(set(str(v) for v in values if v is not None))
        analysis["distinct_values"][col] = distinct_count
        
        # Detect categorical columns (very few distinct values relative to total rows)
        if analysis["column_types"][col] == "string" and distinct_count <= min(10, len(results) // 2):
            analysis["patterns"].append(f"{col}:categorical")
    
    return analysis


def generate_text_summary(query: str, results: List[dict], conversation_context: dict = None) -> str:
    """
    Generate natural language summary using LLM.
    Completely data-driven - analyzes actual result structure, no hardcoded logic.
    """
    if not results:
        return "No records found matching your criteria."
    
    # For single row with single column (COUNT queries, etc)
    if len(results) == 1:
        row = results[0]
        if len(row) == 1:
            key, val = list(row.items())[0]
            return f"There are **{val}** {key.lower()}."
    

    
    # For large datasets, use LLM with smart structure analysis
    try:
        # Analyze result structure without any hardcoding
        analysis = analyze_result_structure(results)
        
        # Build context string
        context_str = ""
        if conversation_context and conversation_context.get("prev_query"):
            context_str = f"\nPrevious question was: '{conversation_context['prev_query']}'\nPrevious result: {conversation_context.get('prev_result_summary', 'N/A')}"
        
        # Prepare structure insights for LLM
        structure_info = f"""
RESULT STRUCTURE (detected automatically from data):
- Total records: {len(results)}
- Columns: {list(results[0].keys())}
- Column types: {analysis['column_types']}
- Distinct values per column: {analysis['distinct_values']}
- Detected patterns: {', '.join(analysis['patterns']) if analysis['patterns'] else 'None'}
"""
        
        # If categorical columns found, suggest breakdown
        categorical_suggestion = ""
        categorical_cols = [col for col, pattern in 
                          [(c, p) for c in analysis['column_types'].keys() 
                           for p in analysis['patterns']] 
                          if 'categorical' in pattern]
        
        if categorical_cols and len(results) > 5:
            # Calculate actual distribution for LLM reference
            from collections import Counter
            for cat_col in categorical_cols[:1]:  # First categorical column
                distribution = Counter(row.get(cat_col) for row in results if cat_col in row)
                categorical_suggestion = f"\n\nDETECTED CATEGORICAL COLUMN: '{cat_col}'\nDistribution: {dict(distribution)}\nConsider summarizing these counts in natural language."
        
        # Smarter data window: if results are few, send them all. 
        # If many, send a representative sample.
        data_window = results if len(results) <= 20 else results[:10]
        
        prompt = f"""User asked: "{query}"{context_str}{structure_info}{categorical_suggestion}

RECORDS FOUND ({len(results)} total):
{json.dumps(data_window, indent=2, default=str)}

Write a natural, professional response that DIRECTLY ANSWERS the user's question.
Guidelines:
1. If the user asked to "list" or "show all" and there are fewer than 20 records, LIST THEM ALL.
2. If there are many records, summarize the key patterns and mention the total count.
3. Use natural English (ChatGPT style).
4. If categorical distributions are relevant, include them (e.g., "71 documents: 35 Processing, 25 Completed").
5. DO NOT say "Top 5" or "Sample" if you are showing all relevant records.

Respond ONLY with the summary, no extra text."""
        
        response = shared_llm.invoke(prompt)
        summary = response.content.strip()
        
        return summary
    except Exception as e:
        print(f"⚠️ Summary generation error: {e}")
        return f"Found **{len(results)} records** matching your query."

def format_response(state: AgentState):
    """
    Intelligently convert SQL results to natural language summaries (ChatGPT-style).
    Never returns raw tables - always returns formatted text + optional chart.
    """
    query = state.get("query", "").lower()
    results = state.get("results", [])
    conversation_context = state.get("conversation_context", {})
    
    # Preserve messages through the graph
    messages = state.get("messages", [])
    
    if not results or state.get("intent_type") == "chat":
        return {"messages": messages}
    
    # --- SINGLE VALUE QUERIES (e.g., "How many?" "What model?") ---
    if len(results) == 1 and len(results[0]) == 1:
        value = list(results[0].values())[0]
        key = list(results[0].keys())[0]
        
        # Use LLM to generate a MEANINGFUL response based on the user's actual question
        try:
            summary_prompt = f"""The user asked: "{state.get('query', '')}"
The database returned: {key} = {value}

Write ONE clear, professional sentence that DIRECTLY ANSWERS the user's question with this value.
- Use the actual number/value in your response
- Make it sound natural and conversational (like ChatGPT)
- Connect the value to what they asked about
- Do NOT just list the column name

Example: If they asked "How many orders completed?" and you got count=150, say "150 orders were completed last month." NOT "There are 150 count."

Respond ONLY with the sentence, no explanation."""
            
            response = shared_llm.invoke(summary_prompt)
            summary = response.content.strip()
        except Exception as e:
            print(f"⚠️ LLM response generation error: {e}")
            # Fallback: at least create a sensible default
            summary = f"The result is **{value}**."
        
        return {
            "results": [{"summary": summary, "type": "single_value"}],
            "chart": {"chart_type": "none"},
            "messages": messages
        }
    
    # --- SINGLE ROW, MULTIPLE COLUMNS (e.g., "Show me order #123" OR summary stats) ---
    if len(results) == 1 and len(results[0]) > 1:
        row = results[0]
        
        # Check if user explicitly asked for charts
        requesting_chart = state.get("is_viz_request", False)
        
        # For ANY single row with multiple columns, use LLM to write professional summary
        # Don't hardcode formatting - let LLM understand the data semantically
        try:
            # Prepare data for LLM
            data_pairs = []
            for key, val in row.items():
                if isinstance(val, float) and val > 100:
                    data_pairs.append(f"{key}: {val:,.2f}")
                elif isinstance(val, (int, float)):
                    data_pairs.append(f"{key}: {val}")
                elif isinstance(val, str) and val.startswith('http'):
                    data_pairs.append(f"{key}: [Link]({val})")
                else:
                    data_pairs.append(f"{key}: {val}")
            
            data_text = ", ".join(data_pairs)
            
            # Use LLM to convert to professional natural language
            summary_prompt = f"""Convert this database result into a natural, professional sentence:
User Query: {query}
Data: {data_text}

Write ONE clear, concise sentence summarizing this data (like ChatGPT would).
- Understand what the columns mean (e.g., "organization_code" is an organization identifier, "order_count" is a count of orders)
- Write a complete sentence in natural English
- Don't just list the values - write it as a professional summary

Respond ONLY with the sentence, no explanation."""
            
            response = shared_llm.invoke(summary_prompt)
            summary = response.content.strip()
        except Exception as e:
            print(f"⚠️ LLM formatting error: {e}")
            # Fallback: simple natural formatting
            lines = ["Here's the information:\n"]
            for key, val in row.items():
                if isinstance(val, str) and val.startswith('http'):
                    lines.append(f"• **{key}**: [View]({val})")
                else:
                    lines.append(f"• **{key}**: {val}")
            summary = "\n".join(lines)
        
        # If user requested charts, create a relevant chart from the single row
        chart_type = "none"
        requesting_chart = state.get("is_viz_request", False)
        if requesting_chart and any(isinstance(v, (int, float)) for v in row.values()):
            # Convert row to chartable format
            chart_data = []
            for key, val in row.items():
                if isinstance(val, (int, float)):
                    chart_data.append({"metric": str(key), "value": val})
            
            if chart_data:
                chart_type = "pie" if "pie" in query.lower() else "bar"
                return {
                    "results": [{"summary": summary, "type": "single_record"}],
                    "chart": {
                        "chart_type": chart_type,
                        "x_axis": "metric",
                        "y_axis": "value",
                        "raw_data": chart_data
                    },
                    "messages": messages
                }
        
        return {
            "results": [{"summary": summary, "type": "single_record"}],
            "chart": {"chart_type": "none"},
            "messages": messages
        }
    
    # --- MULTIPLE ROWS, SINGLE COLUMN (e.g., "List all models") ---
    if len(results) > 1 and len(results[0]) == 1:
        values = [list(r.values())[0] for r in results]
        col_name = list(results[0].keys())[0]
        
        if len(values) <= 5:
            items = ", ".join([f"**{v}**" for v in values])
            summary = f"Found {len(values)} {col_name.lower()}: {items}"
        else:
            top_items = ", ".join([f"**{v}**" for v in values[:5]])
            rest_items = ", ".join([f"**{v}**" for v in values[5:]])
            summary = (
                f"Found {len(values)} {col_name.lower()}. Top 5: {top_items}"
                f" (and {len(values) - 5} more)||MORE||{rest_items}"
            )
        
        # Check if user explicitly asked for charts
        requesting_chart = state.get("is_viz_request", False)
        
        chart_type = "none"
        x_axis = None
        y_axis = None
        
        # If user explicitly requested charts, create a simple chart showing count
        if requesting_chart and len(values) >= 2:
            # Convert single column to chartable format: value -> count
            from collections import Counter
            value_counts = Counter(values)
            chart_data = [{"name": str(k), "count": v} for k, v in value_counts.most_common()]
            
            if len(chart_data) <= 20:  # Only chart if reasonable number of unique values
                chart_type = "pie" if "pie" in query.lower() else "bar"
                x_axis = "name"
                y_axis = "count"
                # Store processed data for visualization
                return {
                    "results": [{"summary": summary, "type": "multi_values"}],
                    "chart": {
                        "chart_type": chart_type,
                        "x_axis": x_axis,
                        "y_axis": y_axis,
                        "raw_data": chart_data
                    },
                    "messages": messages
                }
        
        return {
            "results": [{"summary": summary, "type": "multi_values"}],
            "chart": {"chart_type": "none"},
            "messages": messages
        }
    
    # --- MULTIPLE ROWS, MULTIPLE COLUMNS (Complex data - summarize with context) ---
    else:
        summary = generate_text_summary(query, results, conversation_context)
        
        # Check if user explicitly asked for charts
        requesting_chart = state.get("is_viz_request", False)
        
        # Smart chart type detection - data-driven, no hardcoding
        chart_type = "none"
        x_axis, y_axis = None, None
        
        # Use structure analysis to decide on chart
        analysis = analyze_result_structure(results)
        
        # Check explicit user chart requests first
        query_lower = query.lower()
        explicit_pie = "pie" in query_lower
        explicit_bar = "bar" in query_lower
        explicit_line = "line" in query_lower or "trend" in query_lower
        explicit_table = "table" in query_lower
        
        print(f"\n📊 CHART DETECTION DEBUG:")
        print(f"   Results: {len(results)} rows, {len(results[0]) if results else 0} columns")
        print(f"   Explicit user requests - Pie: {explicit_pie}, Bar: {explicit_bar}, Line: {explicit_line}, Table: {explicit_table}")
        
        # Identify column types
        numeric_cols = [col for col, typ in analysis['column_types'].items() if typ == "numeric"]
        string_cols = [col for col, typ in analysis['column_types'].items() if typ == "string"]
        date_cols = [col for col, typ in analysis['column_types'].items() if typ == "date"]
        categorical_cols = [col for col, pattern in 
                          [(c, p) for c in analysis['column_types'].keys() 
                           for p in analysis['patterns']] 
                          if 'categorical' in pattern]
        
        # Broaden categorical search if none natively detected but strings exist
        if not categorical_cols and string_cols:
             categorical_cols = string_cols
             
        # Only create chart if data is meaningful
        if len(results) >= 2:
            
            # EXPLICIT OVERRIDES (User asked for a specific chart)
            if explicit_table:
                chart_type = "table"
                
            elif explicit_pie and (categorical_cols or string_cols) and numeric_cols:
                chart_type = "pie"
                x_axis = categorical_cols[0] if categorical_cols else string_cols[0]
                y_axis = numeric_cols[0]
                print(f" EXPLICIT OVERRIDE: Forced PIE CHART: {x_axis} (X) vs {y_axis} (Y)")
                
            elif explicit_bar and (categorical_cols or string_cols) and numeric_cols:
                chart_type = "bar"
                x_axis = categorical_cols[0] if categorical_cols else string_cols[0]
                y_axis = numeric_cols[0]
                print(f" EXPLICIT OVERRIDE: Forced BAR CHART: {x_axis} (X) vs {y_axis} (Y)")
                
            elif explicit_line and date_cols and numeric_cols:
                chart_type = "line"
                x_axis = date_cols[0]
                y_axis = numeric_cols[0]
                print(f" EXPLICIT OVERRIDE: Forced LINE CHART: {x_axis} (X) vs {y_axis} (Y)")
                
            # FALLBACK TO AUTO-DETECTION if no specific explicit override triggered
            elif numeric_cols:
                # 1. TIME SERIES: Date column + Numeric value (HIGHEST PRIORITY)
                if date_cols and numeric_cols and len(results) <= 50:
                    chart_type = "line"
                    x_axis = date_cols[0]
                    y_axis = numeric_cols[0]
                    print(f"AUTO: TIME-SERIES CHART: {x_axis} (X) vs {y_axis} (Y)")
                
                # 2. CATEGORICAL BAR: String category + Numeric value
                elif categorical_cols and numeric_cols and len(results) <= 30:
                    chart_type = "bar"
                    x_axis = categorical_cols[0]
                    y_axis = numeric_cols[0]
                    print(f"AUTO: CATEGORICAL BAR CHART: {x_axis} (X) vs {y_axis} (Y)")
                
                # 3. DISTRIBUTION PIE: Multiple categories with counts 
                elif categorical_cols and len(categorical_cols) >= 1 and numeric_cols and len(results) <= 8:
                    chart_type = "pie"
                    x_axis = categorical_cols[0]
                    y_axis = numeric_cols[0]
                    print(f" AUTO: DISTRIBUTION PIE CHART: {x_axis} (X) vs {y_axis} (Y)")
                else:
                    if requesting_chart and (numeric_cols or string_cols):
                        print(f" User requested chart but AUTO match failed - using table visualization")
                        chart_type = "table"
                    else:
                        print(f"❌ No chart detected - insufficient data match")
            else:
                # No numeric cols, but have data, default to table if they specifically wanted a visual
                if requesting_chart or explicit_table:
                    chart_type = "table"
                    print(f"⚠️ No numeric columns available for graphing. Defaulting to table.")
        else:
            # Not enough rows
            if requesting_chart or explicit_table:
                chart_type = "table"
            print(f"❌ Not enough chartable data ({len(results)} rows)")
        
        return {
            "results": [{"summary": summary, "type": "summary"}],
            "chart": {
                "chart_type": chart_type,
                "x_axis": x_axis,
                "y_axis": y_axis,
                "raw_data": results[:20]  # Pass up to 20 rows for visualization
            },
            "messages": messages
        }


def save_conversation_context(state: AgentState):
    """Save current query and formatted response to chat history."""
    messages = state.get("messages", [])
    query = state.get("query", "")
    results = state.get("results", [])
    conversation_context = state.get("conversation_context", {})
    
    # Add current query to messages (if not already added by handle_chat)
    if query and not any(msg.get("content") == query and msg.get("role") == "user" for msg in messages):
        messages.append({"role": "user", "content": query})
    
    # Generate summary ONCE and reuse it (avoid 3x LLM calls)
    response_summary = ""
    if results:
        response_summary = generate_text_summary(query, results, conversation_context)
        messages.append({"role": "assistant", "content": response_summary})
    
    return {
        "messages": messages,
        "conversation_context": {
            "prev_query": state.get("query", ""),
            "prev_sql": state.get("sql", ""),
            "prev_result_summary": response_summary if response_summary else "No results",
            "prev_results_count": len(state.get("results", []))
        }
    }


def route_after_executor(state):
    """Check if query failed and needs refactoring."""
    if state.get("needs_refactor"):
        return "refactor"
    if state.get("error"):
        # Only auto-refactor if no results, not if there's an error msg
        if not state.get("results"):
            # Don't refactor if we've already hit the attempt limit
            if state.get("refactor_attempts", 0) >= 2:
                return "context_saver"
            return "refactor"
    return "context_saver"


def route_after_classify(state):
    intent_type = state.get("intent_type", "sql")
    if intent_type == "chat":
        return "chat_handler"
    elif intent_type == "followup":
        return "followup_handler"
    elif intent_type == "multi_question":
        return "question_splitter"
    elif intent_type == "reset_context":
        return "context_reset_handler"
    else:
        return "schema_fetcher"


def route_after_followup(state):
    if state.get("intent_type") == "chat":
        return END
    return "schema_fetcher"


def route_after_visualizer(state):
    """Check if there are pending questions to process."""
    pending = state.get("pending_questions")
    if pending and len(pending) > 0:
        return "process_pending"
    else:
        return END


workflow = StateGraph(AgentState)

# Add all nodes
workflow.add_node("classifier", classify_intent)
workflow.add_node("chat_handler", handle_chat)
workflow.add_node("context_reset_handler", handle_context_reset)
workflow.add_node("followup_handler", handle_followup)
workflow.add_node("question_splitter", split_questions)
workflow.add_node("process_pending", process_pending_questions)
workflow.add_node("schema_fetcher", call_schema_fetcher)
workflow.add_node("analyst", call_analyst)
workflow.add_node("executor", call_validator_executor)
workflow.add_node("refactor", refactor_query)  # Auto-rephrase on failure
workflow.add_node("context_saver", save_conversation_context)
workflow.add_node("formatter", format_response)
workflow.add_node("visualizer", call_visualizer)

# Set entry point
workflow.set_entry_point("classifier")

# Route based on intent type
workflow.add_conditional_edges("classifier", route_after_classify)

# Chat goes directly to end
workflow.add_edge("chat_handler", END)

# Context reset goes directly to end
workflow.add_edge("context_reset_handler", END)

# Follow-up questions route to SQL processing or END if canceled
workflow.add_conditional_edges("followup_handler", route_after_followup)

# Question splitter leads to SQL processing
workflow.add_edge("question_splitter", "schema_fetcher")

# SQL processing pipeline
workflow.add_edge("schema_fetcher", "analyst")
workflow.add_edge("analyst", "executor")
workflow.add_conditional_edges("executor", route_after_executor)  # Check if refactoring needed

# Refactor path - rephrase query and retry
workflow.add_edge("refactor", "schema_fetcher")

# After refactor succeeds, continue to context saving
workflow.add_edge("context_saver", "formatter")
workflow.add_edge("formatter", "visualizer")

# Check for pending questions after visualization
workflow.add_conditional_edges("visualizer", route_after_visualizer)

# Process pending questions
workflow.add_edge("process_pending", "schema_fetcher")

app = workflow.compile()

if __name__ == "__main__":
    inputs = {
        "query": "How many multi-orders had extraction failures yesterday?",
        "messages": []
    }
    for output in app.stream(inputs):
        print(output)