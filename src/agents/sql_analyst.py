import json
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.rate_limiters import InMemoryRateLimiter
import yaml
import re
from config.prompts import SQL_ANALYST_SYSTEM_PROMPT
import pandas as pd

# Conservative rate limiting to avoid 429 errors
pacer = InMemoryRateLimiter(
    requests_per_second=2,  # Reduced from 10 to avoid quota issues
    check_every_n_seconds=0.5,
    max_bucket_size=3
)
current_time_ist = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') + " IST"

class SQLAnalyst:
    _reserved_keywords_cache = None
    
    def __init__(self, llm=None):
        self.llm = llm or ChatGoogleGenerativeAI(
            model="gemini-2.0-flash", 
            temperature=0,
            rate_limiter=pacer,
            max_retries=5,
            timeout=30)
        with open("config/context.yaml", "r") as f:
            self.biz_logic = yaml.safe_load(f)
    
    @staticmethod
    def creserved_keywords():
        """Dynamically fetch PostgreSQL reserved keywords from database."""
        if SQLAnalyst._reserved_keywords_cache is not None:
            return SQLAnalyst._reserved_keywords_cache
        
        try:
            from src.utils.db_engine import FleetDB
            db = FleetDB()
            
            # Query PostgreSQL for reserved words
            query = """
            SELECT word FROM pg_get_keywords() 
            WHERE catcode = 'R';  -- R = reserved keyword
            """
            
            with db.engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(text(query))
                keywords = set(row[0].lower() for row in result.fetchall())
                
            SQLAnalyst._reserved_keywords_cache = keywords
            return keywords
        except Exception as e:
            print(f"⚠️ Could not fetch reserved keywords from DB: {e}")
            # Fallback to empty set - let validation catch errors instead
            return set()
    
    def _fix_table_alias_references(self, sql: str) -> str:
        alias_pattern = r'(\b\w+\b\.?\w*?)\s+AS\s+(\w+)'
        
        aliases_found = {}
        for match in re.finditer(alias_pattern, sql, re.IGNORECASE):
            table_name = match.group(1).strip()
            alias_name = match.group(2).strip()
            aliases_found[table_name] = alias_name
        
        for table_name, alias_name in aliases_found.items():
            pattern = rf'\b{re.escape(table_name)}(?=\.)'
            sql = re.sub(pattern, alias_name, sql)
        
        return sql
    
    def _fix_reserved_keywords(self, sql: str) -> str:
        for keyword in self.creserved_keywords():
            pattern = rf'\bAS\s+{keyword}\b'
            matches = list(re.finditer(pattern, sql, re.IGNORECASE))
            
            if matches:
                new_alias = f'{keyword}_alias'
                sql = re.sub(pattern, f'AS {new_alias}', sql, flags=re.IGNORECASE)
                
                usage_pattern = rf'\b{keyword}\s*\.'
                if re.search(usage_pattern, sql, re.IGNORECASE):
                    sql = re.sub(usage_pattern, f'{new_alias}.', sql, flags=re.IGNORECASE)
        
        return sql
    
    def _fix_date_filtering(self, sql: str) -> str:
        """Fix incorrect date filtering syntax: move INTERVAL inside parentheses."""
        pattern = r"\(NOW\(\) AT TIME ZONE 'UTC'\)\s*-\s*INTERVAL"
        
        if re.search(pattern, sql, re.IGNORECASE):
            sql = re.sub(
                pattern,
                r"(NOW() AT TIME ZONE 'UTC' - INTERVAL",
                sql,
                flags=re.IGNORECASE
            )
        
        return sql
    
    def _fix_groupby_syntax(self, sql: str) -> str:
        """Fix invalid GROUP BY syntax (aliases, AS clauses in GROUP BY)."""
        # PostgreSQL doesn't support aliases in GROUP BY
        # Remove any "AS alias_name" from GROUP BY clause
        sql = re.sub(
            r'GROUP\s+BY\s+(.*?)(?=\s+ORDER|\s+LIMIT|;|$)',
            lambda m: 'GROUP BY ' + re.sub(r'\s+AS\s+\w+', '', m.group(1), flags=re.IGNORECASE),
            sql,
            flags=re.IGNORECASE | re.DOTALL
        )
        return sql
    
    def _fix_groupby_columns(self, sql: str) -> str:
        """Fix GROUP BY issues: add non-aggregated columns to GROUP BY clause or remove them."""
        # Check if query has GROUP BY
        if not re.search(r'\bGROUP\s+BY\b', sql, re.IGNORECASE):
            return sql  # No GROUP BY, no need to fix
        
        # Skip if this looks like a time-series query (DATE_TRUNC, etc)
        # These are usually correct as-is
        if re.search(r'DATE_TRUNC|EXTRACT|TO_CHAR.*date', sql, re.IGNORECASE):
            return sql
        
        # Extract SELECT clause columns
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return sql
        
        select_clause = select_match.group(1)
        
        # Extract GROUP BY columns
        groupby_match = re.search(r'GROUP\s+BY\s+(.*?)(?:\s+ORDER|\s+LIMIT|;|$)', sql, re.IGNORECASE | re.DOTALL)
        if not groupby_match:
            return sql
        
        groupby_clause = groupby_match.group(1)
        
        # Simple aggregates that are safe to keep
        aggregate_funcs = r'\b(COUNT|SUM|AVG|MIN|MAX|STRING_AGG|ARRAY_AGG|BOOL_AND|BOOL_OR)\s*\('
        
        # Find columns in SELECT that are not aggregated
        columns = re.split(r',\s*', select_clause)
        unaggregated = []
        
        for col in columns:
            col = col.strip()
            # Skip if it's an aggregate function
            if re.search(aggregate_funcs, col, re.IGNORECASE):
                continue
            # Skip if it's already in GROUP BY
            if col.lower() in groupby_clause.lower():
                continue
            # Skip if it's '*'
            if col == '*':
                continue
            
            unaggregated.append(col)
        
        # Add unaggregated columns to GROUP BY (only if not time-series)
        if unaggregated and len(unaggregated) < 3:  # Reasonable limit
            # Clean groupby clause and add missing columns
            groupby_cleaned = groupby_clause.strip().rstrip(';')
            cols_to_add = ', '.join(unaggregated)
            new_groupby = f"{groupby_cleaned}, {cols_to_add}"
            
            # Replace the GROUP BY clause
            sql = re.sub(
                r'(GROUP\s+BY\s+).*?(?=\s+ORDER|\s+LIMIT|;|$)',
                f'GROUP BY {new_groupby}',
                sql,
                flags=re.IGNORECASE | re.DOTALL
            )
        
        return sql
    
    def _remove_limit_from_aggregates(self, sql: str) -> str:
        """Remove LIMIT clause from aggregate queries (COUNT, SUM, AVG, etc)."""
        # Check if query is an aggregate - starts with SELECT and contains aggregate functions
        aggregate_pattern = r'SELECT\s+(COUNT|SUM|AVG|MAX|MIN|STRING_AGG|ARRAY_AGG)'
        if re.search(aggregate_pattern, sql, re.IGNORECASE):
            # Remove LIMIT clause if present
            sql = re.sub(r'\s+LIMIT\s+\d+\s*;?\s*$', ';', sql, flags=re.IGNORECASE)
            sql = re.sub(r'\s+LIMIT\s+\d+(?=\s*;)', '', sql, flags=re.IGNORECASE)
        return sql

    def _fix_double_casts(self, sql: str) -> str:
        """Fix double casts like ::text::text → ::text"""
        # Remove duplicate casts (::text::text, ::int::int, etc)
        fixed_sql = re.sub(r'::(\w+)::\1\b', r'::\1', sql, flags=re.IGNORECASE)
        if fixed_sql != sql:
            print(f"🔧 Fixed double casts")
        return fixed_sql
    
    def _fix_case_sensitivity(self, sql: str) -> str:
        """Convert string equals (=) to ILIKE for case-insensitive matching on text columns."""
        # This handles cases where the LLM uses = 'value' for text columns
        # Pattern: column = 'something' where column is likely text
        # Convert to: column ILIKE 'something'
        
        fixed_sql = sql
        
        # Find all WHERE clauses with = 'string' patterns
        # Only for columns that look like codes/names (not IDs or dates)
        pattern = r"(\w+\.?\w*)\s*=\s*'([^']+)'"
        
        for match in re.finditer(pattern, sql):
            col_name = match.group(1)
            value = match.group(2)
            
            # Skip if it's a date/timestamp column
            if any(x in col_name.lower() for x in ['date', 'time', 'created', 'updated', 'at']):
                continue
            
            # Skip if value looks like a number or special character sequence
            if value.replace('.', '').replace('-', '').isdigit():
                continue
            
            # Convert = to ILIKE for case-insensitive matching
            old_condition = f"{col_name} = '{value}'"
            new_condition = f"{col_name} ILIKE '{value}'"
            fixed_sql = fixed_sql.replace(old_condition, new_condition)
            print(f"🔄 Case conversion: {col_name} = '{value}' → ILIKE '{value}'")
        
        return fixed_sql

    def _remove_redundant_where_conditions(self, sql: str) -> str:
        """Remove obviously redundant WHERE conditions (schema-aware)."""
        # Extract WHERE clause
        where_match = re.search(r'WHERE\s+(.*?)(?:\s+(?:ORDER|GROUP|LIMIT|;|$))', sql, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return sql
        
        where_clause = where_match.group(1).strip()
        original_where = where_clause
        
        # Detect CONTRADICTORY conditions that can NEVER be true
        # Pattern: same column with conflicting values (e.g., status = 'Exception' AND status = 'Pending')
        status_matches = re.findall(r"\bstatus::?text\s+ILIKE\s+'([^']+)'", where_clause, re.IGNORECASE)
        if len(status_matches) > 1 and len(set(status_matches)) > 1:
            # Multiple different status values = impossible condition
            print(f"⚠️ CONTRADICTORY STATUS conditions detected: {status_matches}")
            # Remove ALL status conditions and keep others
            where_clause = re.sub(
                r"\s*AND\s+status::?text\s+ILIKE\s+'[^']+'",
                '',
                where_clause,
                flags=re.IGNORECASE
            )
            where_clause = re.sub(
                r"^status::?text\s+ILIKE\s+'[^']+'\s+AND\s+",
                '',
                where_clause,
                flags=re.IGNORECASE
            )
            print(f"🔧 Removed contradictory status filters")
        
        # Check for contradictory MONTH extracts
        month_matches = re.findall(r"EXTRACT\s*\(\s*MONTH\s+FROM\s+\w+\.?\w*\s*\)\s*=\s*(\d+)", where_clause, re.IGNORECASE)
        if len(month_matches) > 1 and len(set(month_matches)) > 1:
            # Multiple different months = impossible
            print(f"⚠️ CONTRADICTORY MONTH conditions detected: {month_matches}")
            where_clause = re.sub(
                r"\s*AND\s+EXTRACT\s*\(\s*MONTH\s+FROM\s+\w+\.?\w*\s*\)\s*=\s*\d+",
                '',
                where_clause,
                flags=re.IGNORECASE
            )
            print(f"🔧 Removed contradictory month filters")
        
        # 1. status::text = 'Exception' AND failure_reason ILIKE '%exception%'
        #    → Keep only status::text = 'Exception'
        if re.search(r'status::text\s*=\s*[\'"]Exception[\'"]', where_clause, re.IGNORECASE):
            # If status is Exception, don't also filter on failure_reason containing 'exception'
            where_clause = re.sub(
                r'\s+AND\s+\w+\.?failure_reason\s+ILIKE\s+[\'"]%exception%[\'"]',
                '',
                where_clause,
                flags=re.IGNORECASE
            )
        
        # 2. Remove duplicate AND conditions
        conditions = [c.strip() for c in where_clause.split(' AND ')]
        unique_conditions = []
        seen_normalized = set()
        
        for cond in conditions:
            # Normalize for comparison (remove extra spaces, convert to lowercase)
            normalized = re.sub(r'\s+', ' ', cond.lower())
            if normalized not in seen_normalized:
                seen_normalized.add(normalized)
                unique_conditions.append(cond)
        
        new_where_clause = ' AND '.join(unique_conditions)
        
        # Show what was removed
        if new_where_clause != original_where:
            print(f"\n🧹 REDUNDANT WHERE CONDITIONS REMOVED:")
            print(f"   Original: {original_where[:80]}...")
            print(f"   Cleaned:  {new_where_clause[:80]}...")
        
        # Replace in SQL
        sql = re.sub(
            r'WHERE\s+.*?(?=\s+(?:ORDER|GROUP|LIMIT|;|$))',
            f'WHERE {new_where_clause}',
            sql,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        return sql

    def _fix_premature_semicolons(self, sql: str) -> str:
        """Remove semicolons that appear inside the query (not at the very end)."""
        # Remove ALL semicolons first
        sql_without_semis = sql.replace(';', '')
        
        # Add ONE semicolon at the very end
        sql = sql_without_semis.rstrip() + ';'
        
        if sql_without_semis != sql.rstrip(';'):
            print(f"🔧 Fixed premature semicolons in the query")
        
        return sql
    
    def _fix_unbalanced_parentheses(self, sql: str) -> str:
        """Fix unbalanced parentheses in the query."""
        # Count opening and closing parentheses
        open_parens = sql.count('(')
        close_parens = sql.count(')')
        
        # If we have more opening than closing, add closing parens
        if open_parens > close_parens:
            missing = open_parens - close_parens
            sql = sql.rstrip(';').rstrip() + ')' * missing + ';'
            print(f"🔧 Fixed {missing} unbalanced opening parenthesis(es)")
        
        # If we have more closing than opening, we have a problem - but remove extras
        elif close_parens > open_parens:
            # Remove extra closing parentheses
            excess = close_parens - open_parens
            # Remove from the end before the semicolon
            sql_nse = sql.rstrip(';').rstrip()
            for _ in range(excess):
                sql_nse = sql_nse.rstrip(')')
            sql = sql_nse + ';'
            print(f"🔧 Removed {excess} extra closing parenthesis(es)")
        
        return sql
    
    def _remove_stray_characters(self, sql: str) -> str:
        """Remove stray characters that shouldn't be in SQL."""
        # Remove stray brackets that aren't part of array syntax
        # Pattern: ) followed by ] or ];
        sql = re.sub(r'\)\s*\];?\s*$', ');', sql)
        sql = re.sub(r'\]\s*;', ';', sql)
        
        return sql
    
    def _remove_unnecessary_groupby(self, sql: str) -> str:
        """Remove GROUP BY if there are no aggregate functions in SELECT."""
        # Check if there's a GROUP BY clause
        if not re.search(r'\bGROUP\s+BY\b', sql, re.IGNORECASE):
            return sql
        
        # Check if SELECT clause contains aggregate functions
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return sql
        
        select_clause = select_match.group(1)
        
        # Aggregate functions that indicate real aggregation
        aggregate_pattern = r'\b(COUNT|SUM|AVG|MIN|MAX|STRING_AGG|ARRAY_AGG|BOOL_AND|BOOL_OR)\s*\('
        has_aggregates = bool(re.search(aggregate_pattern, select_clause, re.IGNORECASE))
        
        # If no aggregates but there's a GROUP BY, remove it
        if not has_aggregates:
            print(f"🔧 Removed unnecessary GROUP BY (no aggregates in SELECT)")
            # Remove the entire GROUP BY clause
            sql = re.sub(
                r'\s+GROUP\s+BY\s+.*?(?=\s+ORDER|\s+LIMIT|;|$)',
                '',
                sql,
                flags=re.IGNORECASE | re.DOTALL
            )
        
        return sql
    
    def _validate_sql(self, sql: str) -> str:
        """Validate and fix SQL query."""
        sql = self._fix_premature_semicolons(sql)  # Fix semicolons FIRST
        sql = self._fix_unbalanced_parentheses(sql)  # Fix parentheses SECOND
        sql = self._remove_stray_characters(sql)  # Remove stray chars THIRD
        sql = self._remove_unnecessary_groupby(sql)  # Remove GROUP BY if no aggregates
        sql = self._fix_table_alias_references(sql)
        sql = self._fix_reserved_keywords(sql)
        sql = self._fix_date_filtering(sql)
        sql = self._fix_groupby_syntax(sql)  # Fix GROUP BY syntax (remove AS aliases)
        sql = self._fix_groupby_columns(sql)
        sql = self._remove_limit_from_aggregates(sql)
        sql = self._fix_double_casts(sql)
        sql = self._fix_case_sensitivity(sql)  # Convert = to ILIKE for text columns
        sql = self._remove_redundant_where_conditions(sql)
        
        pattern = r"(\bstatus\b)(?!\s*::text)"
        if re.search(pattern, sql, re.IGNORECASE):
            sql = re.sub(pattern, r"status::text", sql, flags=re.IGNORECASE)

        return sql

    def generate(self, query: str, schema_info: str, messages: list = None, conversation_context: dict = None):
        formatted_system_prompt = SQL_ANALYST_SYSTEM_PROMPT.format(
            total_consumption_def=self.biz_logic['kpi_definitions']['total_consumption'],
            processing_latency_def=self.biz_logic['kpi_definitions']['processing_latency'],
            extraction_accuracy_def=self.biz_logic['kpi_definitions']['extraction_accuracy'],
            schema_info=schema_info,
        )
        
        # Build human input with conversation history context built-in
        context_str = ""
        if conversation_context and conversation_context.get("prev_query"):
            context_str += f"\nPrevious Question: {conversation_context['prev_query']}"
            context_str += f"\nPrevious SQL used: {conversation_context.get('prev_sql', 'N/A')}"
            context_str += f"\nIMPORTANT: The user is asking a follow-up. Start with the 'Previous SQL' and modify it to answer the new question. Keep the same grouping and selected columns unless the new question explicitly asks for different groupings or columns.\n"
            
        human_input = f"""
        User Query: {query}{context_str}
        CONTEXTUAL DATA:
        - Current Time (User Local): {current_time_ist}
        - Database Storage: UTC
        - Timezone Rule: To get "last week" from now, subtract 7 days from (NOW() AT TIME ZONE 'UTC').
        """
        res = self.llm.invoke([
            ("system", formatted_system_prompt),
            ("human", human_input)
        ])

        sql = res.content.replace("```sql", "").replace("```", "").strip()
        validated_sql = self._validate_sql(sql)
        
        return validated_sql