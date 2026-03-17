import json
import yaml
import pickle
import os
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from pydantic import BaseModel, Field
from typing import List
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


class SchemaContext(BaseModel):
    relevant_tables: List[str] = Field(...)
    relevant_columns: List[str] = Field(...)
    join_conditions: List[str] = Field(default=[])
    reasoning: str = Field(...)


class SchemaLinker:
    def __init__(self, llm=None):
        # Keep ONLY the LLM setup
        if llm:
            self.llm = llm
        else:
            from langchain_google_genai import ChatGoogleGenerativeAI
            self.llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash")

        # Keep business context ONLY if you use it in the prompt
        # Otherwise, remove this too to save more time
        try:
            with open("config/context.yaml", "r") as f:
                self.business_context = yaml.safe_load(f)
        except FileNotFoundError:
            self.business_context = {}
            print("⚠️ Business context not found, skipping...")
            
            
    def _precompute_embeddings(self):
        embeddings = {}
        for table_name, table_info in self.schema['tables'].items():
            # Fallback if description is missing
            table_desc = table_info.get('description') or "No description provided"

            for col_name in table_info['columns']:
                # 1. Ensur we have valid strings
                if not table_name or not col_name:
                    continue
                text_to_embed = f"Table: {table_name}. Description: {table_desc}. Column: {col_name}"

            if text_to_embed.strip():
                    try:
                        embeddings[f"{table_name}.{col_name}"] = self.embeddings.embed_query(text_to_embed)
                    except Exception as e:
                        print(f"⚠️ Skipped embedding for {table_name}.{col_name}: {e}")

        return embeddings
    
    
    def get_context(self, query: str, db_context: str) -> str:
        # Use the shared_llm you're passing in
        structured_llm = self.llm.with_structured_output(SchemaContext)

        prompt = f"""
            User Query: {query}
            
            DATABASE CONTEXT (Tables, Enums, and Relationships):
            {db_context}
            
            2. BUSINESS RULES & KPI DEFINITIONS:
            {self.business_context}
            
            Task: Identify the specific tables and columns needed for this query.
            Provide the JOIN logic based on the Relationships provided above.
            Use the 'kpi_definitions' and 'sql_rules' from the Business Rules to ensure accuracy.
            """
        context = structured_llm.invoke(prompt)
        return f"Tables: {context.relevant_tables}\nColumns: {context.relevant_columns}\nJoins: {context.join_conditions}"