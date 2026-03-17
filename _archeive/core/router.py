from pydantic import BaseModel, Field
from typing import Literal
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from src.agents.sql_analyst import SQLAnalyst
from src.agents.researcher import create_researcher

class RouteDecision(BaseModel):
    path: Literal["sql_analyst", "researcher", "both"] = Field(...)
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = Field(...)


def create_router():
    llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)
    structured_llm = llm.with_structured_output(RouteDecision)

    system_prompt = """You route queries intelligently:
    - SQL_ANALYST: Numbers, counts, trends, comparisons ("total tokens", "top orders")
    - RESEARCHER: Process explanations, industry advice ("why spikes?", "best practices") 
    - BOTH: When explanation + data needed

    Be precise - wrong routing wastes compute."""

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("human", "{query}")
    ])

    return prompt | structured_llm
