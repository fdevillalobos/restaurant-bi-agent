from dotenv import load_dotenv
load_dotenv()

import os

from fastapi import FastAPI, HTTPException, Body
from openai import RateLimitError, OpenAIError
from pydantic import BaseModel, Field

from app.query_plan import QueryPlan
from app.sql_builder import build_sql
from app.db import run_select, DatabaseError
from app.sql_safety import UnsafeSQL
from app.introspect import introspect_tables
from app.llm_planner import question_to_plan
from app.fallback_planner import fallback_plan
from app.verbalizer import verbalize_answer


app = FastAPI(title="Restaurant BI Agent (MVP)")


class QueryRequest(BaseModel):
    sql: str = Field(..., description="SELECT-only SQL (Postgres dialect)")
    preview: bool = True

class AskRequest(BaseModel):
    question: str
    restaurant: str
    preview: bool = True

def _pretty_sql(sql: str) -> str:
    return "\n".join(line.rstrip() for line in sql.strip().splitlines())

@app.get("/restaurants")
def restaurants():
    rows = run_select(
        "SELECT restaurant, COUNT(*) AS sales_cnt "
        "FROM sales GROUP BY restaurant ORDER BY sales_cnt DESC LIMIT 200;",
        preview=False
    )
    return {"restaurants": rows}

@app.post("/ask")
def ask(
    question: str = Body(..., media_type="text/plain"),
    include_data: bool = False,
    include_sql: bool = False,
    preview: bool = True,
):
    plan = question_to_plan(question)
    built = build_sql(plan)
    try:
        rows = run_select(
        built.sql,
        params=built.params,
        preview=preview,
        statement_timeout_ms=int(os.getenv("STATEMENT_TIMEOUT_MS_ASK", "30000")),
        )
    except DatabaseError as e:
        # Always return SQL/params if requested, even on DB failures (timeouts, etc.)
        detail = {"db_error": str(e)}
        if include_sql:
            detail["sql"] = built.sql
            detail["params"] = built.params
            detail["plan"] = plan.model_dump()
        raise HTTPException(status_code=500, detail=detail)

    answer = verbalize_answer(question, plan, rows)

    resp = {"message": answer}

    if include_data:
        resp["data"] = rows

    if include_sql:
        resp["sql"] = built.sql
        resp["params"] = built.params
        resp["plan"] = plan.model_dump()

    return resp

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/ask_plan")
def ask_plan(plan: QueryPlan):
    built = build_sql(plan)
    return {"plan": plan.model_dump(), "sql": built.sql, "params": built.params}

@app.get("/introspect")
def introspect(schema: str = "public"):
    return introspect_tables(schema=schema)

@app.post("/run_sql")
def run_sql(req: QueryRequest):
    try:
        rows = run_select(
        req.sql,
        preview=req.preview,
        statement_timeout_ms=int(os.getenv("STATEMENT_TIMEOUT_MS_RUNSQL", "8000")),
        )
        return {"rows": rows, "row_count": len(rows)}
    except UnsafeSQL as e:
        raise HTTPException(status_code=400, detail=f"Unsafe SQL: {e}")
    except DatabaseError as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
