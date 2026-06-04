from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app.analyst import detect_language
from app.auth import verify_password
from app.db import DatabaseError
from app.sql_safety import UnsafeSQL
from app.tenant_store import (
    User,
    append_user_conversation_memory,
    append_web_chat_message,
    clear_user_conversation_memory,
    clear_web_chat_messages,
    export_restaurant_knowledge_markdown,
    get_dsn_by_id,
    get_restaurant_knowledge,
    get_user_by_email,
    get_user_by_id,
    get_user_conversation_memory,
    list_web_chat_messages,
    list_accessible_restaurants,
    upsert_restaurant_knowledge,
)
from app.vera import VeraQueryExecutionError, answer_with_vera, response_to_web_payload


router = APIRouter(prefix="/api")

SESSION_COOKIE = "vera_session"
SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 7


class LoginRequest(BaseModel):
    email: str
    password: str


class RestaurantSelectRequest(BaseModel):
    restaurant_names: List[str] = Field(default_factory=list)


class ChatRequest(BaseModel):
    message: str
    restaurant_names: Optional[List[str]] = None
    include_debug: bool = False


def _pending_clarification(session: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    pending = session.get("pending_clarification")
    return pending if isinstance(pending, dict) and pending.get("original_question") else None


def _session_secret() -> str:
    return os.getenv("WEB_SESSION_SECRET", "dev-secret-change-me")


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _unb64(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))


def _sign(payload: str) -> str:
    digest = hmac.new(_session_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return _b64(digest)


def _encode_session(data: Dict[str, Any]) -> str:
    payload = _b64(json.dumps(data, separators=(",", ":")).encode("utf-8"))
    return f"{payload}.{_sign(payload)}"


def _decode_session(raw: str) -> Dict[str, Any]:
    try:
        payload, sig = raw.split(".", 1)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="Invalid session.") from exc
    if not hmac.compare_digest(_sign(payload), sig):
        raise HTTPException(status_code=401, detail="Invalid session.")
    data = json.loads(_unb64(payload).decode("utf-8"))
    if int(data.get("iat", 0)) + SESSION_MAX_AGE_SECONDS < int(time.time()):
        raise HTTPException(status_code=401, detail="Session expired.")
    return data


def _set_session_cookie(response: Response, data: Dict[str, Any]) -> None:
    response.set_cookie(
        SESSION_COOKIE,
        _encode_session(data),
        max_age=SESSION_MAX_AGE_SECONDS,
        httponly=True,
        samesite="lax",
        secure=os.getenv("WEB_COOKIE_SECURE", "").lower() in ("1", "true", "yes"),
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE)


def _session_from_request(request: Request) -> Dict[str, Any]:
    raw = request.cookies.get(SESSION_COOKIE)
    if not raw:
        raise HTTPException(status_code=401, detail="Not logged in.")
    return _decode_session(raw)


def _current_user(request: Request) -> tuple[User, Dict[str, Any]]:
    session = _session_from_request(request)
    user = get_user_by_id(int(session["user_id"]))
    if not user:
        raise HTTPException(status_code=401, detail="Session user not found.")
    return user, session


def _serialize_user(user: User) -> Dict[str, Any]:
    return {"id": user.id, "email": user.email, "role": user.role, "dsn_id": user.dsn_id}


def _accessible_names(user: User) -> List[str]:
    return [r["name"] for r in list_accessible_restaurants(user)]


def _validate_restaurants(user: User, names: List[str]) -> List[str]:
    allowed = set(_accessible_names(user))
    selected = [n for n in names if n in allowed]
    if len(selected) != len([n for n in names if n]):
        raise HTTPException(status_code=403, detail="One or more restaurants are not accessible.")
    return list(dict.fromkeys(selected))


def _me_payload(user: User, selected: List[str]) -> Dict[str, Any]:
    dsn = get_dsn_by_id(user.dsn_id) if user.dsn_id else None
    return {
        "user": _serialize_user(user),
        "dsn": {"id": dsn["id"], "name": dsn["name"]} if dsn else None,
        "restaurants": _accessible_names(user),
        "selected_restaurants": selected,
    }


def _chat_failure_payload(
    message: str,
    *,
    include_debug: bool = False,
    error: Optional[str] = None,
    failed_query: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "action": "answer",
        "message": message,
        "tables": [],
        "charts": [],
        "recommendations": [
            "Try narrowing the date range, restaurant scope, or metric so Vera can run a faster query.",
            "Ask for the same analysis by month or by week first, then drill into days or products after the broad pattern is clear.",
        ],
        "suggested_next_questions": [
            "Can you summarize this by month first?",
            "Which part of this analysis is most important: sales, tickets, or average ticket?",
        ],
    }
    if include_debug:
        debug: Dict[str, Any] = {}
        if error:
            debug["error"] = error
        if failed_query:
            debug["failed_query"] = failed_query
            debug["sql"] = failed_query.get("sql")
        if debug:
            payload["debug"] = debug
    return payload


@router.post("/login")
def login(req: LoginRequest, response: Response):
    user = get_user_by_email(req.email.strip())
    if not user or not verify_password(req.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials.")
    session = {"user_id": user.id, "selected_restaurants": [], "iat": int(time.time())}
    _set_session_cookie(response, session)
    return _me_payload(user, [])


@router.post("/logout")
def logout(response: Response):
    _clear_session_cookie(response)
    return {"ok": True}


@router.get("/me")
def me(request: Request):
    user, session = _current_user(request)
    return _me_payload(user, session.get("selected_restaurants") or [])


@router.post("/restaurants/select")
def select_restaurants(req: RestaurantSelectRequest, request: Request, response: Response):
    user, session = _current_user(request)
    selected = _validate_restaurants(user, req.restaurant_names)
    session["selected_restaurants"] = selected
    session["iat"] = int(time.time())
    _set_session_cookie(response, session)
    return _me_payload(user, selected)


@router.post("/memory/reset")
def reset_memory(request: Request):
    user, _session = _current_user(request)
    if user.dsn_id is None:
        raise HTTPException(status_code=400, detail="No DSN assigned.")
    clear_user_conversation_memory(user.id, user.dsn_id)
    clear_web_chat_messages(user.id, user.dsn_id)
    return {"ok": True}


@router.get("/chat/history")
def chat_history(request: Request):
    user, _session = _current_user(request)
    if user.dsn_id is None:
        raise HTTPException(status_code=400, detail="No DSN assigned.")
    return {"messages": list_web_chat_messages(user.id, user.dsn_id, assistant_limit=10)}


@router.post("/chat")
def chat(req: ChatRequest, request: Request, response: Response):
    user, session = _current_user(request)
    if user.dsn_id is None:
        raise HTTPException(status_code=400, detail="No DSN assigned.")
    dsn = get_dsn_by_id(user.dsn_id)
    if not dsn:
        raise HTTPException(status_code=400, detail="DSN not found.")

    restaurants = req.restaurant_names if req.restaurant_names is not None else session.get("selected_restaurants") or []
    restaurants = _validate_restaurants(user, restaurants)
    accessible = _accessible_names(user)
    if not restaurants and len(accessible) == 1:
        restaurants = accessible
    if not restaurants:
        raise HTTPException(status_code=400, detail="Select at least one restaurant.")

    history = get_user_conversation_memory(user.id, user.dsn_id)
    knowledge = get_restaurant_knowledge(user.dsn_id, restaurants)
    pending = _pending_clarification(session)
    question_for_vera = req.message
    if pending:
        question_for_vera = (
            "The user is answering Vera's previous clarification. "
            "Resolve the prior ambiguity and proceed unless the answer is still impossible.\n\n"
            f"Original question: {pending['original_question']}\n"
            f"Vera clarification: {pending.get('clarifying_question', '')}\n"
            f"User clarification answer: {req.message}"
        )
    append_web_chat_message(user.id, user.dsn_id, "user", req.message, selected_restaurants=restaurants)
    try:
        vera = answer_with_vera(
            question_for_vera,
            restaurants=restaurants,
            dsn=dsn["dsn"],
            history=history,
            restaurant_knowledge=knowledge,
            language=detect_language(req.message),
            preview=True,
        )
    except VeraQueryExecutionError as exc:
        session.pop("pending_clarification", None)
        session["selected_restaurants"] = restaurants
        session["iat"] = int(time.time())
        _set_session_cookie(response, session)
        payload = _chat_failure_payload(
            "I could not complete that analysis because the database query timed out. "
            "The safest next step is to narrow the question first, then drill down once we know where the movement is.",
            include_debug=req.include_debug,
            error=str(exc),
            failed_query={
                "purpose": exc.purpose,
                "sql": exc.sql,
                "params": exc.params,
            },
        )
        append_user_conversation_memory(user.id, user.dsn_id, req.message, payload["message"])
        append_web_chat_message(
            user.id,
            user.dsn_id,
            "assistant",
            payload["message"],
            payload=payload,
            selected_restaurants=restaurants,
        )
        return payload
    except DatabaseError as exc:
        session.pop("pending_clarification", None)
        session["selected_restaurants"] = restaurants
        session["iat"] = int(time.time())
        _set_session_cookie(response, session)
        payload = _chat_failure_payload(
            "I could not complete that analysis because the database query timed out. "
            "The safest next step is to narrow the question first, then drill down once we know where the movement is.",
            include_debug=req.include_debug,
            error=str(exc),
        )
        append_user_conversation_memory(user.id, user.dsn_id, req.message, payload["message"])
        append_web_chat_message(
            user.id,
            user.dsn_id,
            "assistant",
            payload["message"],
            payload=payload,
            selected_restaurants=restaurants,
        )
        return payload
    except UnsafeSQL as exc:
        session.pop("pending_clarification", None)
        session["selected_restaurants"] = restaurants
        session["iat"] = int(time.time())
        _set_session_cookie(response, session)
        payload = _chat_failure_payload(
            "I rejected the generated SQL before running it because it did not pass the app's safety checks. "
            "Please rephrase the question with the metric, period, and restaurant scope you want, and I will plan a safer query.",
            include_debug=req.include_debug,
            error=str(exc),
        )
        append_user_conversation_memory(user.id, user.dsn_id, req.message, payload["message"])
        append_web_chat_message(
            user.id,
            user.dsn_id,
            "assistant",
            payload["message"],
            payload=payload,
            selected_restaurants=restaurants,
        )
        return payload
    if vera.action == "clarify":
        session["pending_clarification"] = {
            "original_question": pending["original_question"] if pending else req.message,
            "clarifying_question": vera.message,
            "restaurants": restaurants,
        }
    else:
        session.pop("pending_clarification", None)
    session["selected_restaurants"] = restaurants
    session["iat"] = int(time.time())
    _set_session_cookie(response, session)

    payload = response_to_web_payload(vera, include_debug=req.include_debug)

    if vera.knowledge_to_save:
        for restaurant in restaurants:
            upsert_restaurant_knowledge(user.dsn_id, restaurant, vera.knowledge_to_save)
            updated = get_restaurant_knowledge(user.dsn_id, [restaurant]).get(restaurant, vera.knowledge_to_save)
            export_restaurant_knowledge_markdown(dsn["name"], user.dsn_id, restaurant, updated)

    append_user_conversation_memory(user.id, user.dsn_id, req.message, vera.message)
    append_web_chat_message(
        user.id,
        user.dsn_id,
        "assistant",
        vera.message,
        payload=payload,
        selected_restaurants=restaurants,
    )
    return payload
