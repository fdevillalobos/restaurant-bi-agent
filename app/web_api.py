from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from typing import Any, Dict, List, Optional

import psycopg
from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app.analyst import detect_language
from app.auth import hash_password, verify_password
from app.db import DatabaseError
from app.sql_safety import UnsafeSQL
from app.tenant_store import (
    ADMIN_ROLES,
    MANAGEABLE_ROLES,
    SCOPED_ADMIN_ASSIGNABLE_ROLES,
    User,
    append_user_conversation_memory,
    append_web_chat_message,
    accept_invite,
    clear_user_conversation_memory,
    clear_web_chat_messages,
    clear_sessions_for_user,
    count_active_superusers,
    create_dsn,
    create_invite,
    export_restaurant_knowledge_markdown,
    get_dsn_by_id,
    get_invite_by_id,
    get_invite_by_token,
    get_restaurant_knowledge,
    get_user_by_email,
    get_user_by_id,
    get_user_conversation_memory,
    list_dsns_safe,
    list_invites,
    list_restaurants_by_dsn,
    list_users,
    list_web_chat_messages,
    list_accessible_restaurants,
    record_admin_audit_event,
    revoke_invite,
    set_user_restaurants,
    sync_restaurants_from_dsn,
    update_dsn,
    update_user_admin_fields,
    upsert_restaurant_knowledge,
)
from app.vera import VeraQueryExecutionError, answer_with_vera, response_to_web_payload


router = APIRouter(prefix="/api")

SESSION_COOKIE = "vera_session"
SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 7


class LoginRequest(BaseModel):
    email: str
    password: str
    language: Optional[str] = None


class RestaurantSelectRequest(BaseModel):
    restaurant_names: List[str] = Field(default_factory=list)


class LanguageRequest(BaseModel):
    language: str


class ChatRequest(BaseModel):
    message: str
    restaurant_names: Optional[List[str]] = None
    include_debug: bool = False
    language: Optional[str] = None


class AdminUserPatch(BaseModel):
    role: Optional[str] = None
    dsn_id: Optional[int] = None
    restaurant_ids: Optional[List[int]] = None
    is_active: Optional[bool] = None


class InviteCreateRequest(BaseModel):
    email: str
    role: str = "user"
    dsn_id: Optional[int] = None
    restaurant_ids: List[int] = Field(default_factory=list)


class DsnCreateRequest(BaseModel):
    name: str
    dsn: str


class DsnPatchRequest(BaseModel):
    name: Optional[str] = None
    dsn: Optional[str] = None


class InviteAcceptRequest(BaseModel):
    password: str


def _pending_clarification(session: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    pending = session.get("pending_clarification")
    return pending if isinstance(pending, dict) and pending.get("original_question") else None


def _normalize_language(language: Optional[str]) -> str:
    value = (language or "").strip().lower()
    if value in {"es", "es-ar", "es-latam", "spanish"}:
        return "es"
    if value in {"en", "en-us", "english"}:
        return "en"
    return "en"


def _chat_language(message: str, session: Dict[str, Any], requested: Optional[str]) -> str:
    detected = detect_language(message)
    if detected == "es":
        return "es"
    return _normalize_language(requested or session.get("language") or "en")


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


def _new_csrf_token() -> str:
    return secrets.token_urlsafe(24)


def _require_csrf(request: Request, session: Dict[str, Any]) -> None:
    expected = str(session.get("csrf_token") or "")
    provided = request.headers.get("X-CSRF-Token", "")
    if not expected or not hmac.compare_digest(expected, provided):
        raise HTTPException(status_code=403, detail="Missing or invalid CSRF token.")


def _current_user(request: Request) -> tuple[User, Dict[str, Any]]:
    session = _session_from_request(request)
    user = get_user_by_id(int(session["user_id"]))
    if not user:
        raise HTTPException(status_code=401, detail="Session user not found.")
    if not user.is_active:
        raise HTTPException(status_code=401, detail="User is deactivated.")
    return user, session


def _serialize_user(user: User) -> Dict[str, Any]:
    return {"id": user.id, "email": user.email, "role": user.role, "dsn_id": user.dsn_id, "is_active": user.is_active}


def _accessible_names(user: User) -> List[str]:
    return [r["name"] for r in list_accessible_restaurants(user)]


def _auto_selected_restaurants(user: User, selected: Optional[List[str]]) -> List[str]:
    current = [name for name in (selected or []) if name]
    if current:
        return current
    accessible = _accessible_names(user)
    return accessible[:1]


def _validate_restaurants(user: User, names: List[str]) -> List[str]:
    allowed = set(_accessible_names(user))
    selected = [n for n in names if n in allowed]
    if len(selected) != len([n for n in names if n]):
        raise HTTPException(status_code=403, detail="One or more restaurants are not accessible.")
    return list(dict.fromkeys(selected))


def _me_payload(
    user: User,
    selected: List[str],
    csrf_token: Optional[str] = None,
    language: Optional[str] = None,
) -> Dict[str, Any]:
    dsn = get_dsn_by_id(user.dsn_id) if user.dsn_id else None
    return {
        "user": _serialize_user(user),
        "dsn": {"id": dsn["id"], "name": dsn["name"]} if dsn else None,
        "restaurants": _accessible_names(user),
        "selected_restaurants": selected,
        "csrf_token": csrf_token,
        "language": _normalize_language(language),
        "capabilities": {
            "settings": user.role in ADMIN_ROLES,
            "manage_dsns": user.role == "superuser",
        },
    }


def _chat_failure_payload(
    message: str,
    *,
    include_debug: bool = False,
    error: Optional[str] = None,
    failed_query: Optional[Dict[str, Any]] = None,
    language: str = "en",
) -> Dict[str, Any]:
    if language == "es":
        recommendations = [
            "Prueba acotar el período, el alcance de restaurantes o la métrica para que Vera pueda ejecutar una consulta más rápida.",
            "Pide primero el análisis por mes o por semana, y después profundiza en días o productos cuando el patrón esté claro.",
        ]
        suggested = [
            "¿Puedes resumir esto por mes primero?",
            "¿Qué parte del análisis importa más: ventas, tickets o ticket promedio?",
        ]
    else:
        recommendations = [
            "Try narrowing the date range, restaurant scope, or metric so Vera can run a faster query.",
            "Ask for the same analysis by month or by week first, then drill into days or products after the broad pattern is clear.",
        ]
        suggested = [
            "Can you summarize this by month first?",
            "Which part of this analysis is most important: sales, tickets, or average ticket?",
        ]
    payload: Dict[str, Any] = {
        "action": "answer",
        "message": message,
        "tables": [],
        "charts": [],
        "recommendations": recommendations,
        "suggested_next_questions": suggested,
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


def _require_admin(user: User) -> None:
    if user.role not in ADMIN_ROLES:
        raise HTTPException(status_code=403, detail="Settings access is restricted to admins.")


def _require_superuser(user: User) -> None:
    if user.role != "superuser":
        raise HTTPException(status_code=403, detail="Only superusers can manage DSNs.")


def _can_scope_dsn(actor: User, dsn_id: Optional[int]) -> bool:
    return actor.role == "superuser" or (actor.role == "admin" and actor.dsn_id is not None and dsn_id == actor.dsn_id)


def _admin_visible_users(actor: User) -> List[Dict[str, Any]]:
    _require_admin(actor)
    return list_users(dsn_id=actor.dsn_id if actor.role == "admin" else None)


def _admin_restaurant_ids_for_dsn(dsn_id: int) -> set[int]:
    return {int(row["id"]) for row in list_restaurants_by_dsn(dsn_id)}


def _validate_role(role: str) -> None:
    if role not in MANAGEABLE_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role.")


def _ensure_admin_can_assign(actor: User, *, role: str, dsn_id: Optional[int]) -> None:
    _require_admin(actor)
    _validate_role(role)
    if actor.role == "superuser":
        return
    if role not in SCOPED_ADMIN_ASSIGNABLE_ROLES:
        raise HTTPException(status_code=403, detail="Scoped admins can only assign user or db_admin roles.")
    if actor.dsn_id is None or dsn_id != actor.dsn_id:
        raise HTTPException(status_code=403, detail="Scoped admins can only manage their own DSN.")


def _ensure_admin_can_manage_target(actor: User, target: User) -> None:
    _require_admin(actor)
    if actor.role == "superuser":
        return
    if actor.dsn_id is None or target.dsn_id != actor.dsn_id or target.role not in SCOPED_ADMIN_ASSIGNABLE_ROLES:
        raise HTTPException(status_code=403, detail="Scoped admins can only manage user/db_admin accounts in their own DSN.")


def _ensure_not_last_superuser(target: User, *, next_role: Optional[str], next_active: Optional[bool]) -> None:
    demoting = next_role is not None and next_role != "superuser"
    deactivating = next_active is False
    if target.role == "superuser" and target.is_active and (demoting or deactivating) and count_active_superusers() <= 1:
        raise HTTPException(status_code=400, detail="Cannot demote or deactivate the last active superuser.")


def _validate_restaurant_ids_for_dsn(restaurant_ids: List[int], dsn_id: Optional[int]) -> List[int]:
    if not restaurant_ids:
        return []
    if dsn_id is None:
        raise HTTPException(status_code=400, detail="Restaurant restrictions require a DSN.")
    allowed = _admin_restaurant_ids_for_dsn(dsn_id)
    unique_ids = list(dict.fromkeys(int(rid) for rid in restaurant_ids))
    if any(rid not in allowed for rid in unique_ids):
        raise HTTPException(status_code=403, detail="One or more restaurants are outside the selected DSN.")
    return unique_ids


def _validate_password_policy(password: str, email: str) -> None:
    normalized = (password or "").strip()
    if len(normalized) < 10:
        raise HTTPException(status_code=400, detail="Password must be at least 10 characters.")
    if normalized.lower() == (email or "").strip().lower():
        raise HTTPException(status_code=400, detail="Password cannot be the same as the email.")


def _public_base_url(request: Request) -> str:
    configured = os.getenv("PUBLIC_BASE_URL", "").strip()
    if configured:
        return configured.rstrip("/")
    return str(request.base_url).rstrip("/")


@router.post("/login")
def login(req: LoginRequest, response: Response):
    user = get_user_by_email(req.email.strip())
    if not user or not verify_password(req.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials.")
    if not user.is_active:
        raise HTTPException(status_code=401, detail="User is deactivated.")
    csrf_token = _new_csrf_token()
    language = _normalize_language(req.language)
    session = {
        "user_id": user.id,
        "selected_restaurants": [],
        "language": language,
        "csrf_token": csrf_token,
        "iat": int(time.time()),
    }
    selected = _auto_selected_restaurants(user, [])
    session["selected_restaurants"] = selected
    _set_session_cookie(response, session)
    return _me_payload(user, selected, csrf_token, language)


@router.post("/logout")
def logout(request: Request, response: Response):
    session = _session_from_request(request)
    _require_csrf(request, session)
    _clear_session_cookie(response)
    return {"ok": True}


@router.get("/me")
def me(request: Request, response: Response):
    user, session = _current_user(request)
    csrf_token = session.get("csrf_token")
    if not csrf_token:
        csrf_token = _new_csrf_token()
        session["csrf_token"] = csrf_token
        session["iat"] = int(time.time())
        _set_session_cookie(response, session)
    language = _normalize_language(session.get("language"))
    if session.get("language") != language:
        session["language"] = language
        session["iat"] = int(time.time())
        _set_session_cookie(response, session)
    selected = _auto_selected_restaurants(user, session.get("selected_restaurants") or [])
    if selected != (session.get("selected_restaurants") or []):
        session["selected_restaurants"] = selected
        session["iat"] = int(time.time())
        _set_session_cookie(response, session)
    return _me_payload(user, selected, csrf_token, language)


@router.post("/restaurants/select")
def select_restaurants(req: RestaurantSelectRequest, request: Request, response: Response):
    user, session = _current_user(request)
    _require_csrf(request, session)
    selected = _auto_selected_restaurants(user, _validate_restaurants(user, req.restaurant_names))
    session["selected_restaurants"] = selected
    session["iat"] = int(time.time())
    _set_session_cookie(response, session)
    return _me_payload(user, selected, session.get("csrf_token"), session.get("language"))


@router.post("/language")
def set_language(req: LanguageRequest, request: Request, response: Response):
    user, session = _current_user(request)
    _require_csrf(request, session)
    language = _normalize_language(req.language)
    session["language"] = language
    session["iat"] = int(time.time())
    _set_session_cookie(response, session)
    selected = _auto_selected_restaurants(user, session.get("selected_restaurants") or [])
    session["selected_restaurants"] = selected
    _set_session_cookie(response, session)
    return _me_payload(user, selected, session.get("csrf_token"), language)


@router.post("/memory/reset")
def reset_memory(request: Request):
    user, _session = _current_user(request)
    _require_csrf(request, _session)
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


@router.get("/admin/users")
def admin_users(request: Request):
    actor, _session = _current_user(request)
    return {"users": _admin_visible_users(actor)}


@router.patch("/admin/users/{user_id}")
def admin_update_user(user_id: int, req: AdminUserPatch, request: Request):
    actor, session = _current_user(request)
    _require_csrf(request, session)
    target = get_user_by_id(user_id)
    if not target:
        raise HTTPException(status_code=404, detail="User not found.")
    _ensure_admin_can_manage_target(actor, target)

    next_role = req.role if req.role is not None else target.role
    next_dsn_id = req.dsn_id if "dsn_id" in req.model_fields_set else target.dsn_id
    _ensure_admin_can_assign(actor, role=next_role, dsn_id=next_dsn_id)
    _ensure_not_last_superuser(target, next_role=req.role, next_active=req.is_active)
    if req.is_active is False and target.id == actor.id:
        raise HTTPException(status_code=400, detail="Users cannot deactivate themselves.")

    restaurant_ids: Optional[List[int]] = None
    if req.restaurant_ids is not None:
        restaurant_ids = _validate_restaurant_ids_for_dsn(req.restaurant_ids, next_dsn_id)

    dsn_changed = "dsn_id" in req.model_fields_set and req.dsn_id != target.dsn_id
    update_user_admin_fields(
        target.id,
        role=req.role,
        dsn_id=next_dsn_id,
        set_dsn="dsn_id" in req.model_fields_set,
        is_active=req.is_active,
    )
    if restaurant_ids is not None:
        set_user_restaurants(target.id, restaurant_ids)
    elif dsn_changed:
        set_user_restaurants(target.id, [])
    if req.is_active is False:
        clear_sessions_for_user(target.id)
    record_admin_audit_event(
        actor_user_id=actor.id,
        action="update_user",
        target_type="user",
        target_id=target.id,
        details=req.model_dump(exclude_unset=True),
    )
    return {"ok": True, "users": _admin_visible_users(actor)}


@router.get("/admin/invites")
def admin_invites(request: Request):
    actor, _session = _current_user(request)
    _require_admin(actor)
    return {"invites": list_invites(dsn_id=actor.dsn_id if actor.role == "admin" else None)}


@router.post("/admin/invites")
def admin_create_invite(req: InviteCreateRequest, request: Request):
    actor, session = _current_user(request)
    _require_csrf(request, session)
    dsn_id = req.dsn_id if actor.role == "superuser" else actor.dsn_id
    _ensure_admin_can_assign(actor, role=req.role, dsn_id=dsn_id)
    if not req.email.strip():
        raise HTTPException(status_code=400, detail="Email is required.")
    existing = get_user_by_email(req.email)
    if existing and existing.is_active:
        raise HTTPException(status_code=409, detail="A user with this email already exists. Edit that user instead.")
    restaurant_ids = _validate_restaurant_ids_for_dsn(req.restaurant_ids, dsn_id)
    invite = create_invite(
        email=req.email,
        role=req.role,
        dsn_id=dsn_id,
        restaurant_ids=restaurant_ids,
        created_by=actor.id,
    )
    invite_url = f"{_public_base_url(request)}/invite/{invite.pop('token')}"
    record_admin_audit_event(
        actor_user_id=actor.id,
        action="create_invite",
        target_type="invite",
        target_id=invite["id"],
        details={"email": invite["email"], "role": invite["role"], "dsn_id": invite["dsn_id"], "restaurant_ids": restaurant_ids},
    )
    return {"invite": invite, "invite_url": invite_url}


@router.delete("/admin/invites/{invite_id}")
def admin_revoke_invite(invite_id: int, request: Request):
    actor, session = _current_user(request)
    _require_csrf(request, session)
    _require_admin(actor)
    invite = get_invite_by_id(invite_id)
    if not invite:
        raise HTTPException(status_code=404, detail="Invite not found.")
    if not _can_scope_dsn(actor, invite["dsn_id"]):
        raise HTTPException(status_code=403, detail="Invite is outside your admin scope.")
    revoke_invite(invite_id)
    record_admin_audit_event(actor_user_id=actor.id, action="revoke_invite", target_type="invite", target_id=invite_id)
    return {"ok": True}


@router.get("/admin/dsns")
def admin_dsns(request: Request):
    actor, _session = _current_user(request)
    _require_admin(actor)
    dsns = list_dsns_safe()
    if actor.role != "superuser":
        dsns = [dsn for dsn in dsns if dsn["id"] == actor.dsn_id]
    return {"dsns": [{key: value for key, value in dsn.items() if key != "dsn"} for dsn in dsns]}


@router.post("/admin/dsns")
def admin_create_dsn(req: DsnCreateRequest, request: Request):
    actor, session = _current_user(request)
    _require_csrf(request, session)
    _require_superuser(actor)
    if not req.name.strip() or not req.dsn.strip():
        raise HTTPException(status_code=400, detail="Name and DSN are required.")
    try:
        with psycopg.connect(req.dsn, connect_timeout=8) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"DSN connection failed: {exc}") from exc
    dsn_id = create_dsn(req.name.strip(), req.dsn.strip())
    synced_count = sync_restaurants_from_dsn(dsn_id)
    record_admin_audit_event(
        actor_user_id=actor.id,
        action="create_dsn",
        target_type="dsn",
        target_id=dsn_id,
        details={"name": req.name.strip(), "synced_restaurants": synced_count},
    )
    return {"ok": True, "dsn_id": dsn_id, "synced_restaurants": synced_count}


@router.patch("/admin/dsns/{dsn_id}")
def admin_update_dsn(dsn_id: int, req: DsnPatchRequest, request: Request):
    actor, session = _current_user(request)
    _require_csrf(request, session)
    _require_superuser(actor)
    if req.dsn is not None:
        try:
            with psycopg.connect(req.dsn, connect_timeout=8) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"DSN connection failed: {exc}") from exc
    update_dsn(dsn_id, name=req.name.strip() if req.name is not None else None, dsn=req.dsn.strip() if req.dsn is not None else None)
    record_admin_audit_event(
        actor_user_id=actor.id,
        action="update_dsn",
        target_type="dsn",
        target_id=dsn_id,
        details={"name_changed": req.name is not None, "dsn_changed": req.dsn is not None},
    )
    return {"ok": True}


@router.post("/admin/dsns/{dsn_id}/sync-restaurants")
def admin_sync_dsn_restaurants(dsn_id: int, request: Request):
    actor, session = _current_user(request)
    _require_csrf(request, session)
    _require_superuser(actor)
    synced_count = sync_restaurants_from_dsn(dsn_id)
    record_admin_audit_event(
        actor_user_id=actor.id,
        action="sync_restaurants",
        target_type="dsn",
        target_id=dsn_id,
        details={"synced_restaurants": synced_count},
    )
    return {"ok": True, "synced_restaurants": synced_count}


@router.get("/admin/dsns/{dsn_id}/restaurants")
def admin_dsn_restaurants(dsn_id: int, request: Request):
    actor, _session = _current_user(request)
    _require_admin(actor)
    if not _can_scope_dsn(actor, dsn_id):
        raise HTTPException(status_code=403, detail="DSN is outside your admin scope.")
    return {"restaurants": list_restaurants_by_dsn(dsn_id)}


@router.get("/invites/{token}")
def invite_preview(token: str):
    invite = get_invite_by_token(token)
    if not invite:
        raise HTTPException(status_code=404, detail="Invite not found.")
    invite.pop("token_hash", None)
    return {"invite": invite}


@router.post("/invites/{token}/accept")
def invite_accept(token: str, req: InviteAcceptRequest):
    invite = get_invite_by_token(token)
    if not invite:
        raise HTTPException(status_code=404, detail="Invite not found.")
    if invite["status"] != "pending":
        raise HTTPException(status_code=400, detail="Invite is no longer valid.")
    _validate_password_policy(req.password, invite["email"])
    user_id = accept_invite(token, hash_password(req.password))
    record_admin_audit_event(
        actor_user_id=None,
        action="accept_invite",
        target_type="invite",
        target_id=invite["id"],
        details={"accepted_user_id": user_id, "email": invite["email"]},
    )
    return {"ok": True}


@router.post("/chat")
def chat(req: ChatRequest, request: Request, response: Response):
    user, session = _current_user(request)
    _require_csrf(request, session)
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
    language = _chat_language(req.message, session, req.language)

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
            language=language,
            preview=True,
        )
    except VeraQueryExecutionError as exc:
        session.pop("pending_clarification", None)
        session["selected_restaurants"] = restaurants
        session["iat"] = int(time.time())
        _set_session_cookie(response, session)
        message = (
            "No pude completar ese análisis porque la consulta a la base de datos agotó el tiempo de espera. "
            "El próximo paso más seguro es acotar la pregunta primero y después profundizar cuando sepamos dónde está el movimiento."
            if language == "es"
            else "I could not complete that analysis because the database query timed out. "
            "The safest next step is to narrow the question first, then drill down once we know where the movement is."
        )
        payload = _chat_failure_payload(
            message,
            include_debug=req.include_debug,
            error=str(exc),
            failed_query={
                "purpose": exc.purpose,
                "sql": exc.sql,
                "params": exc.params,
            },
            language=language,
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
        message = (
            "No pude completar ese análisis porque la consulta a la base de datos agotó el tiempo de espera. "
            "El próximo paso más seguro es acotar la pregunta primero y después profundizar cuando sepamos dónde está el movimiento."
            if language == "es"
            else "I could not complete that analysis because the database query timed out. "
            "The safest next step is to narrow the question first, then drill down once we know where the movement is."
        )
        payload = _chat_failure_payload(
            message,
            include_debug=req.include_debug,
            error=str(exc),
            language=language,
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
        message = (
            "Rechacé el SQL generado antes de ejecutarlo porque no pasó las validaciones de seguridad de la app. "
            "Reformula la pregunta con la métrica, el período y el alcance de restaurantes que querés, y voy a planear una consulta más segura."
            if language == "es"
            else "I rejected the generated SQL before running it because it did not pass the app's safety checks. "
            "Please rephrase the question with the metric, period, and restaurant scope you want, and I will plan a safer query."
        )
        payload = _chat_failure_payload(
            message,
            include_debug=req.include_debug,
            error=str(exc),
            language=language,
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
