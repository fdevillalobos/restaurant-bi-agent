from __future__ import annotations

import json
import os
import re
import secrets
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any

import psycopg
from psycopg.rows import dict_row


DEFAULT_DB_DSN = os.getenv("CONTROL_DB_DSN", "")


@dataclass
class User:
    id: int
    email: str
    password_hash: str
    role: str
    dsn_id: Optional[int]
    is_active: bool = True


ADMIN_ROLES = {"admin", "superuser"}
MANAGEABLE_ROLES = {"user", "db_admin", "admin", "superuser"}
SCOPED_ADMIN_ASSIGNABLE_ROLES = {"user", "db_admin"}
INVITE_TTL_HOURS = 24


def _normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _hash_invite_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _connect(db_dsn: str = DEFAULT_DB_DSN) -> psycopg.Connection:
    dsn = db_dsn or os.getenv("CONTROL_DB_DSN", "")
    return psycopg.connect(dsn, row_factory=dict_row)


def init_db(db_dsn: str = DEFAULT_DB_DSN) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dsns (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    dsn TEXT NOT NULL
                );
                """
            )
            cur.execute("ALTER TABLE dsns ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
            cur.execute("ALTER TABLE dsns ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL,
                    dsn_id INTEGER REFERENCES dsns(id)
                );
                """
            )
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_lower_unique
                ON users (LOWER(email));
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS restaurants (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    dsn_id INTEGER NOT NULL REFERENCES dsns(id),
                    UNIQUE (name, dsn_id)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_restaurants (
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    restaurant_id INTEGER NOT NULL REFERENCES restaurants(id),
                    UNIQUE (user_id, restaurant_id)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    chat_id BIGINT PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    selected_restaurants TEXT,
                    language TEXT,
                    include_sql BOOLEAN,
                    conversation_history TEXT
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_conversation_memories (
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    dsn_id INTEGER NOT NULL REFERENCES dsns(id),
                    conversation_history TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, dsn_id)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS restaurant_knowledge (
                    dsn_id INTEGER NOT NULL REFERENCES dsns(id),
                    restaurant_name TEXT NOT NULL,
                    content TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (dsn_id, restaurant_name)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS web_chat_messages (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    dsn_id INTEGER NOT NULL REFERENCES dsns(id),
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    payload TEXT,
                    selected_restaurants TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_web_chat_messages_user_dsn_created
                ON web_chat_messages (user_id, dsn_id, created_at DESC, id DESC);
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_invites (
                    id SERIAL PRIMARY KEY,
                    email TEXT NOT NULL,
                    role TEXT NOT NULL,
                    dsn_id INTEGER REFERENCES dsns(id),
                    restaurant_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                    token_hash TEXT NOT NULL UNIQUE,
                    created_by INTEGER REFERENCES users(id),
                    expires_at TIMESTAMPTZ NOT NULL,
                    accepted_at TIMESTAMPTZ,
                    accepted_user_id INTEGER REFERENCES users(id),
                    revoked_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_user_invites_email_created
                ON user_invites (LOWER(email), created_at DESC);
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_audit_events (
                    id SERIAL PRIMARY KEY,
                    actor_user_id INTEGER REFERENCES users(id),
                    action TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    target_id TEXT,
                    details JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_admin_audit_events_created
                ON admin_audit_events (created_at DESC);
                """
            )
        conn.commit()


def create_dsn(name: str, dsn: str, db_dsn: str = DEFAULT_DB_DSN) -> int:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dsns (name, dsn, created_at, updated_at) VALUES (%s, %s, NOW(), NOW()) RETURNING id",
                (name, dsn),
            )
            row = cur.fetchone()
        conn.commit()
        return int(row["id"])


def update_dsn(
    dsn_id: int,
    *,
    name: Optional[str] = None,
    dsn: Optional[str] = None,
    db_dsn: str = DEFAULT_DB_DSN,
) -> None:
    fields: List[str] = []
    params: List[Any] = []
    if name is not None:
        fields.append("name = %s")
        params.append(name)
    if dsn is not None:
        fields.append("dsn = %s")
        params.append(dsn)
    if not fields:
        return
    fields.append("updated_at = NOW()")
    params.append(dsn_id)
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE dsns SET {', '.join(fields)} WHERE id = %s", params)
        conn.commit()


def get_dsn_by_name(name: str, db_dsn: str = DEFAULT_DB_DSN) -> Optional[Dict[str, Any]]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM dsns WHERE name = %s", (name,))
            return cur.fetchone()


def list_dsns(db_dsn: str = DEFAULT_DB_DSN) -> List[Dict[str, Any]]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM dsns ORDER BY name")
            return cur.fetchall()


def list_dsns_safe(db_dsn: str = DEFAULT_DB_DSN) -> List[Dict[str, Any]]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.id, d.name, d.created_at, d.updated_at, COUNT(r.id)::INT AS restaurant_count
                FROM dsns d
                LEFT JOIN restaurants r ON r.dsn_id = d.id
                GROUP BY d.id, d.name, d.created_at, d.updated_at
                ORDER BY d.name
                """
            )
            return cur.fetchall()


def get_dsn_by_id(dsn_id: int, db_dsn: str = DEFAULT_DB_DSN) -> Optional[Dict[str, Any]]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM dsns WHERE id = %s", (dsn_id,))
            return cur.fetchone()


def sync_restaurants_from_dsn(dsn_id: int, db_dsn: str = DEFAULT_DB_DSN) -> int:
    dsn = get_dsn_by_id(dsn_id, db_dsn=db_dsn)
    if not dsn:
        raise ValueError("DSN not found")
    dsn_value = dsn["dsn"]
    with psycopg.connect(dsn_value) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT restaurant FROM sales ORDER BY restaurant;")
            rows = cur.fetchall()

    names = [r[0] for r in rows if r and r[0] is not None]
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            for name in names:
                cur.execute(
                    "INSERT INTO restaurants (name, dsn_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (name, dsn_id),
                )
        conn.commit()
    return len(names)


def list_restaurants_by_dsn(dsn_id: int, db_dsn: str = DEFAULT_DB_DSN) -> List[Dict[str, Any]]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM restaurants WHERE dsn_id = %s ORDER BY name",
                (dsn_id,),
            )
            return cur.fetchall()


def create_user(
    email: str,
    password_hash: str,
    role: str,
    dsn_id: Optional[int],
    db_dsn: str = DEFAULT_DB_DSN,
) -> int:
    email = _normalize_email(email)
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (email, password_hash, role, dsn_id, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, TRUE, NOW(), NOW())
                RETURNING id
                """,
                (email, password_hash, role, dsn_id),
            )
            row = cur.fetchone()
        conn.commit()
        return int(row["id"])


def get_user_by_email(email: str, db_dsn: str = DEFAULT_DB_DSN) -> Optional[User]:
    email = _normalize_email(email)
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE LOWER(email) = LOWER(%s)", (email,))
            row = cur.fetchone()
    if not row:
        return None
    return User(
        id=row["id"],
        email=row["email"],
        password_hash=row["password_hash"],
        role=row["role"],
        dsn_id=row["dsn_id"],
        is_active=bool(row.get("is_active", True)),
    )


def get_user_by_id(user_id: int, db_dsn: str = DEFAULT_DB_DSN) -> Optional[User]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
    if not row:
        return None
    return User(
        id=row["id"],
        email=row["email"],
        password_hash=row["password_hash"],
        role=row["role"],
        dsn_id=row["dsn_id"],
        is_active=bool(row.get("is_active", True)),
    )


def set_user_restaurants(
    user_id: int, restaurant_ids: List[int], db_dsn: str = DEFAULT_DB_DSN
) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM user_restaurants WHERE user_id = %s", (user_id,))
            for rid in restaurant_ids:
                cur.execute(
                    "INSERT INTO user_restaurants (user_id, restaurant_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (user_id, rid),
                )
        conn.commit()


def list_user_restaurants(user_id: int, db_dsn: str = DEFAULT_DB_DSN) -> List[Dict[str, Any]]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.* FROM restaurants r
                JOIN user_restaurants ur ON ur.restaurant_id = r.id
                WHERE ur.user_id = %s
                ORDER BY r.name
                """,
                (user_id,),
            )
            return cur.fetchall()


def list_accessible_restaurants(user: User, db_dsn: str = DEFAULT_DB_DSN) -> List[Dict[str, Any]]:
    if user.dsn_id is None:
        return []
    restricted = list_user_restaurants(user.id, db_dsn=db_dsn)
    if restricted:
        return restricted
    return list_restaurants_by_dsn(user.dsn_id, db_dsn=db_dsn)


def set_session(
    chat_id: int,
    user_id: int,
    selected_restaurants: Optional[str],
    db_dsn: str = DEFAULT_DB_DSN,
) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sessions (chat_id, user_id, selected_restaurants)
                VALUES (%s, %s, %s)
                ON CONFLICT (chat_id) DO UPDATE
                SET user_id = EXCLUDED.user_id,
                    selected_restaurants = EXCLUDED.selected_restaurants
                """,
                (chat_id, user_id, selected_restaurants),
            )
        conn.commit()


def set_session_language(chat_id: int, language: str, db_dsn: str = DEFAULT_DB_DSN) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sessions SET language = %s WHERE chat_id = %s",
                (language, chat_id),
            )
        conn.commit()


def set_session_include_sql(chat_id: int, include_sql: bool, db_dsn: str = DEFAULT_DB_DSN) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sessions SET include_sql = %s WHERE chat_id = %s",
                (include_sql, chat_id),
            )
        conn.commit()


def clear_session(chat_id: int, db_dsn: str = DEFAULT_DB_DSN) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sessions WHERE chat_id = %s", (chat_id,))
        conn.commit()


def clear_sessions_for_user(user_id: int, db_dsn: str = DEFAULT_DB_DSN) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sessions WHERE user_id = %s", (user_id,))
        conn.commit()


def get_session(chat_id: int, db_dsn: str = DEFAULT_DB_DSN) -> Optional[Dict[str, Any]]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM sessions WHERE chat_id = %s", (chat_id,))
            return cur.fetchone()


_MAX_HISTORY_MESSAGES = 20  # 10 exchanges


def get_conversation_history(chat_id: int, db_dsn: str = DEFAULT_DB_DSN) -> List[Dict[str, Any]]:
    sess = get_session(chat_id, db_dsn=db_dsn)
    if not sess or not sess.get("conversation_history"):
        return []
    try:
        return json.loads(sess["conversation_history"])
    except Exception:
        return []


def append_conversation(
    chat_id: int,
    user_message: str,
    assistant_message: str,
    db_dsn: str = DEFAULT_DB_DSN,
) -> None:
    history = get_conversation_history(chat_id, db_dsn=db_dsn)
    history.append({"role": "user", "content": user_message})
    history.append({"role": "assistant", "content": assistant_message})
    if len(history) > _MAX_HISTORY_MESSAGES:
        history = history[-_MAX_HISTORY_MESSAGES:]
    raw = json.dumps(history)
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sessions SET conversation_history = %s WHERE chat_id = %s",
                (raw, chat_id),
            )
        conn.commit()


def clear_conversation_history(chat_id: int, db_dsn: str = DEFAULT_DB_DSN) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sessions SET conversation_history = NULL WHERE chat_id = %s",
                (chat_id,),
            )
        conn.commit()


_MAX_VERA_MEMORY_MESSAGES = 60  # 30 exchanges
_MAX_MEMORY_CONTENT_CHARS = 4000


def _trim_memory_content(content: str, max_chars: int = _MAX_MEMORY_CONTENT_CHARS) -> str:
    text = str(content or "")
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "..."


def trim_conversation_messages(
    history: List[Dict[str, Any]],
    max_messages: int = _MAX_VERA_MEMORY_MESSAGES,
) -> List[Dict[str, str]]:
    trimmed: List[Dict[str, str]] = []
    for msg in history[-max_messages:]:
        role = str(msg.get("role") or "assistant")
        if role not in ("user", "assistant"):
            role = "assistant"
        trimmed.append({"role": role, "content": _trim_memory_content(msg.get("content", ""))})
    return trimmed


def get_user_conversation_memory(
    user_id: int,
    dsn_id: int,
    db_dsn: str = DEFAULT_DB_DSN,
) -> List[Dict[str, Any]]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT conversation_history
                FROM user_conversation_memories
                WHERE user_id = %s AND dsn_id = %s
                """,
                (user_id, dsn_id),
            )
            row = cur.fetchone()
    if not row or not row.get("conversation_history"):
        return []
    try:
        return json.loads(row["conversation_history"])
    except Exception:
        return []


def append_user_conversation_memory(
    user_id: int,
    dsn_id: int,
    user_message: str,
    assistant_message: str,
    db_dsn: str = DEFAULT_DB_DSN,
) -> None:
    history = get_user_conversation_memory(user_id, dsn_id, db_dsn=db_dsn)
    history.append({"role": "user", "content": user_message})
    history.append({"role": "assistant", "content": assistant_message})
    history = trim_conversation_messages(history)
    raw = json.dumps(history)
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_conversation_memories (user_id, dsn_id, conversation_history, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (user_id, dsn_id) DO UPDATE
                SET conversation_history = EXCLUDED.conversation_history,
                    updated_at = NOW()
                """,
                (user_id, dsn_id, raw),
            )
        conn.commit()


def clear_user_conversation_memory(
    user_id: int,
    dsn_id: int,
    db_dsn: str = DEFAULT_DB_DSN,
) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM user_conversation_memories WHERE user_id = %s AND dsn_id = %s",
                (user_id, dsn_id),
            )
        conn.commit()


def append_web_chat_message(
    user_id: int,
    dsn_id: int,
    role: str,
    content: str,
    payload: Optional[Dict[str, Any]] = None,
    selected_restaurants: Optional[List[str]] = None,
    db_dsn: str = DEFAULT_DB_DSN,
) -> None:
    raw_payload = json.dumps(payload, default=str) if payload is not None else None
    raw_restaurants = json.dumps(selected_restaurants or [])
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO web_chat_messages
                    (user_id, dsn_id, role, content, payload, selected_restaurants)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (user_id, dsn_id, role, _trim_memory_content(content, 8000), raw_payload, raw_restaurants),
            )
        conn.commit()


def list_web_chat_messages(
    user_id: int,
    dsn_id: int,
    assistant_limit: int = 10,
    db_dsn: str = DEFAULT_DB_DSN,
) -> List[Dict[str, Any]]:
    row_limit = max(assistant_limit * 4, 20)
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, role, content, payload, selected_restaurants, created_at
                FROM web_chat_messages
                WHERE user_id = %s AND dsn_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (user_id, dsn_id, row_limit),
            )
            rows = cur.fetchall()

    ordered = list(reversed(rows))
    assistant_seen = 0
    keep_from = 0
    for idx in range(len(ordered) - 1, -1, -1):
        if ordered[idx]["role"] == "assistant":
            assistant_seen += 1
            if assistant_seen == assistant_limit:
                keep_from = max(idx - 1, 0)
                break
    selected = ordered[keep_from:] if assistant_seen >= assistant_limit else ordered

    result: List[Dict[str, Any]] = []
    for row in selected:
        try:
            payload = json.loads(row["payload"]) if row.get("payload") else None
        except Exception:
            payload = None
        try:
            restaurants = json.loads(row["selected_restaurants"] or "[]")
        except Exception:
            restaurants = []
        result.append(
            {
                "id": row["id"],
                "role": row["role"],
                "content": row["content"],
                "payload": payload,
                "selected_restaurants": restaurants,
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
            }
        )
    return result


def clear_web_chat_messages(
    user_id: int,
    dsn_id: int,
    db_dsn: str = DEFAULT_DB_DSN,
) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM web_chat_messages WHERE user_id = %s AND dsn_id = %s",
                (user_id, dsn_id),
            )
        conn.commit()


def get_restaurant_knowledge(
    dsn_id: int,
    restaurant_names: List[str],
    db_dsn: str = DEFAULT_DB_DSN,
) -> Dict[str, str]:
    if not restaurant_names:
        return {}
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT restaurant_name, content
                FROM restaurant_knowledge
                WHERE dsn_id = %s AND restaurant_name = ANY(%s)
                ORDER BY restaurant_name
                """,
                (dsn_id, restaurant_names),
            )
            rows = cur.fetchall()
    return {row["restaurant_name"]: row["content"] for row in rows}


def upsert_restaurant_knowledge(
    dsn_id: int,
    restaurant_name: str,
    content: str,
    db_dsn: str = DEFAULT_DB_DSN,
) -> None:
    content = _trim_memory_content(content, max_chars=12000)
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO restaurant_knowledge (dsn_id, restaurant_name, content, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (dsn_id, restaurant_name) DO UPDATE
                SET content = CASE
                        WHEN restaurant_knowledge.content = '' THEN EXCLUDED.content
                        ELSE restaurant_knowledge.content || E'\n\n' || EXCLUDED.content
                    END,
                    updated_at = NOW()
                """,
                (dsn_id, restaurant_name, content),
            )
        conn.commit()


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip().lower()).strip("-")
    return slug or "unknown"


def export_restaurant_knowledge_markdown(
    dsn_name: str,
    dsn_id: int,
    restaurant_name: str,
    content: str,
    base_dir: str = "runtime/vera_knowledge",
) -> Path:
    root = Path(base_dir)
    path = root / _slug(dsn_name) / f"{_slug(restaurant_name)}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    body = (
        f"# {restaurant_name}\n\n"
        f"DSN: {dsn_name} ({dsn_id})\n"
        f"Updated: {now}\n\n"
        "## Vera Knowledge\n\n"
        f"{content.strip()}\n"
    )
    path.write_text(body, encoding="utf-8")
    return path


def get_restaurant_ids_by_names(
    dsn_id: int, names: List[str], db_dsn: str = DEFAULT_DB_DSN
) -> List[int]:
    if not names:
        return []
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM restaurants WHERE dsn_id = %s AND name = ANY(%s)",
                (dsn_id, names),
            )
            return [r["id"] for r in cur.fetchall()]


def list_users(dsn_id: Optional[int] = None, db_dsn: str = DEFAULT_DB_DSN) -> List[Dict[str, Any]]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            if dsn_id is not None:
                cur.execute(
                    """
                    SELECT u.id, u.email, u.role, u.dsn_id, u.is_active, u.created_at, u.updated_at,
                           COALESCE(d.name, '(none)') AS dsn_name
                    FROM users u
                    LEFT JOIN dsns d ON d.id = u.dsn_id
                    WHERE u.dsn_id = %s
                    ORDER BY u.email
                    """,
                    (dsn_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT u.id, u.email, u.role, u.dsn_id, u.is_active, u.created_at, u.updated_at,
                           COALESCE(d.name, '(none)') AS dsn_name
                    FROM users u
                    LEFT JOIN dsns d ON d.id = u.dsn_id
                    ORDER BY u.email
                    """
                )
            users = cur.fetchall()

        if not users:
            return []

        # Fetch all restaurant restrictions in one query, then group in Python
        user_ids = [u["id"] for u in users]
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ur.user_id, r.name
                FROM user_restaurants ur
                JOIN restaurants r ON r.id = ur.restaurant_id
                WHERE ur.user_id = ANY(%s)
                ORDER BY ur.user_id, r.name
                """,
                (user_ids,),
            )
            rows = cur.fetchall()

    restaurants_by_user: Dict[int, List[str]] = {uid: [] for uid in user_ids}
    for row in rows:
        restaurants_by_user[row["user_id"]].append(row["name"])

    return [{**u, "restaurants": restaurants_by_user[u["id"]]} for u in users]


def update_user_password(user_id: int, password_hash: str, db_dsn: str = DEFAULT_DB_DSN) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET password_hash = %s, updated_at = NOW() WHERE id = %s",
                (password_hash, user_id),
            )
        conn.commit()


def update_user_role(user_id: int, role: str, db_dsn: str = DEFAULT_DB_DSN) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET role = %s, updated_at = NOW() WHERE id = %s",
                (role, user_id),
            )
        conn.commit()


def update_user_dsn(user_id: int, dsn_id: Optional[int], db_dsn: str = DEFAULT_DB_DSN) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET dsn_id = %s, updated_at = NOW() WHERE id = %s",
                (dsn_id, user_id),
            )
        conn.commit()


def update_user_admin_fields(
    user_id: int,
    *,
    role: Optional[str] = None,
    dsn_id: Optional[int] = None,
    is_active: Optional[bool] = None,
    set_dsn: bool = False,
    db_dsn: str = DEFAULT_DB_DSN,
) -> None:
    fields: List[str] = []
    params: List[Any] = []
    if role is not None:
        fields.append("role = %s")
        params.append(role)
    if set_dsn:
        fields.append("dsn_id = %s")
        params.append(dsn_id)
    if is_active is not None:
        fields.append("is_active = %s")
        params.append(is_active)
    if not fields:
        return
    fields.append("updated_at = NOW()")
    params.append(user_id)
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE users SET {', '.join(fields)} WHERE id = %s", params)
        conn.commit()


def count_active_superusers(db_dsn: str = DEFAULT_DB_DSN) -> int:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*)::INT AS count FROM users WHERE role = 'superuser' AND is_active IS TRUE")
            row = cur.fetchone()
    return int(row["count"] if row else 0)


def _invite_status(row: Dict[str, Any]) -> str:
    if row.get("accepted_at"):
        return "accepted"
    if row.get("revoked_at"):
        return "revoked"
    expires_at = row.get("expires_at")
    if expires_at and expires_at < _utcnow():
        return "expired"
    return "pending"


def _serialize_invite(row: Dict[str, Any], include_token_hash: bool = False) -> Dict[str, Any]:
    restaurant_ids = row.get("restaurant_ids") or []
    if isinstance(restaurant_ids, str):
        try:
            restaurant_ids = json.loads(restaurant_ids)
        except Exception:
            restaurant_ids = []
    data = {
        "id": row["id"],
        "email": row["email"],
        "role": row["role"],
        "dsn_id": row["dsn_id"],
        "dsn_name": row.get("dsn_name"),
        "restaurant_ids": [int(rid) for rid in restaurant_ids],
        "restaurant_names": row.get("restaurant_names") or [],
        "created_by": row.get("created_by"),
        "created_by_email": row.get("created_by_email"),
        "expires_at": row["expires_at"].isoformat() if row.get("expires_at") else None,
        "accepted_at": row["accepted_at"].isoformat() if row.get("accepted_at") else None,
        "revoked_at": row["revoked_at"].isoformat() if row.get("revoked_at") else None,
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "status": _invite_status(row),
    }
    if include_token_hash:
        data["token_hash"] = row.get("token_hash")
    return data


def create_invite(
    *,
    email: str,
    role: str,
    dsn_id: Optional[int],
    restaurant_ids: List[int],
    created_by: int,
    ttl_hours: int = INVITE_TTL_HOURS,
    db_dsn: str = DEFAULT_DB_DSN,
) -> Dict[str, Any]:
    token = secrets.token_urlsafe(32)
    token_hash = _hash_invite_token(token)
    expires_at = _utcnow() + timedelta(hours=ttl_hours)
    email = _normalize_email(email)
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_invites
                    (email, role, dsn_id, restaurant_ids, token_hash, created_by, expires_at)
                VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s)
                RETURNING *
                """,
                (email, role, dsn_id, json.dumps(restaurant_ids), token_hash, created_by, expires_at),
            )
            row = cur.fetchone()
        conn.commit()
    invite = _serialize_invite(row)
    invite["token"] = token
    return invite


def _invite_select_sql() -> str:
    return """
        SELECT i.*,
               d.name AS dsn_name,
               creator.email AS created_by_email,
               COALESCE(
                   ARRAY_REMOVE(ARRAY_AGG(r.name ORDER BY r.name), NULL),
                   ARRAY[]::TEXT[]
               ) AS restaurant_names
        FROM user_invites i
        LEFT JOIN dsns d ON d.id = i.dsn_id
        LEFT JOIN users creator ON creator.id = i.created_by
        LEFT JOIN restaurants r ON r.id IN (
            SELECT value::INT FROM jsonb_array_elements_text(i.restaurant_ids)
        )
    """


def list_invites(dsn_id: Optional[int] = None, db_dsn: str = DEFAULT_DB_DSN) -> List[Dict[str, Any]]:
    where = "WHERE i.dsn_id = %s" if dsn_id is not None else ""
    params: List[Any] = [dsn_id] if dsn_id is not None else []
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                _invite_select_sql()
                + f"""
                {where}
                GROUP BY i.id, d.name, creator.email
                ORDER BY i.created_at DESC
                """,
                params,
            )
            rows = cur.fetchall()
    return [_serialize_invite(row) for row in rows]


def get_invite_by_id(invite_id: int, db_dsn: str = DEFAULT_DB_DSN) -> Optional[Dict[str, Any]]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                _invite_select_sql()
                + """
                WHERE i.id = %s
                GROUP BY i.id, d.name, creator.email
                """,
                (invite_id,),
            )
            row = cur.fetchone()
    return _serialize_invite(row, include_token_hash=True) if row else None


def get_invite_by_token(token: str, db_dsn: str = DEFAULT_DB_DSN) -> Optional[Dict[str, Any]]:
    token_hash = _hash_invite_token(token)
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                _invite_select_sql()
                + """
                WHERE i.token_hash = %s
                GROUP BY i.id, d.name, creator.email
                """,
                (token_hash,),
            )
            row = cur.fetchone()
    return _serialize_invite(row, include_token_hash=True) if row else None


def revoke_invite(invite_id: int, db_dsn: str = DEFAULT_DB_DSN) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE user_invites SET revoked_at = COALESCE(revoked_at, NOW()) WHERE id = %s AND accepted_at IS NULL",
                (invite_id,),
            )
        conn.commit()


def accept_invite(
    token: str,
    password_hash: str,
    db_dsn: str = DEFAULT_DB_DSN,
) -> int:
    token_hash = _hash_invite_token(token)
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM user_invites WHERE token_hash = %s FOR UPDATE", (token_hash,))
            invite = cur.fetchone()
            if not invite:
                raise ValueError("Invite not found.")
            if _invite_status(invite) != "pending":
                raise ValueError("Invite is no longer valid.")

            email = _normalize_email(invite["email"])
            cur.execute("SELECT * FROM users WHERE LOWER(email) = LOWER(%s) FOR UPDATE", (email,))
            existing = cur.fetchone()
            if existing and existing.get("is_active", True):
                raise ValueError("A user with this email already exists.")

            if existing:
                user_id = int(existing["id"])
                cur.execute(
                    """
                    UPDATE users
                    SET password_hash = %s,
                        role = %s,
                        dsn_id = %s,
                        is_active = TRUE,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (password_hash, invite["role"], invite["dsn_id"], user_id),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO users (email, password_hash, role, dsn_id, is_active, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, TRUE, NOW(), NOW())
                    RETURNING id
                    """,
                    (email, password_hash, invite["role"], invite["dsn_id"]),
                )
                user_id = int(cur.fetchone()["id"])

            restaurant_ids = invite.get("restaurant_ids") or []
            if isinstance(restaurant_ids, str):
                restaurant_ids = json.loads(restaurant_ids)
            cur.execute("DELETE FROM user_restaurants WHERE user_id = %s", (user_id,))
            for restaurant_id in restaurant_ids:
                cur.execute(
                    "INSERT INTO user_restaurants (user_id, restaurant_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (user_id, int(restaurant_id)),
                )
            cur.execute(
                "UPDATE user_invites SET accepted_at = NOW(), accepted_user_id = %s WHERE id = %s",
                (user_id, invite["id"]),
            )
        conn.commit()
    return user_id


def record_admin_audit_event(
    *,
    actor_user_id: Optional[int],
    action: str,
    target_type: str,
    target_id: Optional[Any] = None,
    details: Optional[Dict[str, Any]] = None,
    db_dsn: str = DEFAULT_DB_DSN,
) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO admin_audit_events (actor_user_id, action, target_type, target_id, details)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                """,
                (actor_user_id, action, target_type, str(target_id) if target_id is not None else None, json.dumps(details or {})),
            )
        conn.commit()
