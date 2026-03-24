from __future__ import annotations

import json
import os
from dataclasses import dataclass
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


def _connect(db_dsn: str = DEFAULT_DB_DSN) -> psycopg.Connection:
    return psycopg.connect(db_dsn, row_factory=dict_row)


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
        conn.commit()


def create_dsn(name: str, dsn: str, db_dsn: str = DEFAULT_DB_DSN) -> int:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dsns (name, dsn) VALUES (%s, %s) RETURNING id",
                (name, dsn),
            )
            row = cur.fetchone()
        conn.commit()
        return int(row["id"])


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
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (email, password_hash, role, dsn_id) VALUES (%s, %s, %s, %s) RETURNING id",
                (email, password_hash, role, dsn_id),
            )
            row = cur.fetchone()
        conn.commit()
        return int(row["id"])


def get_user_by_email(email: str, db_dsn: str = DEFAULT_DB_DSN) -> Optional[User]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (email,))
            row = cur.fetchone()
    if not row:
        return None
    return User(
        id=row["id"],
        email=row["email"],
        password_hash=row["password_hash"],
        role=row["role"],
        dsn_id=row["dsn_id"],
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
                    SELECT u.id, u.email, u.role, u.dsn_id,
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
                    SELECT u.id, u.email, u.role, u.dsn_id,
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
                "UPDATE users SET password_hash = %s WHERE id = %s",
                (password_hash, user_id),
            )
        conn.commit()


def update_user_role(user_id: int, role: str, db_dsn: str = DEFAULT_DB_DSN) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET role = %s WHERE id = %s",
                (role, user_id),
            )
        conn.commit()


def update_user_dsn(user_id: int, dsn_id: Optional[int], db_dsn: str = DEFAULT_DB_DSN) -> None:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET dsn_id = %s WHERE id = %s",
                (dsn_id, user_id),
            )
        conn.commit()
