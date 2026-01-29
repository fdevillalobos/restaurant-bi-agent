from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

import psycopg


DEFAULT_DB_PATH = os.getenv("CONTROL_DB_PATH", "control.db")


@dataclass
class User:
    id: int
    email: str
    password_hash: str
    role: str
    dsn_id: Optional[int]


def _connect(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dsns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                dsn TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                dsn_id INTEGER,
                FOREIGN KEY (dsn_id) REFERENCES dsns(id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS restaurants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                dsn_id INTEGER NOT NULL,
                UNIQUE (name, dsn_id),
                FOREIGN KEY (dsn_id) REFERENCES dsns(id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_restaurants (
                user_id INTEGER NOT NULL,
                restaurant_id INTEGER NOT NULL,
                UNIQUE (user_id, restaurant_id),
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (restaurant_id) REFERENCES restaurants(id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                chat_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                selected_restaurants TEXT,
                language TEXT,
                include_sql INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            """
        )
        # lightweight migration for existing DBs
        cur.execute("PRAGMA table_info(sessions)")
        cols = {row[1] for row in cur.fetchall()}
        if "language" not in cols:
            cur.execute("ALTER TABLE sessions ADD COLUMN language TEXT;")
        if "include_sql" not in cols:
            cur.execute("ALTER TABLE sessions ADD COLUMN include_sql INTEGER;")
        conn.commit()


def create_dsn(name: str, dsn: str, db_path: str = DEFAULT_DB_PATH) -> int:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO dsns (name, dsn) VALUES (?, ?)", (name, dsn))
        conn.commit()
        return int(cur.lastrowid)


def get_dsn_by_name(name: str, db_path: str = DEFAULT_DB_PATH) -> Optional[Dict[str, Any]]:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM dsns WHERE name = ?", (name,))
        row = cur.fetchone()
        return dict(row) if row else None


def list_dsns(db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM dsns ORDER BY name")
        return [dict(r) for r in cur.fetchall()]


def get_dsn_by_id(dsn_id: int, db_path: str = DEFAULT_DB_PATH) -> Optional[Dict[str, Any]]:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM dsns WHERE id = ?", (dsn_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def sync_restaurants_from_dsn(dsn_id: int, db_path: str = DEFAULT_DB_PATH) -> int:
    dsn = get_dsn_by_id(dsn_id, db_path=db_path)
    if not dsn:
        raise ValueError("DSN not found")
    dsn_value = dsn["dsn"]
    with psycopg.connect(dsn_value) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT restaurant FROM sales ORDER BY restaurant;")
            rows = cur.fetchall()

    names = [r[0] for r in rows if r and r[0] is not None]
    with _connect(db_path) as conn:
        cur = conn.cursor()
        for name in names:
            cur.execute(
                "INSERT OR IGNORE INTO restaurants (name, dsn_id) VALUES (?, ?)",
                (name, dsn_id),
            )
        conn.commit()
    return len(names)


def list_restaurants_by_dsn(dsn_id: int, db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM restaurants WHERE dsn_id = ? ORDER BY name", (dsn_id,))
        return [dict(r) for r in cur.fetchall()]


def create_user(
    email: str,
    password_hash: str,
    role: str,
    dsn_id: Optional[int],
    db_path: str = DEFAULT_DB_PATH,
) -> int:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (email, password_hash, role, dsn_id) VALUES (?, ?, ?, ?)",
            (email, password_hash, role, dsn_id),
        )
        conn.commit()
        return int(cur.lastrowid)


def get_user_by_email(email: str, db_path: str = DEFAULT_DB_PATH) -> Optional[User]:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE email = ?", (email,))
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


def get_user_by_id(user_id: int, db_path: str = DEFAULT_DB_PATH) -> Optional[User]:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE id = ?", (user_id,))
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


def set_user_restaurants(user_id: int, restaurant_ids: List[int], db_path: str = DEFAULT_DB_PATH) -> None:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM user_restaurants WHERE user_id = ?", (user_id,))
        for rid in restaurant_ids:
            cur.execute(
                "INSERT OR IGNORE INTO user_restaurants (user_id, restaurant_id) VALUES (?, ?)",
                (user_id, rid),
            )
        conn.commit()


def list_user_restaurants(user_id: int, db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT r.* FROM restaurants r
            JOIN user_restaurants ur ON ur.restaurant_id = r.id
            WHERE ur.user_id = ?
            ORDER BY r.name
            """,
            (user_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def list_accessible_restaurants(user: User, db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    if user.dsn_id is None:
        return []
    restricted = list_user_restaurants(user.id, db_path=db_path)
    if restricted:
        return restricted
    return list_restaurants_by_dsn(user.dsn_id, db_path=db_path)


def set_session(chat_id: int, user_id: int, selected_restaurants: Optional[str], db_path: str = DEFAULT_DB_PATH) -> None:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO sessions (chat_id, user_id, selected_restaurants) VALUES (?, ?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET user_id = excluded.user_id, selected_restaurants = excluded.selected_restaurants",
            (chat_id, user_id, selected_restaurants),
        )
        conn.commit()


def set_session_language(chat_id: int, language: str, db_path: str = DEFAULT_DB_PATH) -> None:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("UPDATE sessions SET language = ? WHERE chat_id = ?", (language, chat_id))
        conn.commit()


def set_session_include_sql(chat_id: int, include_sql: bool, db_path: str = DEFAULT_DB_PATH) -> None:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE sessions SET include_sql = ? WHERE chat_id = ?",
            (1 if include_sql else 0, chat_id),
        )
        conn.commit()


def clear_session(chat_id: int, db_path: str = DEFAULT_DB_PATH) -> None:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM sessions WHERE chat_id = ?", (chat_id,))
        conn.commit()


def get_session(chat_id: int, db_path: str = DEFAULT_DB_PATH) -> Optional[Dict[str, Any]]:
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM sessions WHERE chat_id = ?", (chat_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_restaurant_ids_by_names(dsn_id: int, names: List[str], db_path: str = DEFAULT_DB_PATH) -> List[int]:
    if not names:
        return []
    with _connect(db_path) as conn:
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in names)
        cur.execute(
            f"SELECT id FROM restaurants WHERE dsn_id = ? AND name IN ({placeholders})",
            [dsn_id] + names,
        )
        return [r["id"] for r in cur.fetchall()]
