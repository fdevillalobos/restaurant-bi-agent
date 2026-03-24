from __future__ import annotations

import logging
import os
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from psycopg.errors import UniqueViolation
from telegram import Update
from telegram.error import Conflict
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

from app.analyst import generate_answer, format_table, detect_language
from app.auth import hash_password, verify_password
from app.charting import make_chart
from app.db import run_select, DatabaseError
from app.llm_planner import question_to_sql
from app.tenant_store import (
    init_db,
    get_user_by_email,
    get_user_by_id,
    set_session,
    get_session,
    clear_session,
    list_dsns,
    create_dsn,
    sync_restaurants_from_dsn,
    list_accessible_restaurants,
    list_restaurants_by_dsn,
    get_dsn_by_id,
    create_user,
    set_user_restaurants,
    set_session_language,
    set_session_include_sql,
    get_conversation_history,
    append_conversation,
    clear_conversation_history,
    list_users,
    list_user_restaurants,
    update_user_password,
    update_user_role,
    update_user_dsn,
)


LOGIN_EMAIL, LOGIN_PASSWORD = range(2)
ADD_DSN_NAME, ADD_DSN_VALUE, ADD_DSN_CONFIRM = range(2, 5)
ADD_USER_EMAIL, ADD_USER_PASSWORD, ADD_USER_ROLE, ADD_USER_DSN, ADD_USER_LIMIT, ADD_USER_RESTAURANTS, ADD_USER_CONFIRM = range(5, 12)
RESTAURANT_SELECT = 12
EDIT_USER_EMAIL, EDIT_USER_FIELD, EDIT_USER_VALUE, EDIT_USER_CONFIRM = range(13, 17)


def _token() -> str:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set in environment")
    return token


def _session_user(chat_id: int) -> Optional[int]:
    sess = get_session(chat_id)
    if not sess:
        return None
    return sess.get("user_id")


def _selected_restaurants(chat_id: int) -> List[str]:
    sess = get_session(chat_id)
    if not sess:
        return []
    raw = sess.get("selected_restaurants") or ""
    return [r.strip() for r in raw.split(",") if r.strip()]


def _session_language(chat_id: int) -> str:
    sess = get_session(chat_id)
    if not sess:
        return "en"
    lang = sess.get("language") or "en"
    return "es" if lang == "es" else "en"


def _session_include_sql(chat_id: int) -> bool:
    sess = get_session(chat_id)
    if not sess:
        return False
    val = sess.get("include_sql")
    return bool(val) if val is not None else False


def _set_selected_restaurants(chat_id: int, user_id: int, restaurants: List[str]) -> None:
    raw = ", ".join(restaurants) if restaurants else None
    set_session(chat_id, user_id, raw)


def _parse_csv(text: str) -> List[str]:
    return [t.strip() for t in (text or "").split(",") if t.strip()]


def _apply_restaurant_scope(sql: str, restaurants: List[str]) -> Tuple[str, dict]:
    if not restaurants:
        return sql, {}
    if len(restaurants) == 1:
        return sql, {"restaurant": restaurants[0]}

    # Try to replace common equality filter with ANY for multiple restaurants
    updated = sql
    updated = updated.replace("= %(restaurant)s", "= ANY(%(restaurants)s)")
    updated = updated.replace("= LOWER(%(restaurant)s)", "= ANY(%(restaurants)s)")
    params = {"restaurants": restaurants}
    return updated, params


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Welcome to the Restaurant BI bot.\n"
        "Use /login to authenticate, then ask your question."
    )


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user_id = _session_user(chat_id)
    if not user_id:
        await update.message.reply_text(
            "Commands:\n"
            "/login\n"
            "/logout\n"
            "/menu"
        )
        return

    user = get_user_by_id(user_id)
    if not user:
        await update.message.reply_text("Session invalid. Please /login again.")
        return

    base_cmds = ["/restaurants", "/language", "/debug", "/reset", "/whoami", "/logout", "/menu", "/help"]
    admin_cmds = []
    if user.role == "superuser":
        admin_cmds.extend(["/add_dsn", "/add_user", "/list_users", "/edit_user"])
    elif user.role == "admin":
        admin_cmds.extend(["/add_user", "/list_users", "/edit_user"])

    msg = "Commands:\n" + "\n".join(base_cmds + admin_cmds)
    await update.message.reply_text(msg)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await menu(update, context)


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user_id = _session_user(chat_id)
    if not user_id:
        await update.message.reply_text("You are not logged in. Use /login.")
        return
    user = get_user_by_id(user_id)
    if not user:
        await update.message.reply_text("Session invalid. Please /login again.")
        return
    dsn = get_dsn_by_id(user.dsn_id) if user.dsn_id else None
    selected = _selected_restaurants(chat_id)
    msg = (
        f"Email: {user.email}\n"
        f"Role: {user.role}\n"
        f"DSN: {dsn['name'] if dsn else 'none'}\n"
        f"Selected restaurants: {', '.join(selected) if selected else 'none'}\n"
        f"Language: {_session_language(chat_id)}\n"
        f"Include SQL: {'on' if _session_include_sql(chat_id) else 'off'}"
    )
    await update.message.reply_text(msg)


async def language(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user_id = _session_user(chat_id)
    if not user_id:
        await update.message.reply_text("Please /login first.")
        return
    text = (update.message.text or "").strip().lower()
    parts = text.split()
    if len(parts) == 1:
        await update.message.reply_text("Usage: /language en OR /language es")
        return
    lang = parts[1]
    if lang not in ("en", "es"):
        await update.message.reply_text("Invalid language. Use /language en or /language es.")
        return
    set_session_language(chat_id, lang)
    await update.message.reply_text(f"Language set to {lang}.")


async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user_id = _session_user(chat_id)
    if not user_id:
        await update.message.reply_text("Please /login first.")
        return
    text = (update.message.text or "").strip().lower()
    parts = text.split()
    if len(parts) == 1:
        await update.message.reply_text("Usage: /debug on OR /debug off")
        return
    flag = parts[1]
    if flag not in ("on", "off"):
        await update.message.reply_text("Invalid option. Use /debug on or /debug off.")
        return
    set_session_include_sql(chat_id, flag == "on")
    await update.message.reply_text(f"Debug SQL is now {flag}.")


async def login_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    if _session_user(chat_id):
        await update.message.reply_text("You are already logged in. Use /logout to end the session.")
        return ConversationHandler.END
    await update.message.reply_text("Email:")
    return LOGIN_EMAIL


async def login_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["email"] = (update.message.text or "").strip()
    await update.message.reply_text("Password:")
    return LOGIN_PASSWORD


async def login_password(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    email = context.user_data.get("email")
    password = update.message.text or ""
    user = get_user_by_email(email)
    if not user or not verify_password(password, user.password_hash):
        await update.message.reply_text("Invalid credentials. Try /login again.")
        return ConversationHandler.END
    set_session(update.effective_chat.id, user.id, None)
    await update.message.reply_text(
        "Logged in.\n"
        "Next, choose a restaurant with /restaurants."
    )
    return ConversationHandler.END


async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    clear_session(update.effective_chat.id)
    await update.message.reply_text("Logged out.")


async def restaurants_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = _session_user(update.effective_chat.id)
    if not user_id:
        await update.message.reply_text("Please /login first.")
        return ConversationHandler.END
    user = get_user_by_id(user_id)
    if not user:
        await update.message.reply_text("Session invalid. Please /login again.")
        return ConversationHandler.END
    restaurants = list_accessible_restaurants(user)
    if not restaurants:
        await update.message.reply_text("No restaurants available for your account.")
        return ConversationHandler.END
    names = [r["name"] for r in restaurants]
    await update.message.reply_text(
        "Select restaurant(s) by replying with a comma-separated list of names or numbers:\n"
        + "\n".join([f"{i+1}. {n}" for i, n in enumerate(names)])
    )
    return RESTAURANT_SELECT


async def restaurants_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = _session_user(update.effective_chat.id)
    if not user_id:
        await update.message.reply_text("Please /login first.")
        return ConversationHandler.END
    user = get_user_by_id(user_id)
    if not user:
        await update.message.reply_text("Session invalid. Please /login again.")
        return ConversationHandler.END
    restaurants = list_accessible_restaurants(user)
    names = [r["name"] for r in restaurants]
    allowed = {r["name"] for r in restaurants}
    raw = _parse_csv(update.message.text or "")
    selected: List[str] = []
    for token in raw:
        if token.isdigit():
            idx = int(token) - 1
            if 0 <= idx < len(names):
                selected.append(names[idx])
        elif token in allowed:
            selected.append(token)
    selected = list(dict.fromkeys(selected))
    if not selected:
        await update.message.reply_text("No valid restaurants selected. Try /restaurants again.")
        return ConversationHandler.END
    _set_selected_restaurants(update.effective_chat.id, user_id, selected)
    await update.message.reply_text(
        f"Selected restaurants: {', '.join(selected)}\n\n"
        "You can now ask questions in English or Spanish, for example:\n"
        "\"What was the best selling product last week?\""
    )
    return ConversationHandler.END


async def add_dsn_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = _session_user(update.effective_chat.id)
    user = get_user_by_id(user_id) if user_id else None
    if not user or user.role != "superuser":
        await update.message.reply_text("Unauthorized.")
        return ConversationHandler.END
    await update.message.reply_text("DSN name:")
    return ADD_DSN_NAME


async def add_dsn_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["dsn_name"] = (update.message.text or "").strip()
    await update.message.reply_text("DSN value:")
    return ADD_DSN_VALUE


async def add_dsn_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["dsn_value"] = (update.message.text or "").strip()
    await update.message.reply_text("Create DSN and sync restaurants? (yes/no)")
    return ADD_DSN_CONFIRM


async def add_dsn_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    answer = (update.message.text or "").strip().lower()
    if answer not in ("yes", "y"):
        await update.message.reply_text("Canceled.")
        return ConversationHandler.END
    name = context.user_data.get("dsn_name")
    dsn_value = context.user_data.get("dsn_value")
    dsn_id = create_dsn(name, dsn_value)
    count = sync_restaurants_from_dsn(dsn_id)
    await update.message.reply_text(f"DSN created. Synced {count} restaurants.")
    return ConversationHandler.END


async def add_user_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = _session_user(update.effective_chat.id)
    user = get_user_by_id(user_id) if user_id else None
    if not user or user.role not in ("superuser", "admin"):
        await update.message.reply_text("Unauthorized.")
        return ConversationHandler.END
    await update.message.reply_text("New user email:")
    return ADD_USER_EMAIL


async def add_user_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["new_email"] = (update.message.text or "").strip()
    await update.message.reply_text("New user password:")
    return ADD_USER_PASSWORD


async def add_user_password(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["new_password"] = (update.message.text or "").strip()
    await update.message.reply_text("Role:\n1. admin\n2. db_admin\n3. user")
    return ADD_USER_ROLE


async def add_user_role(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _role_map = {"1": "admin", "2": "db_admin", "3": "user"}
    text = (update.message.text or "").strip()
    role = _role_map.get(text) or (text if text in _role_map.values() else None)
    if not role:
        await update.message.reply_text("Invalid choice. Reply with 1 (admin), 2 (db_admin), or 3 (user).")
        return ADD_USER_ROLE
    context.user_data["new_role"] = role
    admin_user_id = _session_user(update.effective_chat.id)
    admin_user = get_user_by_id(admin_user_id) if admin_user_id else None
    if admin_user and admin_user.role == "admin":
        # admins can only create users for their own DSN
        if admin_user.dsn_id is None:
            await update.message.reply_text("Your account has no DSN assigned.")
            return ConversationHandler.END
        context.user_data["new_dsn_id"] = admin_user.dsn_id
        await update.message.reply_text("Limit restaurant access? (yes/no)")
        return ADD_USER_LIMIT

    dsns = list_dsns()
    if not dsns:
        await update.message.reply_text("No DSNs available. Add one with /add_dsn.")
        return ConversationHandler.END
    msg = "Select DSN by number:\n" + "\n".join([f"{i+1}. {d['name']}" for i, d in enumerate(dsns)])
    context.user_data["dsn_list"] = dsns
    await update.message.reply_text(msg)
    return ADD_USER_DSN


async def add_user_dsn(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    try:
        idx = int(text) - 1
        dsns = context.user_data.get("dsn_list", [])
        dsn = dsns[idx]
    except Exception:
        await update.message.reply_text("Invalid selection. Try again.")
        return ADD_USER_DSN
    context.user_data["new_dsn_id"] = dsn["id"]
    await update.message.reply_text("Limit restaurant access? (yes/no)")
    return ADD_USER_LIMIT


async def add_user_limit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    answer = (update.message.text or "").strip().lower()
    if answer in ("no", "n"):
        context.user_data["limit_restaurants"] = False
        await update.message.reply_text("Confirm create user? (yes/no)")
        return ADD_USER_CONFIRM
    if answer in ("yes", "y"):
        context.user_data["limit_restaurants"] = True
        dsn_id = context.user_data.get("new_dsn_id")
        restaurants = list_restaurants_by_dsn(dsn_id)
        if not restaurants:
            await update.message.reply_text("No restaurants found for this DSN.")
            return ConversationHandler.END
        msg = "List allowed restaurants (comma-separated):\n" + "\n".join([r["name"] for r in restaurants])
        await update.message.reply_text(msg)
        return ADD_USER_RESTAURANTS
    await update.message.reply_text("Please answer yes or no.")
    return ADD_USER_LIMIT


async def add_user_restaurants(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    names = _parse_csv(update.message.text or "")
    context.user_data["new_restaurants"] = names
    await update.message.reply_text("Confirm create user? (yes/no)")
    return ADD_USER_CONFIRM


async def add_user_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    answer = (update.message.text or "").strip().lower()
    if answer not in ("yes", "y"):
        await update.message.reply_text("Canceled.")
        return ConversationHandler.END
    email = context.user_data.get("new_email")
    password = context.user_data.get("new_password")
    role = context.user_data.get("new_role")
    dsn_id = context.user_data.get("new_dsn_id")

    pwd_hash = hash_password(password)
    try:
        user_id = create_user(email, pwd_hash, role=role, dsn_id=dsn_id)
    except UniqueViolation:
        await update.message.reply_text(
            "A user with that email already exists. Use /edit_user to modify an existing user."
        )
        return ConversationHandler.END

    if context.user_data.get("limit_restaurants"):
        names = context.user_data.get("new_restaurants", [])
        restaurants = list_restaurants_by_dsn(dsn_id)
        allowed_map = {r["name"]: r["id"] for r in restaurants}
        ids = [allowed_map[n] for n in names if n in allowed_map]
        set_user_restaurants(user_id, ids)

    await update.message.reply_text("User created.")
    return ConversationHandler.END


async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clear the conversation history for this session."""
    chat_id = update.effective_chat.id
    if not _session_user(chat_id):
        await update.message.reply_text("You are not logged in.")
        return
    clear_conversation_history(chat_id)
    await update.message.reply_text("Conversation history cleared. Starting fresh.")


async def handle_question(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if not update.message or not update.message.text:
        return
    user_id = _session_user(chat_id)
    if not user_id:
        await update.message.reply_text("Please /login first.")
        return
    user = get_user_by_id(user_id)
    if not user or user.dsn_id is None:
        await update.message.reply_text("No DSN assigned. Contact admin.")
        return
    restaurants = _selected_restaurants(chat_id)
    if not restaurants:
        await update.message.reply_text("Select restaurant with /restaurants first.")
        return

    dsn = get_dsn_by_id(user.dsn_id)
    if not dsn:
        await update.message.reply_text("DSN not found.")
        return

    question = update.message.text or ""
    language = detect_language(question)
    history = get_conversation_history(chat_id)

    try:
        plan = question_to_sql(question, restaurant=restaurants[0], history=history)
    except Exception as e:
        msg = f"Could not generate a query: {e}"
        if language == "es":
            msg = f"No pude generar la consulta: {e}"
        await update.message.reply_text(msg)
        return

    sql, params = _apply_restaurant_scope(plan.sql, restaurants)
    if "restaurant" not in params and "restaurants" not in params:
        params = {"restaurant": restaurants[0]}

    try:
        rows = run_select(
            sql,
            params=params,
            preview=True,
            statement_timeout_ms=int(os.getenv("STATEMENT_TIMEOUT_MS_ASK", "30000")),
            dsn=dsn["dsn"],
        )

        table = format_table(rows)
        insight = generate_answer(question, rows, language=language, history=history)

        if table:
            await update.message.reply_text(
                f"<pre>{table}</pre>", parse_mode="HTML"
            )

        reply = insight
        if _session_include_sql(chat_id):
            reply = f"{reply}\n\nSQL:\n<pre>{sql}</pre>"
            await update.message.reply_text(reply, parse_mode="HTML")
        else:
            await update.message.reply_text(reply)

        answer = f"{table}\n\n{insight}" if table else insight

        # Send chart if the data warrants one
        chart_bytes = make_chart(rows, question)
        if chart_bytes:
            import io
            await update.message.reply_photo(
                photo=io.BytesIO(chart_bytes),
                caption="Chart",
            )

        # Persist the exchange to conversation history
        append_conversation(chat_id, question, answer)

    except DatabaseError as e:
        msg = "Query failed. Please try again or contact support."
        if language == "es":
            msg = "La consulta falló. Intenta de nuevo o contacta soporte."
        if _session_include_sql(chat_id):
            msg = f"{msg}\n\nError: {e}\n\nSQL:\n```\n{sql}\n```"
        await update.message.reply_text(msg)


async def list_users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user_id = _session_user(chat_id)
    user = get_user_by_id(user_id) if user_id else None
    if not user or user.role not in ("superuser", "admin"):
        await update.message.reply_text("Unauthorized.")
        return

    dsn_filter = None if user.role == "superuser" else user.dsn_id
    users = list_users(dsn_id=dsn_filter)

    if not users:
        await update.message.reply_text("No users found.")
        return

    for i in range(0, len(users), 10):
        batch = users[i:i + 10]
        lines = []
        for u in batch:
            restaurants = ", ".join(u["restaurants"]) if u["restaurants"] else "(all)"
            lines.append(
                f"{u['email']} — {u['role']}\n"
                f"DSN: {u['dsn_name'] or '(none)'}\n"
                f"Restaurants: {restaurants}"
            )
        await update.message.reply_text("\n\n".join(lines))


async def edit_user_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = _session_user(update.effective_chat.id)
    user = get_user_by_id(user_id) if user_id else None
    if not user or user.role not in ("superuser", "admin"):
        await update.message.reply_text("Unauthorized.")
        return ConversationHandler.END
    await update.message.reply_text("Email of user to edit:")
    return EDIT_USER_EMAIL


async def edit_user_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    editor_id = _session_user(update.effective_chat.id)
    editor = get_user_by_id(editor_id)
    if not editor:
        await update.message.reply_text("Session expired. Please /login again.")
        return ConversationHandler.END
    email = (update.message.text or "").strip()
    target = get_user_by_email(email)

    if not target:
        await update.message.reply_text("User not found.")
        return ConversationHandler.END

    if editor.role == "admin":
        if target.dsn_id != editor.dsn_id or target.role in ("admin", "superuser"):
            await update.message.reply_text("Unauthorized.")
            return ConversationHandler.END

    context.user_data["edit_target"] = target

    dsn = get_dsn_by_id(target.dsn_id) if target.dsn_id else None
    restaurants = list_user_restaurants(target.id)
    rest_names = ", ".join(r["name"] for r in restaurants) if restaurants else "(all)"
    info = (
        f"User: {target.email}\n"
        f"Role: {target.role}\n"
        f"DSN: {dsn['name'] if dsn else '(none)'}\n"
        f"Restaurants: {rest_names}"
    )

    field_options = ["password", "role", "restaurants"]
    if editor.role == "superuser":
        field_options.append("dsn")
    context.user_data["field_options"] = field_options
    numbered = "\n".join(f"{i + 1}. {f}" for i, f in enumerate(field_options))
    await update.message.reply_text(f"{info}\n\nWhat would you like to change?\n{numbered}")
    return EDIT_USER_FIELD


async def edit_user_field(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    editor_id = _session_user(update.effective_chat.id)
    editor = get_user_by_id(editor_id)
    if not editor:
        await update.message.reply_text("Session expired. Please /login again.")
        return ConversationHandler.END
    target = context.user_data["edit_target"]
    field_options = context.user_data.get("field_options", ["password", "role", "restaurants"])
    text = (update.message.text or "").strip()

    # Accept number or field name
    try:
        field = field_options[int(text) - 1]
    except (ValueError, IndexError):
        field = text.lower()

    if field not in field_options:
        numbered = "\n".join(f"{i + 1}. {f}" for i, f in enumerate(field_options))
        await update.message.reply_text(f"Invalid choice. Pick a number:\n{numbered}")
        return EDIT_USER_FIELD

    context.user_data["edit_field"] = field

    if field == "password":
        await update.message.reply_text("New password:")

    elif field == "role":
        if editor.role == "superuser":
            roles = ["user", "db_admin", "admin", "superuser"]
        else:
            roles = ["user", "db_admin"]
        context.user_data["role_options"] = roles
        numbered = "\n".join(f"{i + 1}. {r}" for i, r in enumerate(roles))
        await update.message.reply_text(f"New role:\n{numbered}")

    elif field == "restaurants":
        if not target.dsn_id:
            await update.message.reply_text(
                "Cannot set restaurant restrictions: this user has no DSN assigned. Assign a DSN first."
            )
            return ConversationHandler.END
        available = list_restaurants_by_dsn(target.dsn_id)
        names_list = "\n".join(r["name"] for r in available)
        await update.message.reply_text(
            f"Enter restaurant names (comma-separated), or 'all' to remove restrictions:\n{names_list}"
        )

    elif field == "dsn":
        dsns = list_dsns()
        if not dsns:
            await update.message.reply_text("No DSNs available.")
            return ConversationHandler.END
        context.user_data["dsn_list"] = dsns
        options = "0. (none)\n" + "\n".join(f"{i + 1}. {d['name']}" for i, d in enumerate(dsns))
        await update.message.reply_text(f"Select DSN by number:\n{options}")

    return EDIT_USER_VALUE


async def edit_user_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    editor_id = _session_user(update.effective_chat.id)
    editor = get_user_by_id(editor_id)
    if not editor:
        await update.message.reply_text("Session expired. Please /login again.")
        return ConversationHandler.END
    field = context.user_data["edit_field"]
    target = context.user_data["edit_target"]
    text = (update.message.text or "").strip()

    if field == "password":
        context.user_data["edit_value"] = text
        await update.message.reply_text(
            f"Confirm: set new password for {target.email}? (yes/no)"
        )
        return EDIT_USER_CONFIRM

    elif field == "role":
        role_options = context.user_data.get("role_options", ["user", "db_admin"])
        try:
            role = role_options[int(text) - 1]
        except (ValueError, IndexError):
            role = text.lower()
        if role not in role_options:
            numbered = "\n".join(f"{i + 1}. {r}" for i, r in enumerate(role_options))
            await update.message.reply_text(f"Invalid choice. Pick a number:\n{numbered}")
            return EDIT_USER_VALUE
        context.user_data["edit_value"] = role
        text = role  # use resolved role name in confirmation below
        await update.message.reply_text(
            f"Confirm: change {target.email}'s role to '{text}'? (yes/no)"
        )
        return EDIT_USER_CONFIRM

    elif field == "restaurants":
        if text.lower() == "all":
            context.user_data["edit_value"] = []
            await update.message.reply_text(
                f"Confirm: remove all restaurant restrictions for {target.email}? (yes/no)"
            )
            return EDIT_USER_CONFIRM

        names = _parse_csv(text)
        available = list_restaurants_by_dsn(target.dsn_id)
        valid_map = {r["name"]: r["id"] for r in available}
        skipped = [n for n in names if n not in valid_map]
        kept = [n for n in names if n in valid_map]
        ids = [valid_map[n] for n in kept]

        if not kept:
            names_list = "\n".join(r["name"] for r in available)
            await update.message.reply_text(
                f"None of the entered names were found. Enter restaurant names (comma-separated), or 'all' to remove restrictions:\n{names_list}"
            )
            return EDIT_USER_VALUE

        context.user_data["edit_value"] = ids
        warning = f"\nWarning: not found and skipped: {', '.join(skipped)}" if skipped else ""
        await update.message.reply_text(
            f"Confirm: restrict {target.email} to: {', '.join(kept)}?{warning} (yes/no)"
        )
        return EDIT_USER_CONFIRM

    elif field == "dsn":
        dsns = context.user_data.get("dsn_list", [])
        try:
            idx = int(text)
            if idx == 0:
                context.user_data["edit_value"] = None
                await update.message.reply_text(
                    f"Confirm: unassign DSN from {target.email}? (yes/no)"
                )
            else:
                dsn = dsns[idx - 1]
                context.user_data["edit_value"] = dsn["id"]
                await update.message.reply_text(
                    f"Confirm: set {target.email}'s DSN to '{dsn['name']}'? (yes/no)"
                )
        except (ValueError, IndexError):
            options = "0. (none)\n" + "\n".join(
                f"{i + 1}. {d['name']}" for i, d in enumerate(dsns)
            )
            await update.message.reply_text(f"Invalid selection. Choose:\n{options}")
            return EDIT_USER_VALUE
        return EDIT_USER_CONFIRM

    return ConversationHandler.END


async def edit_user_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    answer = (update.message.text or "").strip().lower()
    if answer not in ("yes", "y"):
        await update.message.reply_text("Cancelled.")
        return ConversationHandler.END

    field = context.user_data["edit_field"]
    target = context.user_data["edit_target"]
    value = context.user_data["edit_value"]

    if field == "password":
        update_user_password(target.id, hash_password(value))
    elif field == "role":
        update_user_role(target.id, value)
    elif field == "restaurants":
        set_user_restaurants(target.id, value)
    elif field == "dsn":
        update_user_dsn(target.id, value)

    await update.message.reply_text("Done.")
    return ConversationHandler.END


async def handle_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if isinstance(context.error, Conflict):
        logger.warning("Conflict: another bot instance is running — retrying...")
        return
    logger.error("Unhandled error: %s", context.error, exc_info=context.error)


def build_app():
    load_dotenv()
    init_db()
    async def post_init(app):
        commands = [
            ("start", "Start the bot"),
            ("login", "Log in"),
            ("logout", "Log out"),
            ("restaurants", "Select restaurant(s)"),
            ("language", "Set language (en/es)"),
            ("debug", "Toggle SQL output (on/off)"),
            ("reset", "Clear conversation history"),
            ("menu", "Show available commands"),
            ("help", "Show available commands"),
            ("whoami", "Show current session info"),
            ("add_user", "Create a user (admin/superuser)"),
            ("add_dsn", "Add a DSN (superuser)"),
            ("list_users", "List all users (admin/superuser)"),
            ("edit_user", "Edit a user (admin/superuser)"),
        ]
        await app.bot.set_my_commands(commands)

    app = ApplicationBuilder().token(_token()).post_init(post_init).build()
    app.add_error_handler(handle_error)
    return app


def main() -> None:
    app = build_app()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("language", language))
    app.add_handler(CommandHandler("debug", debug))
    app.add_handler(CommandHandler("menu", menu))
    app.add_handler(CommandHandler("logout", logout))
    app.add_handler(CommandHandler("reset", reset))
    app.add_handler(CommandHandler("list_users", list_users_cmd))

    login_conv = ConversationHandler(
        entry_points=[CommandHandler("login", login_start)],
        states={
            LOGIN_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, login_email)],
            LOGIN_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, login_password)],
        },
        fallbacks=[],
    )
    app.add_handler(login_conv)

    restaurants_conv = ConversationHandler(
        entry_points=[CommandHandler("restaurants", restaurants_start)],
        states={RESTAURANT_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, restaurants_select)]},
        fallbacks=[],
    )
    app.add_handler(restaurants_conv)

    add_dsn_conv = ConversationHandler(
        entry_points=[CommandHandler("add_dsn", add_dsn_start)],
        states={
            ADD_DSN_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_dsn_name)],
            ADD_DSN_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_dsn_value)],
            ADD_DSN_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_dsn_confirm)],
        },
        fallbacks=[],
    )
    app.add_handler(add_dsn_conv)

    add_user_conv = ConversationHandler(
        entry_points=[CommandHandler("add_user", add_user_start)],
        states={
            ADD_USER_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_user_email)],
            ADD_USER_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_user_password)],
            ADD_USER_ROLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_user_role)],
            ADD_USER_DSN: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_user_dsn)],
            ADD_USER_LIMIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_user_limit)],
            ADD_USER_RESTAURANTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_user_restaurants)],
            ADD_USER_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_user_confirm)],
        },
        fallbacks=[],
    )
    app.add_handler(add_user_conv)

    edit_user_conv = ConversationHandler(
        entry_points=[CommandHandler("edit_user", edit_user_start)],
        states={
            EDIT_USER_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_user_email)],
            EDIT_USER_FIELD: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_user_field)],
            EDIT_USER_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_user_value)],
            EDIT_USER_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_user_confirm)],
        },
        fallbacks=[],
    )
    app.add_handler(edit_user_conv)

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_question))

    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
