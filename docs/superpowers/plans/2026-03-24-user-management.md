# User Management Improvements Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `/list_users` and `/edit_user` commands to the Telegram bot, and fix the silent crash when creating a duplicate user.

**Architecture:** Four new functions in `tenant_store.py` provide the data layer. Three changes to `telegram_bot.py` add the UX: a try/except for duplicate users in the existing `add_user_confirm`, a new `list_users_cmd` handler, and a new `edit_user` ConversationHandler with four states (13–16).

**Tech Stack:** Python 3.11, python-telegram-bot 21, psycopg3, Postgres

---

## Chunk 1: tenant_store.py — four new functions

### Task 1: Add `list_users`, `update_user_password`, `update_user_role`, `update_user_dsn`

**Files:**
- Modify: `app/tenant_store.py`

- [ ] **Step 1: Add the four functions to the end of `app/tenant_store.py`**

```python
def list_users(dsn_id: Optional[int] = None, db_dsn: str = DEFAULT_DB_DSN) -> List[Dict[str, Any]]:
    with _connect(db_dsn) as conn:
        with conn.cursor() as cur:
            if dsn_id is not None:
                cur.execute(
                    """
                    SELECT u.id, u.email, u.role, u.dsn_id,
                           COALESCE(d.name, 'none') AS dsn_name
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
                           COALESCE(d.name, 'none') AS dsn_name
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
```

- [ ] **Step 2: Verify the module imports without error**

```bash
CONTROL_DB_DSN="postgresql://x:x@localhost/x" .venv/bin/python -c "import app.tenant_store; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add app/tenant_store.py
git commit -m "feat: add list_users, update_user_password, update_user_role, update_user_dsn"
```

---

## Chunk 2: telegram_bot.py — all three UX changes

### Task 2: Update imports and state constants

**Files:**
- Modify: `app/telegram_bot.py`

- [ ] **Step 1: Add `UniqueViolation` to psycopg imports at the top of the file**

Find the existing psycopg import area (currently only `from psycopg.rows import dict_row` is imported indirectly via tenant_store). Add at the top of the file after the existing imports:

```python
from psycopg.errors import UniqueViolation
```

- [ ] **Step 2: Add new state constants after the existing `RESTAURANT_SELECT = 12` line**

```python
EDIT_USER_EMAIL, EDIT_USER_FIELD, EDIT_USER_VALUE, EDIT_USER_CONFIRM = range(13, 17)
```

- [ ] **Step 3: Add new tenant_store imports**

Find the existing `from app.tenant_store import (` block and add these to the list:

```python
    list_users,
    list_user_restaurants,
    update_user_password,
    update_user_role,
    update_user_dsn,
```

---

### Task 3: Fix duplicate user error in `add_user_confirm`

**Files:**
- Modify: `app/telegram_bot.py`

- [ ] **Step 1: Wrap `create_user` in `add_user_confirm` with a try/except**

Find the existing `add_user_confirm` function. Replace:

```python
    pwd_hash = hash_password(password)
    user_id = create_user(email, pwd_hash, role=role, dsn_id=dsn_id)
```

With:

```python
    pwd_hash = hash_password(password)
    try:
        user_id = create_user(email, pwd_hash, role=role, dsn_id=dsn_id)
    except UniqueViolation:
        await update.message.reply_text(
            "A user with that email already exists. Use /edit_user to modify an existing user."
        )
        return ConversationHandler.END
```

---

### Task 4: Add `/list_users` command handler

**Files:**
- Modify: `app/telegram_bot.py`

- [ ] **Step 1: Add `list_users_cmd` function before `build_app`**

```python
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
                f"DSN: {u['dsn_name']}\n"
                f"Restaurants: {restaurants}"
            )
        await update.message.reply_text("\n\n".join(lines))
```

---

### Task 5: Add `/edit_user` conversation handler

**Files:**
- Modify: `app/telegram_bot.py`

- [ ] **Step 1: Add all `/edit_user` handler functions before `build_app`**

```python
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

    fields = "password / role / restaurants"
    if editor.role == "superuser":
        fields += " / dsn"

    await update.message.reply_text(f"{info}\n\nWhat would you like to change? ({fields})")
    return EDIT_USER_FIELD


async def edit_user_field(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    editor_id = _session_user(update.effective_chat.id)
    editor = get_user_by_id(editor_id)
    target = context.user_data["edit_target"]
    field = (update.message.text or "").strip().lower()

    allowed_fields = {"password", "role", "restaurants"}
    if editor.role == "superuser":
        allowed_fields.add("dsn")

    if field not in allowed_fields:
        await update.message.reply_text(
            f"Invalid field. Choose: {' / '.join(sorted(allowed_fields))}"
        )
        return EDIT_USER_FIELD

    context.user_data["edit_field"] = field

    if field == "password":
        await update.message.reply_text("New password:")

    elif field == "role":
        if editor.role == "superuser":
            await update.message.reply_text("New role (user / db_admin / admin / superuser):")
        else:
            await update.message.reply_text("New role (user / db_admin):")

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
        allowed_roles = (
            {"user", "db_admin", "admin", "superuser"}
            if editor.role == "superuser"
            else {"user", "db_admin"}
        )
        if text not in allowed_roles:
            await update.message.reply_text(
                f"Invalid role. Choose: {' / '.join(sorted(allowed_roles))}"
            )
            return EDIT_USER_VALUE
        context.user_data["edit_value"] = text
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

        context.user_data["edit_value"] = ids
        confirm_str = ", ".join(kept) if kept else "(none)"
        warning = f"\nWarning: not found and skipped: {', '.join(skipped)}" if skipped else ""
        await update.message.reply_text(
            f"Confirm: restrict {target.email} to: {confirm_str}?{warning} (yes/no)"
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
```

---

### Task 6: Update `menu()`, `set_my_commands`, and register handlers

**Files:**
- Modify: `app/telegram_bot.py`

- [ ] **Step 1: Update `menu()` to show new commands**

Find the existing lines in `menu()`:
```python
    if user.role == "superuser":
        admin_cmds.extend(["/add_dsn", "/add_user"])
    elif user.role == "admin":
        admin_cmds.append("/add_user")
```

Replace with:
```python
    if user.role == "superuser":
        admin_cmds.extend(["/add_dsn", "/add_user", "/list_users", "/edit_user"])
    elif user.role == "admin":
        admin_cmds.extend(["/add_user", "/list_users", "/edit_user"])
```

- [ ] **Step 2: Update `set_my_commands` in `post_init`**

Find the existing commands list in `post_init` inside `build_app`. Add two entries:
```python
            ("list_users", "List all users (admin/superuser)"),
            ("edit_user", "Edit a user (admin/superuser)"),
```

- [ ] **Step 3: Register new handlers in `main()`**

After the existing `app.add_handler(CommandHandler("reset", reset))` line, add:
```python
    app.add_handler(CommandHandler("list_users", list_users_cmd))
```

After the existing `add_user_conv` handler registration, add:
```python
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
```

- [ ] **Step 4: Verify the module imports without error**

```bash
CONTROL_DB_DSN="postgresql://x:x@localhost/x" TELEGRAM_BOT_TOKEN="x" .venv/bin/python -c "import app.telegram_bot; print('OK')"
```

Expected: `OK`

- [ ] **Step 5: Commit**

```bash
git add app/telegram_bot.py
git commit -m "feat: add /list_users, /edit_user, fix duplicate user error"
```

- [ ] **Step 6: Push**

```bash
git push
```
