# User Management Improvements — Design Spec

## Goal

Improve the Telegram bot's user management UX with three changes:
1. Show a friendly error when creating a duplicate user instead of a silent crash
2. Add a `/list_users` command showing full user details
3. Add an `/edit_user` command for password, role, restaurant, and DSN changes

---

## 1. Duplicate User Error Fix

**Where:** `add_user_confirm` in `app/telegram_bot.py`

**Current behaviour:** When `create_user` is called with a duplicate email, psycopg raises `UniqueViolation`, which is unhandled and causes a silent failure in Telegram.

**Fix:** Wrap the `create_user` call in a try/except. Catch `psycopg.errors.UniqueViolation` and reply:
> "A user with that email already exists. Use /edit_user to modify an existing user."

---

## 2. New tenant_store.py Functions

Four new functions added to `app/tenant_store.py`:

### `list_users(dsn_id=None) → List[Dict]`
Returns users with enriched data. Each dict includes:
- `id`, `email`, `role`, `dsn_id`, `dsn_name` (joined from dsns table, or `"none"`)
- `restaurants`: list of restaurant name strings the user is restricted to; empty list means no restriction (access to all)

If `dsn_id` is provided, filters to only users assigned to that DSN. If `None`, returns all users.

### `update_user_password(user_id: int, password_hash: str) → None`
Updates `password_hash` for the given user.

### `update_user_role(user_id: int, role: str) → None`
Updates `role` for the given user.

### `update_user_dsn(user_id: int, dsn_id: Optional[int]) → None`
Updates `dsn_id` for the given user. Accepts `None` to unassign DSN.

**Note:** Restaurant edits reuse the existing `set_user_restaurants(user_id, restaurant_ids)` and `get_restaurant_ids_by_names(dsn_id, names)` functions. Name-to-ID resolution always uses the **target user's `dsn_id`**, not the editing user's.

---

## 3. `/list_users` Command

**File:** `app/telegram_bot.py`

**Access:**
- `superuser` — lists all users across all DSNs
- `admin` — lists only users assigned to their own DSN
- `db_admin`, `user` — "Unauthorized."

**Output format** — batched into messages of ≤ 10 users (conservative estimate to stay safely under Telegram's 4096-character message limit, given variable email and restaurant name lengths):
```
user@example.com — admin
DSN: gamba
Restaurants: Sucursal Centro, Sucursal Norte

user2@example.com — user
DSN: gamba
Restaurants: (all)

user3@example.com — superuser
DSN: (none)
Restaurants: (all)
```

User lookup in the edit flow uses `get_user_by_email` (already in `tenant_store.py`).

---

## 4. `/edit_user` Conversation Flow

**File:** `app/telegram_bot.py`

### Conversation state constants

Existing constants occupy 0–12. New constants start at 13:

```python
EDIT_USER_EMAIL, EDIT_USER_FIELD, EDIT_USER_VALUE, EDIT_USER_CONFIRM = range(13, 17)
```

### Flow

```
/edit_user
→ "Email of user to edit:"
→ [look up user via get_user_by_email; show current info: role, DSN, restaurants]
→ "What would you like to change? (password / role / restaurants / dsn)"
   ← 'dsn' option only shown to superusers
→ [field-specific prompt and input]
→ "Confirm change? (yes/no)"
→ "Done." or "Cancelled."
```

No `/cancel` fallback — consistent with all existing conversation handlers which use `fallbacks=[]`.

### Per-field prompts

| Field | Prompt | Input |
|---|---|---|
| password | "New password:" | plain text, hashed before saving |
| role | "New role:" followed by allowed options for the editor's role | validated against allowed values per permission matrix |
| restaurants | "Enter restaurant names (comma-separated), or 'all' to remove restrictions:\n[lists available restaurants from target user's DSN]" | parsed CSV or keyword `all` |
| dsn | "Select DSN by number:\n0. (none)\n1. gamba\n2. ..." | number picker; option 0 unassigns DSN (sets to NULL) |

**Role prompt options by editor role:**
- Superuser editing: shows `user / db_admin / admin / superuser`
- Admin editing: shows `user / db_admin` only

**Restaurant name mismatch handling:** If any entered name does not match an existing restaurant in the target user's DSN, skip it silently and include a warning in the confirmation:
> "Warning: the following names were not found and were skipped: X, Y"

### Permission matrix

| Action | Superuser | Admin | db_admin / user |
|---|---|---|---|
| Access `/edit_user` | ✓ | ✓ | ✗ Unauthorized |
| Edit any user's password | ✓ | Own DSN, non-admin/superuser only | ✗ |
| Edit role → user/db_admin | ✓ | Own DSN, non-admin/superuser only | ✗ |
| Edit role → admin | ✓ | ✗ | ✗ |
| Edit role → superuser | ✓ | ✗ | ✗ |
| Edit restaurants | ✓ | Own DSN, non-admin/superuser only | ✗ |
| Edit DSN | ✓ (including own) | ✗ | ✗ |

**Guard rules:**
- Target user not found → "User not found."
- Admin tries to edit user outside their DSN → "Unauthorized."
- Admin tries to edit another admin or superuser → "Unauthorized."
- Admin tries to set role to admin or above → "Unauthorized."
- `dsn` option not offered to admins in the field selection prompt

---

## 5. Additional locations to update in telegram_bot.py

- `menu()` function — add `/list_users` and `/edit_user` to the commands shown to `admin` and `superuser`
- `post_init` / `set_my_commands` — register both new commands with descriptions
- `build_app` / `main` — register `CommandHandler` for `/list_users` and `ConversationHandler` for `/edit_user`

---

## Files Changed

| File | What changes |
|---|---|
| `app/tenant_store.py` | Add `list_users`, `update_user_password`, `update_user_role`, `update_user_dsn` |
| `app/telegram_bot.py` | Fix duplicate error in `add_user_confirm`; add `/list_users` handler; add `/edit_user` conversation handler; update `menu()` and `set_my_commands` |
