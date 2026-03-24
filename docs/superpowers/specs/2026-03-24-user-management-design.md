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

---

## 3. `/list_users` Command

**File:** `app/telegram_bot.py`

**Access:**
- `superuser` — lists all users across all DSNs
- `admin` — lists only users assigned to their own DSN
- Others — "Unauthorized."

**Output format** (one block per user, batched into messages of ≤ 10 users to stay within Telegram limits):
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

---

## 4. `/edit_user` Conversation Flow

**File:** `app/telegram_bot.py`

**States:**
```
EDIT_USER_EMAIL → EDIT_USER_FIELD → EDIT_USER_VALUE → EDIT_USER_CONFIRM
```

**Flow:**
```
/edit_user
→ "Email of user to edit:"
→ [look up user, show current info]
→ "What would you like to change?
   - password
   - role
   - restaurants
   - dsn"          ← only shown to superuser
→ [field-specific prompt and input]
→ "Confirm change? (yes/no)"
→ "Done." or "Cancelled."
```

**Per-field prompts:**

| Field | Prompt | Input |
|---|---|---|
| password | "New password:" | plain text, hashed before saving |
| role | "New role (user / db_admin / admin):" | validated against allowed values |
| restaurants | "Enter restaurant names (comma-separated), or 'all' to remove restrictions:" | parsed CSV or keyword `all` |
| dsn | "Select DSN by number:\n1. gamba\n2. ..." | number picker from list of all DSNs |

**Permission matrix:**

| Action | Superuser | Admin |
|---|---|---|
| Edit any user's password | ✓ | Own DSN users only (not other admins/superusers) |
| Edit role → user/db_admin | ✓ | Own DSN users only |
| Edit role → admin | ✓ | ✗ |
| Edit role → superuser | ✓ | ✗ |
| Edit restaurants | ✓ | Own DSN users only (not other admins/superusers) |
| Edit DSN | ✓ (including own) | ✗ |

**Guard rules:**
- If target user not found → "User not found."
- If admin tries to edit a user outside their DSN → "Unauthorized."
- If admin tries to edit another admin or superuser → "Unauthorized."
- If admin tries to change role to admin or above → "Unauthorized."
- `dsn` field option is only shown to superusers

---

## Files Changed

| File | Change |
|---|---|
| `app/tenant_store.py` | Add `list_users`, `update_user_password`, `update_user_role`, `update_user_dsn` |
| `app/telegram_bot.py` | Fix duplicate error in `add_user_confirm`; add `/list_users` handler; add `/edit_user` conversation handler |
