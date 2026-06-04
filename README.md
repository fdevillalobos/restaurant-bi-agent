# Restaurant BI Agent

Natural-language analytics bot for Fudo POS restaurants. Ask questions in plain English or Spanish and get answers, tables, charts, and analyst-style recommendations — delivered through Telegram, CLI, or Vera's web chat workspace.

The analyst is Vera: a BI analyst persona that can plan multiple safe SQL queries, cross-reference results, explain what the numbers mean, recommend what to investigate next, and remember the last 30 exchanges per user/client database.

---

## Using the Telegram Bot

### Getting started

1. Find your bot on Telegram and send `/start`
2. Log in with `/login` — the bot will ask for your email and password
3. Select a restaurant with `/restaurants`
4. Start asking questions in plain text

### Asking questions

Just type your question naturally after logging in and selecting a restaurant. Examples:

```
What were gross sales last week?
Top 5 products by revenue this month
How many covers did we do yesterday?
Sales by day for the last 30 days
Compare takeaway vs eat-in sales this week
¿Cuáles fueron las ventas brutas la semana pasada?
```

Vera will reply with a written answer, a data table (for multi-row results), and a chart when relevant. For broader questions, she may ask a follow-up question before querying if the business goal or filters are unclear.

The bot remembers the last 30 exchanges for your user and selected client database — you can ask follow-up questions and it will understand the context. Use `/reset` to start fresh.

---

## Using the Web Interface

The web interface is a React/Vite app served by FastAPI. It gives Vera a richer analyst workspace with assistant-ui chat primitives, interactive ECharts charts, sortable tables, suggested follow-up questions, and debug query metadata.

assistant-ui is used as a frontend React dependency inside `web/`; it is not a Codex skill or a separate backend. Vera's backend still owns auth, restaurant scope, memory, SQL safety, query execution, and structured BI payloads.

### Run locally

```bash
# Backend/API
.venv/bin/python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000

# Frontend dev server
cd web
npm install
npm run dev
```

Open the Vite URL, log in with an existing control DB user, select restaurant(s), and ask Vera a question.

For a production-like local build:

```bash
cd web
npm run build
cd ..
.venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Then open `http://127.0.0.1:8000`.

### Web API

| Endpoint | Description |
|---|---|
| `POST /api/login` | Log in with existing user email/password |
| `POST /api/logout` | Clear the web session |
| `GET /api/me` | Return user, DSN, accessible restaurants, selected restaurants |
| `POST /api/restaurants/select` | Save selected restaurants in signed cookie session |
| `POST /api/chat` | Ask Vera and receive structured text/table/chart/debug payload |
| `GET /api/chat/history` | Load the persisted web transcript for the current user + DSN |
| `POST /api/memory/reset` | Clear conversation memory and web transcript |

Vera does not return raw HTML. The API returns structured JSON and the frontend renders safe known components.

State-changing web routes use the signed cookie plus a CSRF token returned by `/api/login` and `/api/me`. The frontend sends it as `X-CSRF-Token`.

### Web settings

The web app includes a Settings section for `superuser` and scoped `admin` users:

- `superuser` can manage all users, invites, DSNs, roles, and restaurant sync.
- `admin` can manage only `user` and `db_admin` accounts in their own DSN.
- `db_admin` and `user` do not see Settings.
- New users are created through 24-hour, single-use invite links.
- Users are deactivated instead of hard-deleted.
- Raw DSN values are write-only: they can be created or replaced, but are never returned by list endpoints.

Settings API routes:

| Endpoint | Description |
|---|---|
| `GET /api/admin/users` | List users visible to the current admin scope |
| `PATCH /api/admin/users/{id}` | Update role, DSN, active status, and restaurant restrictions |
| `GET /api/admin/invites` | List invite statuses |
| `POST /api/admin/invites` | Create a copyable invite link |
| `DELETE /api/admin/invites/{id}` | Revoke a pending invite |
| `GET /api/admin/dsns` | List DSNs without raw connection strings |
| `POST /api/admin/dsns` | Superuser-only DSN create, connection test, and restaurant sync |
| `PATCH /api/admin/dsns/{id}` | Superuser-only DSN name/update; DSN value is write-only |
| `POST /api/admin/dsns/{id}/sync-restaurants` | Superuser-only restaurant sync |
| `GET /api/admin/dsns/{id}/restaurants` | List restaurants for access assignment |
| `GET /api/invites/{token}` | Public invite preview |
| `POST /api/invites/{token}/accept` | Public password creation for the fixed invite email/access |

The production UI is built from:

- assistant-ui external-store runtime for the thread and composer
- shadcn-style Radix/Tailwind components for controls, sheets, tabs, badges, and workspace layout
- ECharts for interactive BI charts
- custom Vera blocks for markdown, KPI cards, charts, tables, recommendations, and debug/error states

Architecture details are in [`docs/web-interface-architecture.md`](docs/web-interface-architecture.md).

---

### Commands reference

#### Available to all users

| Command | Description |
|---|---|
| `/start` | Welcome message |
| `/login` | Log in with email and password |
| `/logout` | End your session |
| `/restaurants` | List available restaurants and select which to query |
| `/language en` or `/language es` | Set response language (English or Spanish) |
| `/debug on` or `/debug off` | Show or hide the generated SQL query in responses |
| `/reset` | Clear conversation history and start a fresh context |
| `/whoami` | Show your current session: user, role, DSN, selected restaurants, language, debug mode |
| `/menu` | List all commands available for your role |
| `/help` | Same as `/menu` |

#### Admin and superuser only

| Command | Who | Description |
|---|---|---|
| `/add_user` | admin, superuser | Create a new user. Walks through email, password, role, DSN assignment, and optional restaurant restrictions |
| `/add_dsn` | superuser only | Register a new Fudo Postgres connection string and sync its restaurants |

---

### Selecting restaurants

After `/restaurants` the bot shows a numbered list. Reply with:
- A number: `1`
- Multiple numbers: `1, 3`
- Restaurant names: `Sucursal Centro, Sucursal Norte`

Your selection is saved for the session. Change it any time with `/restaurants` again.

---

### User roles

| Role | Can do |
|---|---|
| `user` | Ask questions on assigned restaurants |
| `db_admin` | Same as user |
| `admin` | All of the above + create users within their DSN |
| `superuser` | Everything + add DSNs, create any user |

---

## Administration

### Adding a new restaurant client (superuser)

1. `/login` as superuser
2. `/add_dsn` — enter a name and the Postgres connection string for the client's Fudo database
3. The bot automatically syncs all restaurants from that database
4. `/add_user` — create a user, assign them to the new DSN, optionally restrict to specific restaurants

### Adding a user with restaurant restrictions

When creating a user with `/add_user`, after selecting role and DSN you'll be asked:
> "Limit restaurant access? (yes/no)"

Answer `yes` and provide a comma-separated list of restaurant names. That user will only see those restaurants.

---

## Running locally

### 1. Prerequisites

- Python 3.11
- [Postgres](https://www.postgresql.org/download/) installed and running locally (`brew install postgresql@16` on macOS)

### 2. Clone and install dependencies

```bash
git clone https://github.com/fdevillalobos/restaurant-bi-agent.git
cd restaurant-bi-agent
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 3. Create a local Postgres database for the control DB

```bash
createdb restaurant_bi_control
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in at minimum:

```bash
CONTROL_DB_DSN=postgresql://localhost/restaurant_bi_control
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini
TELEGRAM_BOT_TOKEN=123456:ABC-...
WEB_SESSION_SECRET=replace-with-a-long-random-string
```

By default, Vera uses OpenRouter:

```bash
LLM_PROVIDER=openrouter
OPENROUTER_API_KEY=sk-or-...
OPENROUTER_MODEL=deepseek/deepseek-v4-flash
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
```

To use OpenAI instead, set:

```bash
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini
```

### 5. Initialize the control DB and create a superuser

```bash
.venv/bin/python -m app.admin_cli init-db
.venv/bin/python -m app.admin_cli create-superuser --email you@example.com --password "yourpassword"
```

Test the configured LLM provider without touching restaurant data:

```bash
.venv/bin/python -m app.admin_cli test-openrouter
```

### 6. Run the bot

```bash
.venv/bin/python -m app.telegram_bot
```

Open Telegram → `/login` → `/add_dsn` to register your Fudo connection → `/restaurants` → start asking questions.

For Railway, Telegram should use FastAPI webhook mode rather than polling:

```bash
.venv/bin/python -m app.admin_cli set-telegram-webhook --url https://YOUR_DOMAIN/telegram/webhook
```

### Other useful commands

```bash
# One-off question (CLI)
.venv/bin/python -m app.cli "gross sales last 7 days"

# Interactive REPL
.venv/bin/python -m app.cli --chat
```

### Resetting a superuser password

```bash
.venv/bin/python -c "
from app.auth import hash_password
from app.tenant_store import _connect
with _connect() as conn:
    with conn.cursor() as cur:
        cur.execute('UPDATE users SET password_hash = %s WHERE email = %s', (hash_password('newpassword'), 'you@example.com'))
    conn.commit()
print('Password updated.')
"
```

---

## Railway deployment

### Environment variables

Set these on the Railway bot service (see `.env.example` for all options):

| Variable | Description |
|---|---|
| `CONTROL_DB_DSN` | Postgres DSN for the control DB — use `${{Postgres.DATABASE_URL}}` to reference the Railway Postgres service |
| `LLM_PROVIDER` | `openrouter` by default, or `openai` |
| `OPENROUTER_API_KEY` | OpenRouter API key |
| `OPENROUTER_MODEL` | Model to use, e.g. `deepseek/deepseek-v4-flash` |
| `OPENROUTER_BASE_URL` | Optional, defaults to `https://openrouter.ai/api/v1` |
| `OPENAI_API_KEY` | OpenAI API key if `LLM_PROVIDER=openai` |
| `OPENAI_MODEL` | OpenAI model if `LLM_PROVIDER=openai`, e.g. `gpt-4o-mini` |
| `TELEGRAM_BOT_TOKEN` | Bot token from BotFather |
| `WEB_SESSION_SECRET` | Required in production. Long random string used to sign web session cookies |
| `PUBLIC_BASE_URL` | Optional public URL used when documenting or setting webhook |
| `WEB_COOKIE_SECURE` | Set to `true` in production so cookies are HTTPS-only |

> `DATABASE_DSN` (Fudo POS) is **not** set here — it's registered per-client via `/add_dsn` inside Telegram.

The app refuses to boot in Railway/production if `WEB_SESSION_SECRET` is missing or left as the development default.

### First-time DB setup

Railway's internal Postgres hostname is only reachable from within Railway. Use the **public URL** for local commands — find `DATABASE_PUBLIC_URL` in the Railway Postgres service → Variables tab.

```bash
# Initialize tables
CONTROL_DB_DSN="<DATABASE_PUBLIC_URL>" .venv/bin/python -m app.admin_cli init-db

# Create the first superuser
CONTROL_DB_DSN="<DATABASE_PUBLIC_URL>" .venv/bin/python -m app.admin_cli create-superuser --email you@example.com --password "yourpassword"
```

`init-db` is idempotent — safe to re-run after a DB reset.

`init-db` also creates Vera's durable memory and restaurant-knowledge tables. Runtime markdown exports of restaurant knowledge are written under `runtime/vera_knowledge/` and are intentionally gitignored.

### After deploy

1. Open the Railway public web URL and log in with the superuser credentials
2. Open Telegram → `/login` with the same credentials
3. `/add_dsn` → register your Fudo Postgres connection string
4. `/restaurants` → confirm restaurants synced
5. Configure the Telegram webhook:

```bash
PUBLIC_BASE_URL="https://YOUR_DOMAIN" .venv/bin/python -m app.admin_cli set-telegram-webhook --url https://YOUR_DOMAIN/telegram/webhook
```

6. Ask Vera from the web workspace and Telegram

### Restarting the service

Railway → web service → **Deployments** tab → three dots on latest deployment → **Redeploy**.
