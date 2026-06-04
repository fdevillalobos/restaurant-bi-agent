# Restaurant BI Agent

Natural-language analytics bot for Fudo POS restaurants. Ask questions in plain English or Spanish and get answers, tables, charts, and analyst-style recommendations — delivered via Telegram.

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

> `DATABASE_DSN` (Fudo POS) is **not** set here — it's registered per-client via `/add_dsn` inside Telegram.

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

1. Open Telegram → `/login` with the superuser credentials
2. `/add_dsn` → register your Fudo Postgres connection string
3. `/restaurants` → confirm restaurants synced
4. Start asking questions

### Restarting the bot

Railway → bot service → **Deployments** tab → three dots on latest deployment → **Redeploy**.
