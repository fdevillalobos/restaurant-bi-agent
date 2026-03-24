# Restaurant BI Agent

Natural-language analytics bot for Fudo POS restaurants. Ask questions in plain English or Spanish and get answers, tables, and charts — delivered via Telegram.

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

The bot will reply with a written answer, a data table (for multi-row results), and a chart when relevant.

The bot remembers the last 10 exchanges in a conversation — you can ask follow-up questions and it will understand the context. Use `/reset` to start fresh.

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

Copy `.env.example` to `.env` and fill in the values.

```bash
# One-off question (CLI)
.venv/bin/python -m app.cli "gross sales last 7 days"

# Interactive REPL
.venv/bin/python -m app.cli --chat

# Telegram bot
.venv/bin/python -m app.telegram_bot
```

---

## Railway deployment

### Environment variables

Set these on the Railway bot service (see `.env.example` for all options):

| Variable | Description |
|---|---|
| `CONTROL_DB_DSN` | Postgres DSN for the control DB — use `${{Postgres.DATABASE_URL}}` to reference the Railway Postgres service |
| `OPENAI_API_KEY` | OpenAI API key |
| `OPENAI_MODEL` | Model to use, e.g. `gpt-4o-mini` |
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

### After deploy

1. Open Telegram → `/login` with the superuser credentials
2. `/add_dsn` → register your Fudo Postgres connection string
3. `/restaurants` → confirm restaurants synced
4. Start asking questions

### Restarting the bot

Railway → bot service → **Deployments** tab → three dots on latest deployment → **Redeploy**.
