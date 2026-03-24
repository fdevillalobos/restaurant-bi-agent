# Restaurant BI Agent

Natural-language → SQL → Postgres analytics bot for Fudo POS, delivered via Telegram.

## Running locally

```bash
# One-off question
.venv/bin/python -m app.cli "gross sales last 7 days"

# Interactive REPL
.venv/bin/python -m app.cli --chat

# Telegram bot
.venv/bin/python -m app.telegram_bot
```

## Environment variables

See `.env.example` for the full list. Required:

| Variable | Description |
|---|---|
| `CONTROL_DB_DSN` | Postgres DSN for the control DB (users, sessions, DSNs) |
| `OPENAI_API_KEY` | OpenAI API key |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token from BotFather |
| `DATABASE_DSN` | Fudo POS Postgres DSN (CLI only — bot gets this via `/add_dsn`) |

## Railway deployment

### First-time setup

1. Create a Railway project with a **Postgres** service and a **GitHub** service pointing to this repo.
2. Set env vars on the bot service (see `.env.example`). Use `${{Postgres.DATABASE_URL}}` for `CONTROL_DB_DSN`.
3. Initialize the control DB from your local machine using the **public** Postgres URL (find `DATABASE_PUBLIC_URL` in the Railway Postgres service → Variables tab):

```bash
CONTROL_DB_DSN="<DATABASE_PUBLIC_URL>" .venv/bin/python -m app.admin_cli init-db
CONTROL_DB_DSN="<DATABASE_PUBLIC_URL>" .venv/bin/python -m app.admin_cli create-superuser --email you@example.com --password yourpassword
```

4. Open Telegram → `/login` with your superuser credentials.
5. Use `/add_dsn` to register your Fudo Postgres connection string.
6. Use `/restaurants` to verify restaurants synced correctly.

### Re-running init commands (e.g. after a DB reset)

Same as above — `init-db` is idempotent (`CREATE TABLE IF NOT EXISTS`), safe to re-run.

### Telegram bot commands

| Command | Description |
|---|---|
| `/login` | Authenticate |
| `/logout` | End session |
| `/restaurants` | List / select restaurants |
| `/language` | Switch language (en/es) |
| `/debug` | Toggle SQL debug output |
| `/whoami` | Show current session info |
| `/add_dsn` | (admin+) Register a Fudo Postgres DSN |
