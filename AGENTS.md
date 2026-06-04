# AGENTS.md

## Project Overview
Restaurant BI agent that turns natural-language questions into SQL, executes against a Postgres DB, and answers in English/Spanish via CLI or Telegram.

Key flows:
- LLM → SQL → DB → verbalizer
- Telegram bot handles auth, restaurant selection, and per-user DSN routing
- Control DB (Postgres) stores users, DSNs, restaurants, sessions, Vera memory, and restaurant knowledge

## Running Locally

### CLI (ask questions)
```
.venv/bin/python -m app.cli "gross sales last 7 days"
```

### Telegram bot (polling)
```
.venv/bin/python -m app.telegram_bot
```

### Control DB setup
```
.venv/bin/python -m app.admin_cli init-db
.venv/bin/python -m app.admin_cli create-superuser --email you@example.com --password "..."
```

## Environment Variables (.env)
- OPENAI_API_KEY
- OPENAI_MODEL (default: gpt-4o-mini)
- LLM_PROVIDER (default: openrouter)
- OPENROUTER_API_KEY
- OPENROUTER_MODEL (default: deepseek/deepseek-v4-flash)
- OPENROUTER_BASE_URL (optional, default: https://openrouter.ai/api/v1)
- TELEGRAM_BOT_TOKEN
- DATABASE_DSN (used by CLI and FastAPI)
- DEFAULT_RESTAURANT (optional)
- CONTROL_DB_DSN

## Core Rules (Business Semantics)
- Always filter: `sales.sale_state = 'CLOSED'`
- Time filters use `created_at` (never `closed_at`)
- Gross sales default: `SUM(sales.total)`
- Item revenue: `SUM(items.price * items.quantity)`
- Never use `products.price` for revenue (current price only)
- Always exclude `items.canceled IS NOT TRUE` when items are used
- “last week / semana pasada” means last **completed** week (Mon–Sun)
- “last Monday / lunes pasado” is the most recent Monday (corrected by rules)

## Where to Change Things
- Vera prompting + SQL rules: `app/vera.py`, `app/schema_context.py`
- LLM provider config: `app/llm_client.py`
- SQL safety: `app/sql_safety.py`
- Verbal output: `app/verbalizer.py`
- Telegram bot: `app/telegram_bot.py`
- Control DB + auth: `app/tenant_store.py`, `app/auth.py`

## Debugging Tips
- Use `/debug on` in Telegram to print SQL
- Use `/whoami` to see selected restaurants and language
- Use `/restaurants` to select restaurants for your session

## Common Pitfalls
- Missing OPENROUTER_API_KEY or OPENAI_API_KEY → LLM errors, depending on `LLM_PROVIDER`
- `%` in SQL must be escaped (handled in planner)
- Items queries must include `items.canceled IS NOT TRUE`
