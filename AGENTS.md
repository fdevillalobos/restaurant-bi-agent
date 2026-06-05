# AGENTS.md

## Project Overview
Restaurant BI agent that turns natural-language questions into SQL, executes against a Postgres DB, and answers in English/Spanish via CLI, Telegram, or Vera's web chat workspace.

Key flows:
- LLM → SQL → DB → verbalizer
- Telegram bot handles auth, restaurant selection, and per-user DSN routing
- FastAPI web app handles signed-cookie login, restaurant selection, Vera chat API, React static serving, and Telegram webhook
- React web app uses assistant-ui external-store runtime plus shadcn-style Radix/Tailwind components for Vera's analyst workspace
- Web UI supports English and Spanish/LATAM; Spanish questions force Spanish Vera responses, otherwise Vera follows the selected web language
- Web Settings lets superusers/scoped admins manage users, invite links, DSNs, and restaurant access
- Control DB (Postgres) stores users, DSNs, restaurants, sessions, Vera memory, restaurant knowledge, invites, and admin audit events

## Running Locally

### CLI (ask questions)
```
.venv/bin/python -m app.cli "gross sales last 7 days"
```

### Telegram bot (polling)
```
.venv/bin/python -m app.telegram_bot
```

### Web app
```
.venv/bin/python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
cd web && npm run dev
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
- WEB_SESSION_SECRET
- PUBLIC_BASE_URL (optional)
- WEB_COOKIE_SECURE (optional)
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
- FastAPI web API + Telegram webhook: `app/main.py`, `app/web_api.py`
- React web shell/runtime: `web/src/main.tsx`, `web/src/components/Workspace.tsx`
- Web Settings UI: `web/src/components/Settings.tsx`, `web/src/components/InviteAccept.tsx`
- Vera BI render blocks: `web/src/components/vera/`
- Web UI primitives/styles: `web/src/components/ui/`, `web/src/styles.css`
- Web API/types/formatting: `web/src/api.ts`, `web/src/types.ts`, `web/src/format.ts`, `web/src/charting.ts`
- Web translations/language behavior: `web/src/i18n.ts`, `app/web_api.py`, `app/vera.py`
- Web architecture docs: `docs/web-interface-architecture.md`
- Control DB + auth: `app/tenant_store.py`, `app/auth.py`

## Debugging Tips
- Use `/debug on` in Telegram to print SQL
- Use `/whoami` to see selected restaurants and language
- Use `/restaurants` to select restaurants for your session
- In web, enable Debug in the sidebar to include SQL/query metadata in the debug drawer
- In web Settings, raw DSN values are write-only; use the DSN edit form to replace a value, never expect it in API responses

## Common Pitfalls
- Missing OPENROUTER_API_KEY or OPENAI_API_KEY → LLM errors, depending on `LLM_PROVIDER`
- Missing WEB_SESSION_SECRET in production → insecure default web cookie signing
- Missing `X-CSRF-Token` on authenticated POST/PATCH/DELETE routes → 403 from the web API
- `%` in SQL must be escaped (handled in planner)
- Items queries must include `items.canceled IS NOT TRUE`
