# Vera Web Interface Architecture

## Overview

The web interface is the client-facing Vera workspace. It runs as a single Railway web service:

- FastAPI serves `/api/*` backend routes.
- FastAPI receives Telegram updates at `/telegram/webhook`.
- FastAPI serves the built React/Vite app for `/`.
- Vera remains the shared analyst core for Telegram, CLI, and web.

The Data Visualization plugin is a prototyping/reference tool only. Production UI rendering lives in this repository.

assistant-ui is installed as a React dependency in `web/`. It is not a Codex skill and it does not replace Vera's backend planner/executor.

## Request Flow

1. A browser logs in with `POST /api/login`.
2. FastAPI verifies the existing control DB user and sets a signed HTTP-only cookie.
3. The browser loads accessible restaurants with `GET /api/me`.
4. The browser stores the selected UI language locally and, after login, in the signed session with `POST /api/language`.
5. The user selects one or more restaurants with `POST /api/restaurants/select`.
6. The chat sends `POST /api/chat`.
7. The backend loads user + DSN memory, restaurant knowledge, selected restaurants, and language.
8. Vera plans up to 3 safe SELECT queries, the app executes them, and Vera synthesizes the final answer.
9. The web API returns structured JSON: text, tables, chart specs/data, recommendations, follow-up questions, and optional debug metadata.
10. React renders known components. Vera never returns raw HTML.

## Frontend

The web app lives in `web/` and uses:

- React + Vite + TypeScript
- assistant-ui external-store runtime for the thread/composer experience
- shadcn-style Radix/Tailwind primitives for workspace controls
- Apache ECharts via `echarts-for-react`
- Signed-cookie API calls with `credentials: "include"`

Primary screens:

- Login
- Restaurant selector
- Vera chat workspace with assistant-ui message actions and composer
- Settings for scoped user, invite, DSN, and restaurant-access administration
- Public invite acceptance page at `/invite/<token>`
- English/Spanish-LATAM language selector on login, invite acceptance, and signed-in sidebar
- Interactive charts
- Sortable tables
- Suggested follow-up questions
- Debug inspector for SQL/query metadata, failed SQL, params, selected restaurant context, and visible transcript state

assistant-ui uses the current React state as an external store:

- `GET /api/chat/history` hydrates the thread from the control DB.
- `POST /api/chat` sends new composer messages to Vera.
- Assistant messages keep Vera's structured payload for BI cards, charts, tables, recommendations, and debug output.
- The frontend converts the local Vera message shape into assistant-ui message objects for rendering and composer behavior.

## Language Behavior

The web UI currently supports `en` and `es`:

- Before login, the app initializes from `localStorage` or the browser language.
- Login sends the current language preference to the backend.
- Signed-in language changes call `POST /api/language` and update the signed cookie session.
- `/api/chat` sends the current UI language, but the backend still auto-detects Spanish questions and forces Vera to answer in Spanish.
- If a question is not detected as Spanish, Vera uses the selected UI language.
- Tables and charts use localized number formatting in the frontend; Spanish mode uses `es-AR` formatting.

## Backend

Main files:

- `app/main.py`: FastAPI app, legacy API routes, Telegram webhook, static web serving.
- `app/web_api.py`: Web login/session, restaurant selection, Vera chat endpoint.
- `app/vera.py`: Channel-neutral Vera planner/finalizer plus web payload serialization.
- `app/telegram_bot.py`: Telegram handlers, reusable for polling or webhook mode.

Session behavior:

- Web sessions use `vera_session`, a signed HTTP-only cookie.
- Cookie signing uses `WEB_SESSION_SECRET`.
- Selected restaurants are stored in the signed cookie.
- Sessions include a CSRF token returned by `/api/login` and `/api/me`.
- Cookie-authenticated state-changing routes require `X-CSRF-Token`, except public invite acceptance.
- Inactive users are blocked at login and on every authenticated request.
- Conversation memory remains in the control DB by user + DSN.

## Settings And Invites

Settings is a server-owned administration surface rendered by React:

- `superuser` can manage all users, all DSNs, all roles, invite links, and DSN restaurant sync.
- `admin` can manage only `user` and `db_admin` accounts in their own DSN.
- `db_admin` and `user` have no Settings access.
- Nobody can deactivate or demote the last active `superuser`.
- Users cannot deactivate themselves.

The control DB stores invite records in `user_invites`:

- Raw invite tokens are returned only once from `POST /api/admin/invites`.
- Only SHA-256 token hashes are stored.
- Invites are fixed to email, role, DSN, and restaurant access server-side.
- Invites expire after 24 hours and are single-use.
- Active existing users cannot accept an invite; inactive existing users can be reactivated with the invite's access.

DSN administration is superuser-only. List endpoints return DSN IDs, names, timestamps, and restaurant counts, never raw connection strings. DSN values are accepted only as write-only create/update inputs.

Admin mutations are written to `admin_audit_events` with actor, action, target, and non-secret details.

## Deployment

Railway should run one web service with:

```bash
uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
```

The Dockerfile builds the React app in a Node stage, copies `web/dist` into the Python image, then runs FastAPI.

Required env:

- `CONTROL_DB_DSN`
- `LLM_PROVIDER`
- `OPENROUTER_API_KEY`
- `OPENROUTER_MODEL`
- `TELEGRAM_BOT_TOKEN`
- `WEB_SESSION_SECRET`

Optional env:

- `PUBLIC_BASE_URL`
- `WEB_COOKIE_SECURE=true`
- `OPENROUTER_BASE_URL`
- `OPENAI_API_KEY`
- `OPENAI_MODEL`

After deployment, configure Telegram webhook:

```bash
python -m app.admin_cli set-telegram-webhook --url https://YOUR_DOMAIN/telegram/webhook
```

For local Telegram polling, `python -m app.telegram_bot` still works, but Railway should use webhook mode through FastAPI.

In Railway/production, startup fails if `WEB_SESSION_SECRET` is missing or set to the development default.
