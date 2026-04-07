# Shubhmay — site + API

Monorepo layout: **static site** (HTML/CSS/JS at repo root) and **Node API** in `server/`. Production can run both together with `SERVE_STATIC=1`.

## Requirements

- **Node.js ≥ 22**
- **Supabase** project with schema `v2` (see `server/supabase/migrations/`)
- **Razorpay** keys + webhook pointing at your live `https://your-domain/api/webhooks/razorpay`

## Local run (site + API, same origin)

```bash
cd server
cp .env.example .env   # fill values
npm ci
npm run start:site
```

Open `http://127.0.0.1:3000/` (or your `PORT`). Admin UI: `http://127.0.0.1:3000/admindeoghar` (or `ADMIN_PANEL_PATH`).

## VPS deploy (checklist)

1. Copy repo to the server; `cd server && npm ci --omit=dev` (no dev deps needed).
2. Create `server/.env` from `.env.example` — all secrets on the server only.
3. Run with **`SERVE_STATIC=1`** if this process serves the whole site, or **`SERVE_STATIC=0`** if Nginx serves static files and only proxies `/api` to Node.
4. Put **HTTPS** in front (Nginx/Caddy). Razorpay webhooks require a public HTTPS URL.
5. Use **systemd** or **pm2** to keep `node index.js` alive; set `NODE_ENV=production`.
6. In Razorpay dashboard, set webhook URL to your production endpoint and paste `RAZORPAY_WEBHOOK_SECRET`.

## Health

- `GET /api/health` — basic API check  
- Admin routes under `/api/admin/*` require `ADMIN_SECRET`

## Security notes

- Do not commit `.env`.
- `SUPABASE_SERVICE_ROLE_KEY` is full access — server-side only.
- Change default admin path only if you also update bookmarks; `/admin` UI path is intentionally not served.
