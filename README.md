# BTC Options Algo — Vercel Deployment

## Project Structure
```
btc-algo/
├── api/
│   └── index.py          ← Flask backend (Vercel serverless entry)
├── static/
│   └── index.html        ← Frontend dashboard
├── requirements.txt      ← Python dependencies
├── vercel.json           ← Vercel routing config
└── README.md
```

## Key Changes from FastAPI → Flask

| Feature | Before (FastAPI) | After (Flask/Vercel) |
|---|---|---|
| Framework | FastAPI + uvicorn | Flask (Vercel-native) |
| Real-time | WebSocket (`/ws`) | **Server-Sent Events** (`/api/events`) |
| DB path | `../data/trades.db` | `/tmp/trades.db` (Vercel writable) |
| Threading | asyncio tasks | Python threads (daemon) |

> ⚠️ **Note on persistence**: Vercel serverless functions use `/tmp` which is ephemeral — trade history resets on cold starts. For production, swap SQLite for a hosted DB (PlanetScale, Supabase, etc.).

## Deploy to Vercel

### Option 1 — Vercel CLI (recommended)
```bash
npm i -g vercel
cd btc-algo
vercel deploy
```

### Option 2 — GitHub + Vercel Dashboard
1. Push this folder to a GitHub repo
2. Go to vercel.com → New Project → Import repo
3. Set **Framework Preset** to `Other`
4. Add environment variables (optional, see below)
5. Click Deploy

## Environment Variables (optional but recommended)
Set these in Vercel Dashboard → Settings → Environment Variables:

```
DELTA_API_KEY     = your_api_key
DELTA_API_SECRET  = your_api_secret
```

If not set, the hardcoded values in `api/index.py` are used.

## Local Development
```bash
pip install -r requirements.txt
cd api
python index.py
# Open http://localhost:5000
```

## SSE vs WebSocket
Vercel doesn't support WebSockets in serverless functions.
This version uses **Server-Sent Events (SSE)** — a one-way push stream
from server to browser that works perfectly on Vercel.
The frontend reconnects automatically if the stream drops.
