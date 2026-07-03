# Vynt → Meta + TikTok Catalog Sync

Syncs products from the Vynt API into a Facebook (Meta) Commerce catalog and a
TikTok catalog once a day at **02:00 WAT**, with a web dashboard for monitoring
and manual syncs.

- Fetches every product from the Vynt `seek` API (paginated)
- Transforms them into catalog items
- **Meta**: upserts via the Graph API `items_batch` endpoint (batches of 50,
  `UPDATE` method, retries on transient failures, aborts on auth errors), and
  surfaces Facebook's per-item validation and processing errors in the status
- **TikTok**: writes a feed file served at `/feed.csv` and triggers ingestion
  via the TikTok Business API (`catalog/product/file`)
- Each platform is optional and independent — one failing doesn't block the other

## Endpoints

| Endpoint        | Method | Purpose                              |
| --------------- | ------ | ------------------------------------ |
| `/health`       | GET    | Health probe (used by Render)        |
| `/status`       | GET    | Last sync summary (JSON)             |
| `/sync`         | POST   | Trigger an immediate background sync |
| `/dashboard`    | GET    | Monitoring dashboard (HTML)          |
| `/feed.csv`     | GET    | Latest product feed (TikTok/Meta-compatible CSV) |
| `/sync-history` | GET    | Last 10 sync results (JSON)          |
| `/token-health` | GET    | Vynt JWT expiry check                |

## Configuration (environment variables)

| Variable                | Required | Notes                                            |
| ----------------------- | -------- | ------------------------------------------------ |
| `VYNT_AUTH_TOKEN`       | yes      | Vynt bearer token (a `Bearer ` prefix is ok)     |
| `FACEBOOK_ACCESS_TOKEN` | for Meta | Meta system-user token with `catalog_management` |
| `CATALOG_ID`            | for Meta | Meta commerce catalog id                         |
| `TIKTOK_ACCESS_TOKEN`   | for TikTok | TikTok Business API token with catalog scope   |
| `TIKTOK_BC_ID`          | for TikTok | TikTok Business Center id                      |
| `TIKTOK_CATALOG_ID`     | for TikTok | TikTok catalog id                              |
| `PUBLIC_BASE_URL`       | no       | Public URL of this service, needed by TikTok to fetch `/feed.csv` (on Render, `RENDER_EXTERNAL_URL` is used automatically) |
| `BASE_PRODUCT_URL`      | no       | Product page base URL (default `https://vynt.com/products/`) |
| `GRAPH_API_VERSION`     | no       | Default `v25.0`                                  |
| `DATA_DIR`              | no       | Where logs/state are written (default `./logs`)  |

At least one platform (Meta or TikTok) must be fully configured. Locally, put
these in a `.env` file (gitignored).

### TikTok setup notes

- Create a catalog in [TikTok Business Center](https://business.tiktok.com)
  and generate a Business API access token with catalog permissions; put the
  three `TIKTOK_*` values in the environment.
- **No-API alternative:** skip the `TIKTOK_*` variables entirely and instead
  add a scheduled data feed in TikTok Catalog Manager pointing at
  `https://<your-app>.onrender.com/feed.csv` (TikTok accepts Facebook-format
  feeds). The feed regenerates on every sync.

## Run locally

```bash
pip install -r requirements.txt
uvicorn vynt_to_meta:app --reload
# open http://127.0.0.1:8000/dashboard
```

## Deploy to Render

The repo contains a [render.yaml](render.yaml) Blueprint.

1. Push to GitHub.
2. In the [Render dashboard](https://dashboard.render.com): **New → Blueprint**,
   pick this repo, and Render reads `render.yaml`.
3. When prompted, paste values for `VYNT_AUTH_TOKEN`, `FACEBOOK_ACCESS_TOKEN`
   and `CATALOG_ID` (marked `sync: false`, so they are never committed).
4. Deploy. The health check is `/health`; the dashboard lives at `/dashboard`.

Notes:

- The blueprint uses the **free** plan. Free instances spin down when idle and
  would sleep through the 02:00 WAT sync, so an external cron (e.g.
  [cron-job.org](https://cron-job.org)) must drive it: a `GET /health` at
  01:55 WAT to wake the instance, then a `POST /sync` at 02:05 WAT. (If both
  the internal scheduler and the cron fire, the sync lock and upsert semantics
  make the duplicate harmless.) Alternative: switch `plan:` to `starter`
  (always-on) and the in-process scheduler needs no external help.
- Keep it at **1 instance / 1 worker** or the sync would be scheduled multiple
  times in parallel.
- The filesystem is ephemeral: sync history resets on each deploy/restart
  unless you attach a persistent disk and set `DATA_DIR` (see comments in
  `render.yaml`).
- Subsequent pushes to `main` auto-deploy.
