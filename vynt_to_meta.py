"""
Vynt → Meta (Facebook) + TikTok Catalog Sync
Fetches products from the Vynt API, transforms them into catalog items, then:
  - upserts them into a Facebook catalog via the items_batch API (batches of 50)
  - writes a feed.csv (served at /feed.csv) and points TikTok's Catalog API at it
Runs daily at 02:00 Africa/Lagos (WAT) via APScheduler. Deployed on Render
(see render.yaml).
"""

import base64
import csv
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import quote

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, RedirectResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
VYNT_API_URL = os.getenv("VYNT_API_URL", "https://realapp.vyntapp.com/engine/api/v2/misc/seek")
VYNT_BEARER_TOKEN = os.getenv("VYNT_AUTH_TOKEN", os.getenv("VYNT_BEARER_TOKEN", ""))
FACEBOOK_ACCESS_TOKEN = os.getenv("FACEBOOK_ACCESS_TOKEN", "")
CATALOG_ID = os.getenv("CATALOG_ID", "")
GRAPH_API_VERSION = os.getenv("GRAPH_API_VERSION", "v25.0")
# Either a prefix the product id is appended to, or a template containing
# {id} (e.g. "https://apps.vyntapp.com/F8rp/3db4cmor?type=post&id={id}").
BASE_PRODUCT_URL = os.getenv("BASE_PRODUCT_URL", "https://vynt.com/products/").strip()
if "{id}" not in BASE_PRODUCT_URL and "?" not in BASE_PRODUCT_URL and not BASE_PRODUCT_URL.endswith("/"):
    BASE_PRODUCT_URL += "/"

# TikTok Catalog API (optional destination). Products are delivered by pointing
# TikTok's catalog/product/file endpoint at this service's public /feed.csv.
TIKTOK_API_URL = os.getenv("TIKTOK_API_URL", "https://business-api.tiktok.com/open_api/v1.3")
TIKTOK_ACCESS_TOKEN = os.getenv("TIKTOK_ACCESS_TOKEN", "")
TIKTOK_BC_ID = os.getenv("TIKTOK_BC_ID", "")
TIKTOK_CATALOG_ID = os.getenv("TIKTOK_CATALOG_ID", "")
# Render injects RENDER_EXTERNAL_URL automatically; PUBLIC_BASE_URL overrides it.
PUBLIC_BASE_URL = (os.getenv("PUBLIC_BASE_URL") or os.getenv("RENDER_EXTERNAL_URL") or "").strip().rstrip("/")

PAGE_SIZE = 36
MAX_PAGES = 500  # hard stop in case the API ignores pagination
FB_BATCH_SIZE = 50
FB_TITLE_MAX = 100  # items_batch limit (feed files allow 150, the batch API does not)
FB_DESCRIPTION_MAX = 5000
MAX_ERRORS_KEPT = 30
UPLOAD_MAX_ATTEMPTS = 3

# All mutable state lives under DATA_DIR so a single persistent disk can be
# mounted there on Render (DATA_DIR=/data). Defaults to ./logs locally.
DATA_DIR = Path(os.getenv("DATA_DIR", "logs"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
LAST_SYNC_FILE = DATA_DIR / "last_sync.json"
SYNC_HISTORY_FILE = DATA_DIR / "sync_history.json"
SYNC_HISTORY_LIMIT = 10
FEED_FILE = DATA_DIR / "feed.csv"
FEED_COLUMNS = [
    "id", "title", "description", "availability", "condition",
    "price", "link", "image_link", "brand", "google_product_category",
]

WAT = timezone(timedelta(hours=1))  # West Africa Time = UTC+1

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(DATA_DIR / "sync.log", maxBytes=1_000_000, backupCount=3, encoding="utf-8"),
    ],
)
log = logging.getLogger("vynt_to_meta")


def _product_link(product_id: str) -> str:
    quoted = quote(product_id, safe="")
    if "{id}" in BASE_PRODUCT_URL:
        return BASE_PRODUCT_URL.replace("{id}", quoted)
    return f"{BASE_PRODUCT_URL}{quoted}"


def _extract_id(raw: dict) -> str:
    """Product id as a string. None-aware: an id of 0 is valid, a null id is not."""
    for key in ("id", "_id"):
        value = raw.get(key)
        if value is not None and value != "":
            return str(value)
    return ""


def _clean_token(raw: str) -> str:
    """Strip whitespace and any 'Bearer ' prefix (any casing) from a token."""
    token = (raw or "").strip()
    if token.lower().startswith("bearer "):
        token = token[len("bearer "):].strip()
    return token


def _config_state(required: dict[str, str]) -> tuple[bool, list[str]]:
    """(fully_configured, missing_names) for a platform's required env vars."""
    missing = [name for name, value in required.items() if not (value or "").strip()]
    return (not missing, missing)


def _facebook_config() -> dict[str, str]:
    return {"FACEBOOK_ACCESS_TOKEN": FACEBOOK_ACCESS_TOKEN, "CATALOG_ID": CATALOG_ID}


def _tiktok_config() -> dict[str, str]:
    return {
        "TIKTOK_ACCESS_TOKEN": TIKTOK_ACCESS_TOKEN,
        "TIKTOK_BC_ID": TIKTOK_BC_ID,
        "TIKTOK_CATALOG_ID": TIKTOK_CATALOG_ID,
    }


def _require_config():
    if not _clean_token(VYNT_BEARER_TOKEN):
        raise RuntimeError("Missing required configuration: VYNT_AUTH_TOKEN")
    fb_ok, _ = _config_state(_facebook_config())
    tt_ok, _ = _config_state(_tiktok_config())
    if not fb_ok and not tt_ok:
        raise RuntimeError(
            "No destination configured — set FACEBOOK_ACCESS_TOKEN + CATALOG_ID "
            "and/or TIKTOK_ACCESS_TOKEN + TIKTOK_BC_ID + TIKTOK_CATALOG_ID."
        )


# ---------------------------------------------------------------------------
# Vynt API — fetch all products
# ---------------------------------------------------------------------------

def fetch_all_products() -> list[dict]:
    """Page through the Vynt seek API and return all raw product dicts."""
    headers = {"Authorization": f"Bearer {_clean_token(VYNT_BEARER_TOKEN)}"}
    all_products: list[dict] = []
    seen_ids: set[str] = set()

    with httpx.Client(timeout=60) as client:
        for page in range(MAX_PAGES):
            params = {"entity": "post", "page": page, "size": PAGE_SIZE}
            log.info("Fetching page %d ...", page)
            resp = client.get(VYNT_API_URL, headers=headers, params=params)
            if resp.status_code in (401, 403):
                raise RuntimeError(
                    f"Vynt API auth failed (HTTP {resp.status_code}) — refresh VYNT_AUTH_TOKEN."
                )
            resp.raise_for_status()
            data = resp.json()

            # The API returns a list of items (or a wrapper with a content key)
            items = data if isinstance(data, list) else (data.get("content") or data.get("data") or [])
            if not isinstance(items, list):
                raise RuntimeError(
                    f"Unexpected Vynt API response shape on page {page}: "
                    f"expected a list, got {type(items).__name__}"
                )
            if not items:
                break

            new_count = 0
            for item in items:
                item_id = _extract_id(item) if isinstance(item, dict) else ""
                if item_id:
                    if item_id in seen_ids:
                        continue
                    seen_ids.add(item_id)
                all_products.append(item)
                new_count += 1

            log.info("  -> got %d items, %d new (total so far: %d)", len(items), new_count, len(all_products))

            if new_count == 0:
                log.warning("Page %d returned only already-seen items — stopping pagination.", page)
                break
            # Stop if we got fewer than a full page
            if len(items) < PAGE_SIZE:
                break
        else:
            log.warning("Hit MAX_PAGES=%d — product list may be truncated.", MAX_PAGES)

    log.info("Fetched %d products total.", len(all_products))
    return all_products


# ---------------------------------------------------------------------------
# Transform raw Vynt product → Facebook Catalog item
# ---------------------------------------------------------------------------

def to_facebook_item(raw: dict) -> dict:
    """Map a Vynt product dict to a Facebook catalog product dict.

    Tolerates explicit nulls in the source data (a key present with value
    None must not crash the whole sync). Empty optional fields are omitted
    so Facebook's per-item validation reports them instead of choking on "".
    """
    product_id = _extract_id(raw)
    title = str(raw.get("title") or raw.get("name") or "").strip()
    description = str(raw.get("description") or "").strip() or title
    price_val = raw.get("price") or raw.get("amount") or 0
    currency = str(raw.get("currency") or "NGN").strip().upper()

    # Images — accept string, list of strings, or list of dicts
    images = raw.get("images") or raw.get("image") or []
    image_url = ""
    if isinstance(images, str):
        image_url = images
    elif isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, str):
            image_url = first
        elif isinstance(first, dict):
            image_url = str(first.get("url") or first.get("src") or "")

    # Availability
    sold = bool(raw.get("sold"))
    status = str(raw.get("status") or "").strip().lower()
    availability = "out of stock" if (sold or status == "sold") else "in stock"

    # Category
    category = str(raw.get("category") or raw.get("categoryName") or "").strip()

    item = {
        "id": product_id,
        "title": title[:FB_TITLE_MAX],
        "description": description[:FB_DESCRIPTION_MAX],
        "availability": availability,
        "condition": "new",
        "price": f"{price_val} {currency}",
        "link": _product_link(product_id),
        "image_link": image_url,
        "brand": "Vynt",
        "google_product_category": category,
    }
    return {key: value for key, value in item.items() if value != ""}


# ---------------------------------------------------------------------------
# Facebook Catalog API — batch upload
# ---------------------------------------------------------------------------

def _post_with_retry(
    client: httpx.Client,
    url: str,
    headers: dict,
    form: dict | None = None,
    json_body: dict | None = None,
) -> httpx.Response:
    """POST with retries on network errors, 429 and 5xx (exponential backoff)."""
    delay = 2
    for attempt in range(1, UPLOAD_MAX_ATTEMPTS + 1):
        try:
            resp = client.post(url, headers=headers, data=form, json=json_body)
        except httpx.RequestError as exc:
            if attempt == UPLOAD_MAX_ATTEMPTS:
                raise
            log.warning("Network error (%s) — retry %d/%d in %ds", exc, attempt, UPLOAD_MAX_ATTEMPTS, delay)
        else:
            if resp.status_code != 429 and resp.status_code < 500:
                return resp
            if attempt == UPLOAD_MAX_ATTEMPTS:
                return resp
            log.warning("HTTP %d — retry %d/%d in %ds", resp.status_code, attempt, UPLOAD_MAX_ATTEMPTS, delay)
        time.sleep(delay)
        delay *= 2
    raise RuntimeError("unreachable")  # pragma: no cover


def _check_batch_status(client: httpx.Client, headers: dict, handles: list[str]) -> list[str]:
    """Best-effort check of async batch processing via check_batch_request_status.

    Bounded to ~60s total; never raises. Returns a summary of item-level
    processing errors Facebook reported for finished batches.
    """
    if not handles:
        return []
    url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{CATALOG_ID}/check_batch_request_status"
    time.sleep(3)  # give Facebook a moment to start processing
    deadline = time.monotonic() + 60
    finished = still_processing = total_errors = 0
    samples: list[str] = []

    for handle in handles:
        if time.monotonic() > deadline:
            log.info("Batch status check hit its time budget — %d handles unchecked.", len(handles) - finished - still_processing)
            break
        try:
            resp = client.get(url, headers=headers, params={"handle": handle})
            resp.raise_for_status()
            entries = resp.json().get("data") or []
            if not entries:
                continue
            entry = entries[0]
            if entry.get("status") != "finished":
                still_processing += 1
                continue
            finished += 1
            batch_errors = entry.get("errors") or []
            count = entry.get("errors_total_count") or len(batch_errors)
            if count:
                total_errors += count
                for err in batch_errors[: max(0, 10 - len(samples))]:
                    message = err.get("message") if isinstance(err, dict) else str(err)
                    if message:
                        samples.append(str(message))
        except Exception as exc:
            log.warning("check_batch_request_status failed: %s", exc)

    if still_processing:
        log.info("%d/%d batches still processing at check time — late errors won't appear in this report.", still_processing, len(handles))
    if total_errors:
        detail = f" Samples: {' | '.join(samples)}" if samples else ""
        return [f"Facebook reported {total_errors} item-level processing errors across {finished} finished batches.{detail}"]
    return []


def upload_to_facebook(items: list[dict]) -> dict:
    """Upload items to the Facebook Catalog items_batch API in batches of FB_BATCH_SIZE."""
    url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{CATALOG_ID}/items_batch"
    headers = {"Authorization": f"Bearer {FACEBOOK_ACCESS_TOKEN.strip()}"}
    total_sent = 0
    errors: list[str] = []
    handles: list[str] = []

    with httpx.Client(timeout=120) as client:
        for i in range(0, len(items), FB_BATCH_SIZE):
            batch = items[i : i + FB_BATCH_SIZE]
            requests_payload = [{"method": "UPDATE", "data": item} for item in batch]

            log.info("Uploading batch %d–%d ...", i, i + len(batch) - 1)
            resp = _post_with_retry(
                client,
                url,
                headers,
                form={
                    "item_type": "PRODUCT_ITEM",
                    "requests": json.dumps(requests_payload),
                },
            )

            if resp.status_code == 200:
                total_sent += len(batch)
                body = resp.json()
                handles.extend(h for h in (body.get("handles") or []) if h)
                for validation in body.get("validation_status") or []:
                    for err in validation.get("errors") or []:
                        errors.append(
                            f"Item {validation.get('retailer_id')}: {err.get('message', err)}"
                        )
            else:
                msg = f"Batch {i}: HTTP {resp.status_code} — {resp.text[:300]}"
                errors.append(msg)
                log.error("  -> %s", msg)
                # Abort on auth errors — no point retrying with a bad token
                if resp.status_code in (401, 403):
                    log.error("Auth failure — aborting remaining batches.")
                    break

        errors.extend(_check_batch_status(client, headers, handles))

    if len(errors) > MAX_ERRORS_KEPT:
        overflow = len(errors) - MAX_ERRORS_KEPT
        errors = errors[:MAX_ERRORS_KEPT] + [f"… +{overflow} more errors suppressed"]
    return {"sent": total_sent, "errors": errors}


# ---------------------------------------------------------------------------
# TikTok Catalog API — feed file + ingestion trigger
# ---------------------------------------------------------------------------

def write_feed_csv(items: list[dict]) -> None:
    """Write the catalog feed served at /feed.csv (Facebook/TikTok-compatible columns)."""
    with FEED_FILE.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FEED_COLUMNS, extrasaction="ignore", restval="")
        writer.writeheader()
        writer.writerows(items)
    log.info("Wrote %d items to %s", len(items), FEED_FILE)


def upload_to_tiktok(item_count: int) -> dict:
    """Point TikTok's catalog at this service's public /feed.csv.

    Uses POST /catalog/product/file/ (per TikTok's official business-api SDK):
    body {bc_id, catalog_id, file_url}, auth via the Access-Token header,
    response envelope {code, message, ...} where code 0 means accepted.
    TikTok then fetches and ingests the feed asynchronously.
    """
    if not PUBLIC_BASE_URL:
        return {"sent": 0, "errors": [
            "PUBLIC_BASE_URL is not set (Render provides RENDER_EXTERNAL_URL automatically) — "
            "TikTok needs a public URL for /feed.csv"
        ]}
    feed_url = f"{PUBLIC_BASE_URL}/feed.csv"
    url = f"{TIKTOK_API_URL}/catalog/product/file/"
    headers = {"Access-Token": TIKTOK_ACCESS_TOKEN.strip()}
    body = {
        "bc_id": TIKTOK_BC_ID.strip(),
        "catalog_id": TIKTOK_CATALOG_ID.strip(),
        "file_url": feed_url,
    }

    log.info("Submitting feed to TikTok: %s", feed_url)
    with httpx.Client(timeout=60) as client:
        resp = _post_with_retry(client, url, headers, json_body=body)

    if resp.status_code != 200:
        return {"sent": 0, "errors": [f"HTTP {resp.status_code} - {resp.text[:300]}"]}
    payload = resp.json()
    if payload.get("code") != 0:
        return {"sent": 0, "errors": [f"TikTok error {payload.get('code')}: {payload.get('message')}"]}
    log.info("TikTok accepted the feed (%d items) for ingestion.", item_count)
    return {"sent": item_count, "errors": []}


# ---------------------------------------------------------------------------
# Full sync pipeline
# ---------------------------------------------------------------------------

_sync_lock = threading.Lock()
_last_sync: dict = {}


def _save_sync_history(entry: dict):
    """Append a sync result to the history file, keeping only the most recent runs."""
    history = []
    if SYNC_HISTORY_FILE.exists():
        try:
            loaded = json.loads(SYNC_HISTORY_FILE.read_text())
            if isinstance(loaded, list):
                history = loaded
        except Exception:
            pass
    history.append(entry)
    history = history[-SYNC_HISTORY_LIMIT:]
    SYNC_HISTORY_FILE.write_text(json.dumps(history, indent=2))


def _get_token_health() -> dict:
    """Decode JWT expiry from VYNT_AUTH_TOKEN and return days remaining."""
    token = _clean_token(VYNT_BEARER_TOKEN)
    if not token:
        return {"status": "missing", "message": "No token configured"}
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {"status": "unknown", "message": "Not a JWT token"}
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)  # restore base64 padding
        decoded = json.loads(base64.urlsafe_b64decode(payload))
        exp = decoded.get("exp")
        if not exp:
            return {"status": "unknown", "message": "No exp claim in token"}
        expiry_dt = datetime.fromtimestamp(exp, tz=timezone.utc)
        days_remaining = (expiry_dt - datetime.now(timezone.utc)).days
        if days_remaining < 0:
            return {
                "status": "expired",
                "expires": expiry_dt.isoformat(),
                "days_remaining": days_remaining,
                "message": f"Expired {-days_remaining}d ago",
            }
        return {
            "status": "warning" if days_remaining < 30 else "ok",
            "expires": expiry_dt.isoformat(),
            "days_remaining": days_remaining,
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


def run_sync():
    """Execute a full fetch → transform → upload cycle."""
    global _last_sync
    if not _sync_lock.acquire(blocking=False):
        log.warning("Sync already in progress — skipping.")
        return

    started = datetime.now(WAT).isoformat()
    log.info("===== Sync started at %s =====", started)

    try:
        _require_config()
        raw_products = fetch_all_products()
        if not raw_products:
            raise RuntimeError("Vynt API returned 0 products — aborting sync (catalogs left untouched).")

        transformed = [to_facebook_item(p) for p in raw_products if isinstance(p, dict)]
        valid_items = [item for item in transformed if item.get("id")]
        skipped = len(raw_products) - len(valid_items)
        if skipped:
            log.warning("Skipping %d items without a usable product id.", skipped)
        log.info("Transformed %d items.", len(valid_items))

        feed_error = None
        try:
            write_feed_csv(valid_items)
        except Exception as exc:  # feed failure must not block the Facebook upload
            feed_error = f"could not write feed.csv: {exc}"
            log.exception("Feed write failed")

        platforms: dict[str, dict] = {}
        for name, config, run in (
            ("facebook", _facebook_config(), lambda: upload_to_facebook(valid_items)),
            ("tiktok", _tiktok_config(), lambda: upload_to_tiktok(len(valid_items))),
        ):
            configured, missing = _config_state(config)
            if not configured:
                if len(missing) < len(config):  # some vars set, some missing — likely a mistake
                    platforms[name] = {"sent": 0, "errors": [f"incomplete config — missing {', '.join(missing)}"]}
                else:
                    platforms[name] = {"skipped": "not configured"}
                continue
            if name == "tiktok" and feed_error:
                platforms[name] = {"sent": 0, "errors": [feed_error]}
                continue
            try:
                platforms[name] = run()
            except Exception as exc:
                log.exception("%s upload failed", name)
                platforms[name] = {"sent": 0, "errors": [str(exc)]}

        combined_errors = [
            f"{name}: {err}"
            for name, result in platforms.items()
            for err in result.get("errors", [])
        ]
        sent_best = max((r.get("sent", 0) for r in platforms.values()), default=0)
        if not combined_errors:
            overall = "ok"
        elif sent_best > 0:
            overall = "partial"
        else:
            overall = "error"

        _last_sync = {
            "started": started,
            "finished": datetime.now(WAT).isoformat(),
            "products_fetched": len(raw_products),
            "products_sent": sent_best,
            "products_skipped": skipped,
            "facebook": platforms.get("facebook"),
            "tiktok": platforms.get("tiktok"),
            "errors": combined_errors,
            "status": overall,
        }
    except Exception as exc:
        log.exception("Sync failed: %s", exc)
        _last_sync = {
            "started": started,
            "finished": datetime.now(WAT).isoformat(),
            "products_fetched": 0,
            "products_sent": 0,
            "products_skipped": 0,
            "errors": [str(exc)],
            "status": "error",
        }
    finally:
        # Persistence must never prevent the lock release below — a failed
        # write here would otherwise deadlock every future sync until restart.
        try:
            LAST_SYNC_FILE.write_text(json.dumps(_last_sync, indent=2))
            _save_sync_history(_last_sync)
        except Exception:
            log.exception("Failed to persist sync result")
        log.info("===== Sync finished — %s =====", _last_sync.get("status"))
        _sync_lock.release()


# ---------------------------------------------------------------------------
# FastAPI app + scheduler
# ---------------------------------------------------------------------------
scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _last_sync
    if LAST_SYNC_FILE.exists():
        try:
            _last_sync = json.loads(LAST_SYNC_FILE.read_text())
        except Exception:
            log.warning("Could not load %s — starting with empty status.", LAST_SYNC_FILE)

    # Daily at 02:00 WAT. The timezone is explicit so the schedule does not
    # depend on the host's local timezone, and the misfire grace lets a job
    # that wakes late (busy instance, restart) still run instead of skipping.
    scheduler.add_job(
        run_sync,
        CronTrigger(hour=2, minute=0, timezone="Africa/Lagos"),
        id="daily_sync",
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
    )
    scheduler.start()
    log.info("Scheduler started — daily sync at 02:00 Africa/Lagos (WAT)")
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Vynt → Meta Catalog Sync", lifespan=lifespan)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/status")
def status():
    return _last_sync or {"status": "no sync has run yet"}


@app.post("/sync")
def trigger_sync(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_sync)
    return {"message": "Sync triggered in background"}


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    html_path = Path(__file__).parent / "dashboard.html"
    if not html_path.exists():
        return HTMLResponse("<h1>dashboard.html not found</h1>", status_code=500)
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/feed.csv")
def feed_csv():
    if not FEED_FILE.exists():
        return PlainTextResponse("Feed not generated yet - run a sync first.", status_code=404)
    return FileResponse(FEED_FILE, media_type="text/csv", filename="feed.csv")


@app.get("/sync-history")
def sync_history():
    if SYNC_HISTORY_FILE.exists():
        try:
            history = json.loads(SYNC_HISTORY_FILE.read_text())
            if isinstance(history, list):
                return history
        except Exception:
            pass
    return []


@app.get("/token-health")
def token_health():
    return _get_token_health()
