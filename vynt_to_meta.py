"""
Vynt → Meta (Facebook) Catalog Sync
Fetches products from the Vynt API, transforms them into Facebook Catalog format,
and uploads them in batches of 50. Runs daily at 2 AM WAT via APScheduler.
"""

import os
import json
import logging
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
VYNT_API_URL = "https://realapp.vyntapp.com/engine/api/v2/misc/seek"
VYNT_BEARER_TOKEN = os.getenv("VYNT_AUTH_TOKEN", os.getenv("VYNT_BEARER_TOKEN", ""))
FACEBOOK_ACCESS_TOKEN = os.getenv("FACEBOOK_ACCESS_TOKEN", "")
CATALOG_ID = os.getenv("CATALOG_ID", "")
BASE_PRODUCT_URL = os.getenv("BASE_PRODUCT_URL", "https://vynt.com/products/")
PAGE_SIZE = 36
FB_BATCH_SIZE = 50
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
LAST_SYNC_FILE = LOGS_DIR / "last_sync.json"

WAT = timezone(timedelta(hours=1))  # West Africa Time = UTC+1

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGS_DIR / "sync.log"),
    ],
)
log = logging.getLogger("vynt_to_meta")

# ---------------------------------------------------------------------------
# Vynt API — fetch all products
# ---------------------------------------------------------------------------

def fetch_all_products() -> list[dict]:
    """Page through the Vynt seek API and return all raw product dicts."""
    token = VYNT_BEARER_TOKEN.removeprefix("bearer ").removeprefix("Bearer ")
    headers = {"Authorization": f"Bearer {token}"}
    all_products: list[dict] = []
    page = 0

    with httpx.Client(timeout=60) as client:
        while True:
            params = {"entity": "post", "page": page, "size": PAGE_SIZE}
            log.info("Fetching page %d …", page)
            resp = client.get(VYNT_API_URL, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()

            # The API returns a list of items (or a wrapper with a content key)
            items = data if isinstance(data, list) else data.get("content", data.get("data", []))
            if not items:
                break

            all_products.extend(items)
            log.info("  → got %d items (total so far: %d)", len(items), len(all_products))

            # Stop if we got fewer than a full page
            if len(items) < PAGE_SIZE:
                break
            page += 1

    log.info("Fetched %d products total.", len(all_products))
    return all_products


# ---------------------------------------------------------------------------
# Transform raw Vynt product → Facebook Catalog item
# ---------------------------------------------------------------------------

def to_facebook_item(raw: dict) -> dict:
    """Map a Vynt product dict to a Facebook catalog product dict."""
    product_id = str(raw.get("id", raw.get("_id", "")))
    title = raw.get("title", raw.get("name", ""))
    description = raw.get("description", title)
    price_val = raw.get("price", raw.get("amount", 0))
    currency = raw.get("currency", "NGN")

    # Images — accept string or list
    images = raw.get("images", raw.get("image", []))
    if isinstance(images, str):
        image_url = images
    elif isinstance(images, list) and images:
        image_url = images[0] if isinstance(images[0], str) else images[0].get("url", "")
    else:
        image_url = ""

    # Availability
    sold = raw.get("sold", False)
    status = raw.get("status", "")
    availability = "out of stock" if (sold or status == "sold") else "in stock"

    # Category
    category = raw.get("category", raw.get("categoryName", ""))

    return {
        "id": product_id,
        "title": title[:150],
        "description": (description or title)[:5000],
        "availability": availability,
        "condition": "new",
        "price": f"{price_val} {currency}",
        "link": f"{BASE_PRODUCT_URL}{product_id}",
        "image_link": image_url,
        "brand": "Vynt",
        "google_product_category": category,
    }


# ---------------------------------------------------------------------------
# Facebook Catalog API — batch upload
# ---------------------------------------------------------------------------

def upload_to_facebook(items: list[dict]) -> dict:
    """Upload items to the Facebook Catalog API in batches of FB_BATCH_SIZE."""
    url = f"https://graph.facebook.com/v21.0/{CATALOG_ID}/batch"
    headers = {"Authorization": f"Bearer {FACEBOOK_ACCESS_TOKEN}"}
    total_sent = 0
    errors: list[str] = []

    with httpx.Client(timeout=120) as client:
        for i in range(0, len(items), FB_BATCH_SIZE):
            batch = items[i : i + FB_BATCH_SIZE]
            requests_payload = [
                {"method": "UPDATE", "data": item} for item in batch
            ]

            log.info("Uploading batch %d–%d …", i, i + len(batch) - 1)
            resp = client.post(
                url,
                headers=headers,
                data={
                    "requests": json.dumps(requests_payload),
                    "item_type": "PRODUCT_ITEM",
                },
            )

            if resp.status_code == 200:
                total_sent += len(batch)
                body = resp.json()
                n_errors = body.get("num_errors", 0)
                if n_errors:
                    errors.append(f"Batch {i}: {n_errors} item-level errors")
                    log.warning("  → %d item-level errors in batch starting at %d", n_errors, i)
            else:
                msg = f"Batch {i}: HTTP {resp.status_code} — {resp.text[:300]}"
                errors.append(msg)
                log.error("  → %s", msg)

    return {"sent": total_sent, "errors": errors}


# ---------------------------------------------------------------------------
# Full sync pipeline
# ---------------------------------------------------------------------------

_sync_lock = threading.Lock()
_last_sync: dict = {}


def run_sync():
    """Execute a full fetch → transform → upload cycle."""
    global _last_sync
    if not _sync_lock.acquire(blocking=False):
        log.warning("Sync already in progress — skipping.")
        return

    started = datetime.now(WAT).isoformat()
    log.info("===== Sync started at %s =====", started)

    try:
        raw_products = fetch_all_products()
        fb_items = [to_facebook_item(p) for p in raw_products]
        log.info("Transformed %d items for Facebook.", len(fb_items))

        result = upload_to_facebook(fb_items)

        _last_sync = {
            "started": started,
            "finished": datetime.now(WAT).isoformat(),
            "products_fetched": len(raw_products),
            "products_sent": result["sent"],
            "errors": result["errors"],
            "status": "ok" if not result["errors"] else "partial",
        }
    except Exception as exc:
        log.exception("Sync failed: %s", exc)
        _last_sync = {
            "started": started,
            "finished": datetime.now(WAT).isoformat(),
            "products_fetched": 0,
            "products_sent": 0,
            "errors": [str(exc)],
            "status": "error",
        }
    finally:
        LAST_SYNC_FILE.write_text(json.dumps(_last_sync, indent=2))
        log.info("===== Sync finished — %s =====", _last_sync.get("status"))
        _sync_lock.release()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="Vynt → Meta Catalog Sync")

# Start scheduler on app startup
scheduler = BackgroundScheduler()


@app.on_event("startup")
def start_scheduler():
    # Load last sync from disk if available
    global _last_sync
    if LAST_SYNC_FILE.exists():
        try:
            _last_sync = json.loads(LAST_SYNC_FILE.read_text())
        except Exception:
            pass

    # Daily at 2:00 AM WAT (= 1:00 AM UTC)
    scheduler.add_job(run_sync, CronTrigger(hour=1, minute=0), id="daily_sync")
    scheduler.start()
    log.info("Scheduler started — daily sync at 02:00 WAT (01:00 UTC)")


@app.on_event("shutdown")
def stop_scheduler():
    scheduler.shutdown(wait=False)


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
