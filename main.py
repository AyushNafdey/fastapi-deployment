import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from config import *

import requests
from dateutil import parser
import pytz
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient, ASCENDING, DESCENDING
from apscheduler.schedulers.background import BackgroundScheduler

try:
    from .scheduler_config import get_schedule_minutes
except ImportError:  # pragma: no cover - fallback for direct script execution
    from scheduler_config import get_schedule_minutes

# Configuration
# MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
# MONGODB_DB = os.getenv("MONGODB_DB", "nifty")
# MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "option_chain")
NSE_SOURCE_URL = os.getenv("NSE_SOURCE_URL", "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY")
FETCH_USER_AGENT = os.getenv(
    "FETCH_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/115.0 Safari/537.36"
)

FETCH_TIMEOUT = 10  # seconds
SCHEDULE_MINUTES = get_schedule_minutes()

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("option-chain-backend")

# App
app = FastAPI(title="NIFTY Option Chain Backend")

# Allow frontend (Vite) at any origin in dev (or set exact origin in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # set to your site in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB setup
# client = MongoClient(MONGODB_URI)
# db = client[MONGODB_DB]
# collection = db[MONGODB_COLLECTION]

# Ensure index on timestamp for ordering and queries
collection.create_index([("timestamp", ASCENDING)])

# Pydantic models for responses
class SnapshotData(BaseModel):
    total_ce_oi: int
    total_pe_oi: int
    ce_oi_change: int
    pe_oi_change: int
    selected_expiry: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None

class Snapshot(BaseModel):
    timestamp: datetime
    data: SnapshotData

# Utility functions
IST = pytz.timezone("Asia/Kolkata")

def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return 0

def compute_totals_from_nse_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Attempt to compute totals (Total CE OI, Total PE OI, CE OI Change, PE OI Change)
    from NSE option-chain JSON structure. If structure is unexpected, return zeros
    but still store raw.
    """
    total_ce_oi = 0
    total_pe_oi = 0
    total_ce_change = 0
    total_pe_change = 0
    selected_expiry = None

    # Common NSE payload pattern has 'records' -> 'data' list with 'CE' and 'PE' keys
    try:
        records = payload.get("records") or {}
        if isinstance(records, dict):
            # Try to obtain expiry from records
            selected_expiry = records.get("expiryDates") and records.get("expiryDates")[0] if records.get("expiryDates") else records.get("expiryDate")
            data = records.get("data", [])
        else:
            data = []
        if not isinstance(data, list):
            data = []

        for row in data:
            ce = row.get("CE")
            pe = row.get("PE")
            if ce and isinstance(ce, dict):
                # try multiple possible keys for open interest and change
                oi = ce.get("openInterest") or ce.get("openInterest") or ce.get("OI") or ce.get("oi") or ce.get("openInterestValue")
                change = ce.get("changeinOpenInterest") or ce.get("changeinOpenInterest") or ce.get("changeinOI") or ce.get("changeinOI")
                total_ce_oi += _safe_int(oi)
                total_ce_change += _safe_int(change)
            if pe and isinstance(pe, dict):
                oi = pe.get("openInterest") or pe.get("openInterest") or pe.get("OI") or pe.get("oi") or pe.get("openInterestValue")
                change = pe.get("changeinOpenInterest") or pe.get("changeinOpenInterest") or pe.get("changeinOI") or pe.get("changeinOI")
                total_pe_oi += _safe_int(oi)
                total_pe_change += _safe_int(change)
        # As a fallback, some sources may provide aggregated fields already
        # Try to find them directly
        if total_ce_oi == 0:
            total_ce_oi = _safe_int(payload.get("totalCeOi") or payload.get("Total CE OI") or payload.get("total_ce_oi") or payload.get("total_ce_oi_raw") or 0)
        if total_pe_oi == 0:
            total_pe_oi = _safe_int(payload.get("totalPeOi") or payload.get("Total PE OI") or payload.get("total_pe_oi") or payload.get("total_pe_oi_raw") or 0)
        if total_ce_change == 0:
            total_ce_change = _safe_int(payload.get("ceChange") or payload.get("CE OI Change") or payload.get("ce_oi_change") or 0)
        if total_pe_change == 0:
            total_pe_change = _safe_int(payload.get("peChange") or payload.get("PE OI Change") or payload.get("pe_oi_change") or 0)

    except Exception as e:
        logger.exception("Error while computing totals from NSE payload: %s", e)
        # Leave totals at 0 if something goes wrong

    return {
        "Total CE OI": total_ce_oi,
        "Total PE OI": total_pe_oi,
        "CE OI Change": total_ce_change,
        "PE OI Change": total_pe_change,
        "selectedExpiry": selected_expiry,
    }

def fetch_option_chain_from_source() -> Dict[str, Any]:
    """
    Fetch option-chain JSON from configured source. Returns parsed JSON dict.
    """
    headers = {
        "User-Agent": FETCH_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com",
    }
    try:
        resp = requests.get(NSE_SOURCE_URL, headers=headers, timeout=FETCH_TIMEOUT)
        resp.raise_for_status()
        # Some endpoints return JSON; some return text with JSON inside
        data = resp.json()
        return data
    except Exception as exc:
        logger.warning("Primary fetch failed (%s). Returning empty payload. Exception: %s", NSE_SOURCE_URL, exc)
        # Return empty dict instead of raising so scheduler continues
        return {}

def store_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute totals and store snapshot into MongoDB collection.
    Returns the stored document as dict.
    """
    computed = compute_totals_from_nse_json(payload)
    now_utc = datetime.now(timezone.utc)
    doc = {
        "timestamp": now_utc,
        "data": {
            "Total CE OI": computed.get("Total CE OI", 0),
            "Total PE OI": computed.get("Total PE OI", 0),
            "CE OI Change": computed.get("CE OI Change", 0),
            "PE OI Change": computed.get("PE OI Change", 0),
            "selectedExpiry": computed.get("selectedExpiry"),
            "raw": payload,
        },
    }
    res = collection.insert_one(doc)
    logger.info("Stored snapshot %s at %s (id=%s)", computed, now_utc.isoformat(), str(res.inserted_id))
    # Convert to serializable dict for return
    doc["_id"] = res.inserted_id
    return doc

def fetch_and_store_job() -> None:
    """
    Scheduled job to fetch option-chain from NSE and store it.
    """
    try:
        payload = fetch_option_chain_from_source()
        store_snapshot(payload)
    except Exception as e:
        logger.exception("Error in scheduled fetch_and_store_job: %s", e)

# API endpoints
# @app.get("/history", response_model=List[Snapshot])
# def get_history() -> List[Dict[str, Any]]:
#     """
#     Return today's records (in Asia/Kolkata time) in ascending timestamp order.
#     """
#     try:
#         now_ist = datetime.now(IST)
#         start_of_day_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
#         start_utc = start_of_day_ist.astimezone(timezone.utc)
#         next_day_ist = start_of_day_ist + timedelta(days=1)
#         next_day_utc = next_day_ist.astimezone(timezone.utc)

#         cursor = collection.find(
#             {"timestamp": {"$gte": start_utc, "$lt": next_day_utc}}
#         ).sort("timestamp", ASCENDING)

#         results = []
#         for doc in cursor:
#             # Convert timestamp to ISO string
#             ts = doc.get("timestamp")
#             if isinstance(ts, datetime):
#                 ts_iso = ts.astimezone(timezone.utc).isoformat()
#             else:
#                 # fallback parsing
#                 ts_iso = parser.parse(str(ts)).astimezone(timezone.utc).isoformat()
#             data = doc.get("data", {})
#             results.append({
#                 "timestamp": ts_iso,
#                 "data": {
#                     "total_ce_oi": data.get("Total CE OI") or 0,
#                     "total_pe_oi": data.get("Total PE OI") or 0,
#                     "ce_oi_change": data.get("CE OI Change") or 0,
#                     "pe_oi_change": data.get("PE OI Change") or 0,
#                     "selected_expiry": data.get("selectedExpiry"),
#                     "raw": data.get("raw"),
#                 }
#             })
#         return results
#     except Exception as e:
#         logger.exception("Error in /history: %s", e)
#         raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/history", response_model=List[Snapshot])
def get_history() -> List[Dict[str, Any]]:
    """
    Return today's history.
    If today's collection has no records yet, return the most recent
    previous day's collection.
    """
    try:
        # Get today's collection name
        today = datetime.now(IST).date()

        # First try today's collection
        today_collection_name = f"oc_data_{today.strftime('%d-%m-%Y')}"
        today_collection = db[today_collection_name]

        cursor = today_collection.find({}).sort("timestamp", ASCENDING)
        docs = list(cursor)

        # If today's collection is empty, find the most recent
        # previous option-chain collection.
        if not docs:

            collection_names = db.list_collection_names()

            history_collections = []

            for name in collection_names:
                if name.startswith("oc_data_"):
                    try:
                        collection_date = datetime.strptime(
                            name.replace("oc_data_", ""),
                            "%d-%m-%Y"
                        ).date()

                        if collection_date < today:
                            history_collections.append(
                                (collection_date, name)
                            )

                    except ValueError:
                        continue

            if history_collections:
                # Most recent previous day
                _, previous_collection_name = max(
                    history_collections,
                    key=lambda x: x[0]
                )

                previous_collection = db[previous_collection_name]

                docs = list(
                    previous_collection.find({}).sort(
                        "timestamp",
                        ASCENDING
                    )
                )

        results = []

        for doc in docs:

            ts = doc.get("timestamp")

            if isinstance(ts, datetime):
                ts_iso = ts.astimezone(timezone.utc).isoformat()
            else:
                ts_iso = parser.parse(
                    str(ts)
                ).astimezone(timezone.utc).isoformat()

            data = doc.get("data", {})

            results.append({
                "timestamp": ts_iso,
                "data": {
                    "total_ce_oi": data.get("Total CE OI") or 0,
                    "total_pe_oi": data.get("Total PE OI") or 0,
                    "ce_oi_change": data.get("CE OI Change") or 0,
                    "pe_oi_change": data.get("PE OI Change") or 0,
                    "selected_expiry": data.get("selectedExpiry"),
                    "raw": data.get("raw"),
                }
            })

        return results

    except Exception as e:
        logger.exception("Error in /history: %s", e)
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )

@app.get("/latest-data", response_model=Optional[Snapshot])
def get_latest() -> Optional[Dict[str, Any]]:
    """
    Return the latest stored snapshot from MongoDB (or null if none).
    """
    try:
        doc = collection.find_one(sort=[("timestamp", DESCENDING)])
        if not doc:
            return None
        ts = doc.get("timestamp")
        if isinstance(ts, datetime):
            ts_iso = ts.astimezone(timezone.utc).isoformat()
        else:
            ts_iso = parser.parse(str(ts)).astimezone(timezone.utc).isoformat()
        data = doc.get("data", {})
        return {
            "timestamp": ts_iso,
            "data": {
                "total_ce_oi": data.get("Total CE OI") or 0,
                "total_pe_oi": data.get("Total PE OI") or 0,
                "ce_oi_change": data.get("CE OI Change") or 0,
                "pe_oi_change": data.get("PE OI Change") or 0,
                "selected_expiry": data.get("selectedExpiry"),
                "raw": data.get("raw"),
            }
        }
    except Exception as e:
        logger.exception("Error in /latest-data: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")

# Scheduler lifecycle
scheduler = BackgroundScheduler()

@app.on_event("startup")
def startup_event():
    logger.info("Starting scheduler with interval minutes=%s", SCHEDULE_MINUTES)
    # Add job: run every SCHEDULE_MINUTES
    scheduler.add_job(fetch_and_store_job, "interval", minutes=SCHEDULE_MINUTES, id="fetch_and_store_job", replace_existing=True)
    scheduler.start()
    # Run an immediate fetch once at startup (non-blocking)
    try:
        fetch_and_store_job()
    except Exception as e:
        logger.exception("Startup immediate fetch failed: %s", e)

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Shutting down scheduler")
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        logger.exception("Error shutting down scheduler")

# Example root
@app.get("/")
def root():
    return {"message": "NIFTY Option Chain Backend is running"}