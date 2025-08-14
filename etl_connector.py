#!/usr/bin/env python3
"""
Custom Python ETL Data Connector: Open‑Meteo → MongoDB
Author: <Your Name> (<Roll No>)
Repo: Kyureeus EdTech — SSN CSE Software Architecture Assignment

What this does
--------------
Extracts hourly weather data from Open‑Meteo (no API key needed),
transforms it into a flat schema, and loads it into a MongoDB collection.
All creds (Mongo URI etc.) come from .env.

Collection rule used: one collection per connector (open_meteo_raw).

How to run (example):
    python etl_connector.py --lat 13.0827 --lon 80.2707 --start 2025-08-01 --end 2025-08-14
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Any


from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.errors import ConnectionFailure


def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def make_session() -> requests.Session:
    """HTTP session with retry/backoff for 429/5xx."""
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "SSN-ETL-Connector/1.0"})
    return s


def get_mongo_collection() -> Any:
    """Connect to MongoDB and return the target collection."""
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    mongo_db = os.getenv("MONGO_DB", "etl_db")
    collection_name = os.getenv("COLLECTION_NAME", "open_meteo_raw")

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except ConnectionFailure as e:
        logging.error("Cannot connect to MongoDB. Check MONGO_URI. Error: %s", e)
        sys.exit(2)

    db = client[mongo_db]
    col = db[collection_name]

    # Ensure unique index so repeated runs upsert instead of duplicating.
    col.create_index(
        [
            ("source", ASCENDING),
            ("lat", ASCENDING),
            ("lon", ASCENDING),
            ("timestamp", ASCENDING),
        ],
        unique=True,
        name="uniq_source_lat_lon_ts",
    )
    return col


def extract_open_meteo(session: requests.Session, lat: float, lon: float, start: date, end: date, hourly_vars: List[str]) -> Dict[str, Any]:
    """Call Open‑Meteo API and return JSON."""
    base = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(hourly_vars),
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
        "timezone": "auto",
    }
    logging.info("Extracting from Open‑Meteo: %s", params)
    resp = session.get(base, params=params, timeout=30)
    if resp.status_code != 200:
        logging.error("API error: status=%s body=%s", resp.status_code, resp.text[:500])
        sys.exit(3)
    data = resp.json()
    if "hourly" not in data or "time" not in data["hourly"]:
        logging.error("Unexpected payload: missing 'hourly.time'")
        sys.exit(4)
    return data


def transform_open_meteo(payload: Dict[str, Any], lat: float, lon: float, hourly_vars: List[str]) -> List[Dict[str, Any]]:
    """Flatten hourly arrays into a list of documents."""
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    n = len(times)

    # Sanity-check: every requested var should be present and same length
    for var in hourly_vars:
        if var not in hourly:
            logging.warning("Requested var '%s' missing in response; filling with None.", var)
        else:
            if len(hourly[var]) != n:
                logging.error("Length mismatch for '%s': %d vs %d", var, len(hourly[var]), n)
                sys.exit(5)

    docs = []
    for i, ts in enumerate(times):
        doc = {
            "source": "open-meteo",
            "lat": float(lat),
            "lon": float(lon),
            "timestamp": ts,  # ISO string
        }
        for var in hourly_vars:
            values = hourly.get(var)
            doc[var] = values[i] if values and i < len(values) else None
        docs.append(doc)
    logging.info("Transformed %d hourly records.", len(docs))
    return docs


def load_to_mongo(col, docs: List[Dict[str, Any]]) -> Dict[str, int]:
    """Upsert docs into MongoDB with an ingestion run id."""
    run_id = datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    ops = []
    for d in docs:
        d_set = dict(d)
        d_set["ingested_at"] = datetime.now(timezone.utc)
        d_set["ingestion_run_id"] = run_id
        # unique key enforced via index
        key = {
            "source": d["source"],
            "lat": d["lat"],
            "lon": d["lon"],
            "timestamp": d["timestamp"],
        }
        ops.append(UpdateOne(key, {"$set": d_set}, upsert=True))

    if not ops:
        return {"upserted": 0, "modified": 0}

    result = col.bulk_write(ops, ordered=False)
    # Count logic: result.upserted_count is available; modified count may be in modified_count
    summary = {
        "upserted": getattr(result, "upserted_count", 0),
        "modified": getattr(result, "modified_count", 0),
    }
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Custom Python ETL: Open‑Meteo → MongoDB")
    parser.add_argument("--lat", type=float, default=13.0827, help="Latitude (default: Chennai 13.0827)")
    parser.add_argument("--lon", type=float, default=80.2707, help="Longitude (default: Chennai 80.2707)")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD). Default: today-7d")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD). Default: today")
    parser.add_argument("--hourly", type=str, default="temperature_2m,relative_humidity_2m",
                        help="Comma-separated hourly vars. Default: temperature_2m,relative_humidity_2m")
    return parser.parse_args()


def main():
    load_dotenv()
    setup_logging()

    args = parse_args()
    hourly_vars = [v.strip() for v in args.hourly.split(",") if v.strip()]

    today = date.today()
    start = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else today - timedelta(days=7)
    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else today

    if start > end:
        logging.error("Start date cannot be after end date.")
        sys.exit(1)

    session = make_session()
    col = get_mongo_collection()

    payload = extract_open_meteo(session, args.lat, args.lon, start, end, hourly_vars)
    docs = transform_open_meteo(payload, args.lat, args.lon, hourly_vars)
    summary = load_to_mongo(col, docs)

    logging.info("Ingestion complete. Upserted: %s | Modified: %s | Total processed: %s",
                 summary.get("upserted", 0), summary.get("modified", 0), len(docs))
    print(json.dumps({
        "status": "ok",
        "processed": len(docs),
        "upserted": summary.get("upserted", 0),
        "modified": summary.get("modified", 0),
        "collection": col.name
    }, indent=2, default=str))


if __name__ == "__main__":
    main()
