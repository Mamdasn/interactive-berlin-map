import json
import logging
import os
import time
from datetime import date

import psycopg2.extras

from libs.db_clients import get_redis, pg_conn
from libs.geo_utils import in_berlin_radius, norm_key
from libs.geocoding import geocode_with_redis_cache

PGCACHE_TTL_SECONDS = int(os.environ.get("PGCACHE_TTL_SECONDS", "600"))
EVENTS_MAX_GEOCODES = int(os.environ.get("EVENTS_MAX_GEOCODES", "25"))
EVENTS_MAX_ROWS = int(os.environ.get("EVENTS_MAX_ROWS", "5000"))
REDIS_DB_PG = int(os.environ.get("REDIS_DB_PG", "1"))


def _get_cached_locations(rpg, cache_key: str):
    try:
        cached = rpg.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception as e:
        logging.warning("Redis pg cache read failed: %s", e)
    return None


def _fetch_rows(d: date, max_rows: int):
    with pg_conn() as conn, conn.cursor(
        cursor_factory=psycopg2.extras.RealDictCursor
    ) as cur:
        cur.execute(
            """
            SELECT id, datum, von, bis, thema, plz, versammlungsort, aufzugsstrecke
            FROM events
            WHERE datum = %s
            ORDER BY von ASC, id ASC
            LIMIT %s
            """,
            (d, max_rows),
        )
        return cur.fetchall()


def _resolve_coordinates(row, req_cache, counters, max_geocodes: int):
    plz = (row.get("plz") or "").strip()
    ort = (row.get("versammlungsort") or "").strip()

    if not ort or not plz:
        return None, None, plz, ort

    k = norm_key(plz, ort)
    if k in req_cache:
        lat, lon = req_cache[k]
        return lat, lon, plz, ort

    if counters["new_geocodes"] >= max_geocodes:
        lat, lon = None, None
        counters["skipped_new_geocodes"] += 1
    else:
        lat, lon, hit = geocode_with_redis_cache(ort, plz)
        if hit:
            counters["cache_hits"] += 1
        else:
            if lat is not None and lon is not None:
                counters["new_geocodes"] += 1

    req_cache[k] = (lat, lon)
    return lat, lon, plz, ort


def _add_event(grouped, row, plz: str, ort: str, lat: float, lon: float):
    key = f"{lat:.6f},{lon:.6f}"
    if key not in grouped:
        grouped[key] = {
            "lat": float(f"{lat:.6f}"),
            "lon": float(f"{lon:.6f}"),
            "events": [],
        }

    grouped[key]["events"].append(
        {
            "id": int(row["id"]),
            "title": row["thema"] or "Protest",
            "thema": row["thema"],
            "plz": plz,
            "versammlungsort": ort,
            "von": row["von"].strftime("%H:%M"),
            "bis": row["bis"].strftime("%H:%M"),
            "aufzugsstrecke": row.get("aufzugsstrecke"),
        }
    )


def _finalize_locations(grouped):
    locations = list(grouped.values())
    for loc in locations:
        loc["events"].sort(key=lambda it: (it.get("von") or ""))
    locations.sort(key=lambda loc: (loc["lat"], loc["lon"]))
    return locations


def _cache_locations(rpg, cache_key: str, locations):
    try:
        rpg.setex(
            cache_key,
            PGCACHE_TTL_SECONDS,
            json.dumps(locations, ensure_ascii=False, separators=(",", ":")),
        )
    except Exception as e:
        logging.warning("Redis pg cache write failed: %s", e)


def build_locations(d: date):
    t0 = time.time()
    cache_key = f"locations::{d.isoformat()}"
    rpg = get_redis(REDIS_DB_PG)
    if rpg is None:
        raise RuntimeError("Redis PG cache not configured properly.")

    cached_locations = _get_cached_locations(rpg, cache_key)
    if cached_locations is not None:
        return cached_locations

    max_geocodes = EVENTS_MAX_GEOCODES
    max_rows = EVENTS_MAX_ROWS
    req_cache = {}
    rows = _fetch_rows(d, max_rows)
    total = len(rows)
    grouped = {}
    counters = {
        "cache_hits": 0,
        "new_geocodes": 0,
        "skipped_new_geocodes": 0,
        "filtered_outside_radius": 0,
    }

    for row in rows:
        lat, lon, plz, ort = _resolve_coordinates(row, req_cache, counters, max_geocodes)

        if lat is None or lon is None:
            continue

        if not in_berlin_radius(lat, lon):
            counters["filtered_outside_radius"] += 1
            continue

        _add_event(grouped, row, plz, ort, lat, lon)

    locations = _finalize_locations(grouped)

    logging.info(
        "build_locations(date=%s) rows=%d locations=%d events=%d cache_hits=%d new_geocodes=%d skipped=%d filtered_outside_radius=%d elapsed=%.2fs",
        d.isoformat(),
        total,
        len(locations),
        sum(len(l["events"]) for l in locations),
        counters["cache_hits"],
        counters["new_geocodes"],
        counters["skipped_new_geocodes"],
        counters["filtered_outside_radius"],
        time.time() - t0,
    )

    _cache_locations(rpg, cache_key, locations)
    return locations
