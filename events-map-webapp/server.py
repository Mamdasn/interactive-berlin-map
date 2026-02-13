import os
import json
import time
import base64
import math
import logging
from functools import lru_cache
from datetime import datetime, date

import redis
import requests
import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request, abort, Response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)
app.logger.setLevel(logging.INFO)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    storage_uri=os.environ.get("RATELIMIT_STORAGE_URI", "memory://"),
)

# --- Parameters ---
NOMINATIM_URL = os.environ.get("NOMINATIM_URL", "http://nominatim:8080")
TILE_URL = os.environ.get("TILE_URL", "http://tileserver:8080")

DATABASE_URL = os.environ.get("DATABASE_URL") # postgres DB of events

GEOCACHE_TTL_DAYS = int(os.environ.get("GEOCACHE_TTL_DAYS", "180"))
PGCACHE_TTL_SECONDS = int(os.environ.get("PGCACHE_TTL_SECONDS", "600"))

NOMINATIM_TIMEOUT = float(os.environ.get("NOMINATIM_TIMEOUT", "5"))
EVENTS_MAX_GEOCODES = int(os.environ.get("EVENTS_MAX_GEOCODES", "25"))
EVENTS_MAX_ROWS = int(os.environ.get("EVENTS_MAX_ROWS", "5000"))

ROOT_RATE_LIMIT = os.environ.get("ROOT_RATE_LIMIT", "1000 per minute")

BERLIN_CENTER_LAT = float(os.environ.get("BERLIN_CENTER_LAT", "52.5200"))
BERLIN_CENTER_LON = float(os.environ.get("BERLIN_CENTER_LON", "13.4050"))
BERLIN_RADIUS_KM = float(os.environ.get("BERLIN_RADIUS_KM", "30.0"))

REDIS_DB_GEO = int(os.environ.get("REDIS_DB_GEO", "0")) # 0 = cache nominatim geocodes
REDIS_DB_PG = int(os.environ.get("REDIS_DB_PG", "1"))   # 1 = cache postgres DB of events

# --- Utilities ---
@lru_cache(maxsize=None)
def initiate_nominatim_http_session():
    session = requests.Session()
    session.headers.update({"User-Agent": "berlin-protest-map/1.0"})
    return session

def _redis_client(red_db_stack_num: int):
    host = os.environ.get("REDIS_HOST")
    if not host:
        return None

    port = int(os.environ.get("REDIS_PORT", "6379"))
    db = red_db_stack_num

    r = redis.Redis(
        host=host,
        port=port,
        db=db,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
        health_check_interval=30,
        retry_on_timeout=True,
    )

    try:
        r.ping()
    except redis.RedisError:
        return None

    return r

@lru_cache(maxsize=None)
def get_redis(red_db_stack_num: int):
    return _redis_client(red_db_stack_num)

def get_berlin_viewbox():
    _deg_lat = BERLIN_RADIUS_KM / 111.32
    _deg_lon = BERLIN_RADIUS_KM / (111.32 * math.cos(math.radians(BERLIN_CENTER_LAT)))

    BERLIN_MIN_LAT = BERLIN_CENTER_LAT - _deg_lat
    BERLIN_MAX_LAT = BERLIN_CENTER_LAT + _deg_lat
    BERLIN_MIN_LON = BERLIN_CENTER_LON - _deg_lon
    BERLIN_MAX_LON = BERLIN_CENTER_LON + _deg_lon

    NOMINATIM_VIEWBOX = f"{BERLIN_MIN_LON},{BERLIN_MAX_LAT},{BERLIN_MAX_LON},{BERLIN_MIN_LAT}"
    return NOMINATIM_VIEWBOX

def parse_date_param():
    allowed = {"date"}
    extra = set(request.args.keys()) - allowed
    if extra:
        abort(400, "Only 'date' query parameter is allowed")

    ds = request.args.get("date")
    if not ds:
        return date.today()

    if len(ds) != 10 or ds[4] != "-" or ds[7] != "-":
        abort(400, "date must be YYYY-MM-DD")

    try:
        return datetime.strptime(ds, "%Y-%m-%d").date()
    except ValueError:
        abort(400, "date must be YYYY-MM-DD")

# --- Geocoding functions ---
def calculate_geo_distance(lat1, lon1, lat2, lon2) -> float:
    """
    Great-circle distance between two points (km).
    """
    # I'll keep it like this for now to avoid edge cases
    R = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)

    a = (math.sin(dphi / 2) ** 2) + math.cos(phi1) * math.cos(phi2) * (math.sin(dlmb / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def in_berlin_radius(lat: float, lon: float) -> bool:
    return (
        calculate_geo_distance(BERLIN_CENTER_LAT, BERLIN_CENTER_LON, lat, lon)
        <= BERLIN_RADIUS_KM
    )

def pg_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)

def norm_key(plz: str, ort: str) -> str:
    plz = (plz or "").strip().upper()
    ort = " ".join((ort or "").strip().split())
    return f"{plz}|{ort}"

def geocode_place(versammlungsort: str, plz: str):
    q = f"{versammlungsort} {plz} Berlin"
    try:
        session = initiate_nominatim_http_session()
        r = session.get(
            f"{NOMINATIM_URL}/search",
            params={
                "q": q,
                "format": "json",
                "limit": 1,
                "countrycodes": "de",
                "bounded": 1,
                "viewbox": get_berlin_viewbox(),
            },
            timeout=NOMINATIM_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        if not data:
            return None, None

        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])

        if not in_berlin_radius(lat, lon):
            return None, None

        return lat, lon
    except requests.RequestException as e:
        app.logger.warning("Nominatim request failed for q=%r: %s", q, e)
        return None, None

def geocode_with_redis_cache(versammlungsort: str, plz: str):
    """
    Redis for caching geocodes:
      key = "{PLZ}|{ORT}"
      value = "lat,lon"
      TTL = GEOCACHE_TTL_DAYS
    """
    rgeo = get_redis(REDIS_DB_GEO)
    if rgeo is None:
        raise RuntimeError("Redis geo cache not configured properly.")

    k = norm_key(plz, versammlungsort)
    ttl = GEOCACHE_TTL_DAYS * 86400

    try:
        v = rgeo.get(k)
        if v:
            lat_s, lon_s = v.split(",", 1)
            lat = float(lat_s)
            lon = float(lon_s)
            if in_berlin_radius(lat, lon):
                return lat, lon, True
            rgeo.delete(k)
    except Exception as e:
        app.logger.warning("Redis geo cache read failed: %s", e)

    # fetch geocodes from nominatim directly
    lat, lon = geocode_place(versammlungsort, plz)
    if lat is None or lon is None:
        return None, None, False

    # cache geocoes in Redis
    try:
        rgeo.setex(k, ttl, f"{lat},{lon}")
    except Exception as e:
        app.logger.warning("Redis geo cache write failed: %s", e)

    return lat, lon, False

def build_locations(d: date):
    t0 = time.time()

    cache_key = f"locations::{d.isoformat()}"
    rpg = get_redis(REDIS_DB_PG)
    if rpg is None:
        raise RuntimeError("Redis PG cache not configured properly.")

    if rpg is not None:
        try:
            cached = rpg.get(cache_key)
            if cached:
                return json.loads(cached)
        except Exception as e:
            app.logger.warning("Redis pg cache read failed: %s", e)

    max_geocodes = EVENTS_MAX_GEOCODES
    max_rows = EVENTS_MAX_ROWS
    req_cache = {}

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
        rows = cur.fetchall()

    total = len(rows)

    grouped = {}  # "lat,lon" -> {"lat":..., "lon":..., "events":[...]}
    cache_hits = 0
    new_geocodes = 0
    skipped_new_geocodes = 0
    filtered_outside_radius = 0

    for row in rows:
        plz = (row.get("plz") or "").strip()
        ort = (row.get("versammlungsort") or "").strip()

        if not ort or not plz:
            continue

        k = norm_key(plz, ort)
        if k in req_cache:
            lat, lon = req_cache[k]
        else:
            if new_geocodes >= max_geocodes:
                lat, lon = None, None
                skipped_new_geocodes += 1
            else:
                lat, lon, hit = geocode_with_redis_cache(ort, plz)
                if hit:
                    cache_hits += 1
                else:
                    if lat is not None and lon is not None:
                        new_geocodes += 1
            req_cache[k] = (lat, lon)

        if lat is None or lon is None:
            continue

        if not in_berlin_radius(lat, lon):
            filtered_outside_radius += 1
            continue

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

    locations = list(grouped.values())
    for loc in locations:
        loc["events"].sort(key=lambda it: (it.get("von") or ""))

    locations.sort(key=lambda loc: (loc["lat"], loc["lon"]))

    app.logger.info(
        "build_locations(date=%s) rows=%d locations=%d events=%d cache_hits=%d new_geocodes=%d skipped=%d filtered_outside_radius=%d elapsed=%.2fs",
        d.isoformat(),
        total,
        len(locations),
        sum(len(l["events"]) for l in locations),
        cache_hits,
        new_geocodes,
        skipped_new_geocodes,
        filtered_outside_radius,
        time.time() - t0,
    )

    # cache locations in Redis
    if rpg is not None:
        try:
            rpg.setex(
                cache_key,
                PGCACHE_TTL_SECONDS,
                json.dumps(locations, ensure_ascii=False, separators=(",", ":")),
            )
        except Exception as e:
            app.logger.warning("Redis pg cache write failed: %s", e)

    return locations

# --- Obfuscation functions ---
def _xor_bytes(data: bytes, key: bytes) -> bytes:
    return bytes(b ^ key[i % len(key)] for i, b in enumerate(data))


def make_locations_obf(locations, app_date_iso: str) -> str:
    """
    make_locations_obf: "Obfuscate locations JSON for 
    embedding: XOR & base64."
    """
    key = f"berlin-events::v1::{app_date_iso}".encode("utf-8")

    raw = json.dumps(
        locations,
        ensure_ascii=False,
        separators=(",", ":"),  # minify
    ).encode("utf-8")

    obf = _xor_bytes(raw, key)
    return base64.b64encode(obf).decode("ascii")


# --- Flask routes ---
@app.route("/", methods=["GET", "HEAD"])
@limiter.limit(ROOT_RATE_LIMIT)
def root():
    app.logger.info(f"REMOTE_ADDR: {request.remote_addr}, \
          X-Forwarded-For: {request.headers.get('X-Forwarded-For')}")

    d = parse_date_param()
    locations = build_locations(d)
    obf = make_locations_obf(locations, d.isoformat())
    return render_template("index.html", date=d.isoformat(), locations_obf=obf)

@app.route("/<path:unused>")
def block_everything(unused):
    abort(404)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
