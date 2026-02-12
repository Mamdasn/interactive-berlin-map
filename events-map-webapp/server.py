import os
import json
import time
import base64
import math
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

# Trust a single reverse proxy for client IP/scheme headers.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    storage_uri=os.environ.get("RATELIMIT_STORAGE_URI", "memory://"),
)

# --- Parameters ---
NOMINATIM_URL = os.environ.get("NOMINATIM_URL", "http://nominatim:8080")
TILE_URL = os.environ.get("TILE_URL", "http://tileserver:8080")

DATABASE_URL = os.environ.get("DATABASE_URL")  # postgresql://...

GEOCACHE_TTL_DAYS = int(os.environ.get("GEOCACHE_TTL_DAYS", "180"))
PGCACHE_TTL_SECONDS = int(os.environ.get("PGCACHE_TTL_SECONDS", "600"))

NOMINATIM_TIMEOUT = float(os.environ.get("NOMINATIM_TIMEOUT", "5"))
EVENTS_MAX_GEOCODES = int(os.environ.get("EVENTS_MAX_GEOCODES", "25"))
EVENTS_MAX_ROWS = int(os.environ.get("EVENTS_MAX_ROWS", "5000"))

ROOT_RATE_LIMIT = os.environ.get("ROOT_RATE_LIMIT", "10 per minute")

BERLIN_CENTER_LAT = float(os.environ.get("BERLIN_CENTER_LAT", "52.5200"))
BERLIN_CENTER_LON = float(os.environ.get("BERLIN_CENTER_LON", "13.4050"))
BERLIN_RADIUS_KM = float(os.environ.get("BERLIN_RADIUS_KM", "30.0"))

_deg_lat = BERLIN_RADIUS_KM / 111.32
_deg_lon = BERLIN_RADIUS_KM / (111.32 * math.cos(math.radians(BERLIN_CENTER_LAT)))

BERLIN_MIN_LAT = BERLIN_CENTER_LAT - _deg_lat
BERLIN_MAX_LAT = BERLIN_CENTER_LAT + _deg_lat
BERLIN_MIN_LON = BERLIN_CENTER_LON - _deg_lon
BERLIN_MAX_LON = BERLIN_CENTER_LON + _deg_lon

NOMINATIM_VIEWBOX = f"{BERLIN_MIN_LON},{BERLIN_MAX_LAT},{BERLIN_MAX_LON},{BERLIN_MIN_LAT}"

HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "berlin-protest-map/1.0"})


def _redis_client(db_env: str):
    host = os.environ.get("REDIS_HOST")
    port = os.environ.get("REDIS_PORT", "6379")
    if not host:
        return None
    db = int(os.environ.get(db_env, "0"))
    return redis.Redis(host=host, port=int(port), db=db, decode_responses=True)


_redis_geo = _redis_client("REDIS_DB_GEO")  # DB 0
_redis_pg = _redis_client("REDIS_DB_PG")  # DB 1


# --- unitilities ---
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
        r = HTTP.get(
            f"{NOMINATIM_URL}/search",
            params={
                "q": q,
                "format": "json",
                "limit": 1,
                "countrycodes": "de",
                "bounded": 1,
                "viewbox": NOMINATIM_VIEWBOX,
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
    Redis-only geocode cache:
      key = "{PLZ}|{ORT}"
      value = "lat,lon"
      TTL = GEOCACHE_TTL_DAYS
    """
    if _redis_geo is None:
        raise RuntimeError("Redis geo cache not configured (set REDIS_HOST/REDIS_DB_GEO)")

    k = norm_key(plz, versammlungsort)
    ttl = GEOCACHE_TTL_DAYS * 86400

    try:
        v = _redis_geo.get(k)
        if v:
            lat_s, lon_s = v.split(",", 1)
            lat = float(lat_s)
            lon = float(lon_s)
            if in_berlin_radius(lat, lon):
                return lat, lon, True
            _redis_geo.delete(k)
    except Exception as e:
        app.logger.warning("Redis geo cache read failed: %s", e)

    # fetch geocodes from nominatim directly
    lat, lon = geocode_place(versammlungsort, plz)
    if lat is None or lon is None:
        return None, None, False

    # cache geocoes in Redis
    try:
        _redis_geo.setex(k, ttl, f"{lat},{lon}")
    except Exception as e:
        app.logger.warning("Redis geo cache write failed: %s", e)

    return lat, lon, False

def build_locations(d: date):
    t0 = time.time()

    cache_key = f"locations::{d.isoformat()}"
    if _redis_pg is not None:
        try:
            cached = _redis_pg.get(cache_key)
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
            ORDER BY von ASC
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
        plz = row.get("plz")
        ort = row.get("versammlungsort")

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
    if _redis_pg is not None:
        try:
            _redis_pg.setex(
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
    Returns base64(xor(utf8(minified_json), key(app_date))).
    Not encryption; it's to avoid a clean JSON blob in HTML.
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
    d = parse_date_param()
    locations = build_locations(d)
    obf = make_locations_obf(locations, d.isoformat())
    return render_template("index.html", date=d.isoformat(), locations_obf=obf)

@app.route("/<path:unused>")
def block_everything(unused):
    abort(404)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
