import os
import json
import time
import base64
import sqlite3
import threading
import math
from datetime import datetime, date

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
    default_limits=[os.environ.get("RATELIMIT_DEFAULT_LIMIT", "120 per minute")],
    meta_limits=[
        os.environ.get("RATELIMIT_META_LIMIT_1", "600 per 10 minute"),
        os.environ.get("RATELIMIT_META_LIMIT_2", "5000 per day"),
    ],
)

# --- Parameters ---
NOMINATIM_URL = os.environ.get("NOMINATIM_URL", "http://nominatim:8080")
TILE_URL = os.environ.get("TILE_URL", "http://tileserver:8080")

DATABASE_URL = os.environ.get("DATABASE_URL")  # postgresql://...

GEOCACHE_PATH = os.environ.get("GEOCACHE_PATH", "/cache/geocache.sqlite")
GEOCACHE_TTL_DAYS = int(os.environ.get("GEOCACHE_TTL_DAYS", "180"))

NOMINATIM_TIMEOUT = float(os.environ.get("NOMINATIM_TIMEOUT", "5"))
EVENTS_MAX_GEOCODES = int(os.environ.get("EVENTS_MAX_GEOCODES", "25"))
EVENTS_MAX_ROWS = int(os.environ.get("EVENTS_MAX_ROWS", "5000"))

ROOT_RATE_LIMIT = os.environ.get("ROOT_RATE_LIMIT", "20 per minute")

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

_sqlite_lock = threading.Lock()
_sqlite_conn = None  # lazily created per worker

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
    return calculate_geo_distance(BERLIN_CENTER_LAT, BERLIN_CENTER_LON, lat, lon) <= BERLIN_RADIUS_KM

def pg_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)

def init_geocache(conn: sqlite3.Connection):
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=30000;")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS geocache (
            key TEXT PRIMARY KEY,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            updated_at INTEGER NOT NULL
        )
    """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS geocache_updated_at ON geocache(updated_at)")
    conn.commit()

def get_geocache_conn() -> sqlite3.Connection:
    global _sqlite_conn
    if _sqlite_conn is None:
        conn = sqlite3.connect(GEOCACHE_PATH, timeout=30, check_same_thread=False)
        init_geocache(conn)
        _sqlite_conn = conn
    return _sqlite_conn

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

def geocode_with_sqlite_cache(versammlungsort: str, plz: str):
    k = norm_key(plz, versammlungsort)
    now = int(time.time())
    ttl = GEOCACHE_TTL_DAYS * 86400

    conn = get_geocache_conn()

    with _sqlite_lock:
        row = conn.execute(
            "SELECT lat, lon, updated_at FROM geocache WHERE key=?",
            (k,),
        ).fetchone()

        if row:
            lat, lon, updated_at = row
            lat = float(lat)
            lon = float(lon)
            if now - int(updated_at) <= ttl and in_berlin_radius(lat, lon):
                return lat, lon, True

    # Do external geocoding without holding the SQLite lock.
    lat, lon = geocode_place(versammlungsort, plz)
    if lat is None or lon is None:
        return None, None, False

    with _sqlite_lock:
        conn.execute(
            "INSERT OR REPLACE INTO geocache(key, lat, lon, updated_at) VALUES (?, ?, ?, ?)",
            (k, lat, lon, now),
        )
        conn.commit()

    return lat, lon, False

def build_locations(d: date):
    t0 = time.time()

    max_geocodes = EVENTS_MAX_GEOCODES
    max_rows = EVENTS_MAX_ROWS
    req_cache = {}

    with pg_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
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
                lat, lon, hit = geocode_with_sqlite_cache(ort, plz)
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
@app.errorhandler(429)
def ratelimit_handler(e):
    return render_template("429.html"), 429


@app.route("/", methods=["GET", "HEAD"])
@limiter.limit(ROOT_RATE_LIMIT)
def root():
    d = parse_date_param()
    locations = build_locations(d)
    obf = make_locations_obf(locations, d.isoformat())
    return render_template("index.html", date=d.isoformat(), locations_obf=obf)


@limiter.exempt
@app.route("/tiles/<path:path>")
def tiles_proxy(path):
    r = HTTP.get(f"{TILE_URL}/{path}", params=request.args, timeout=30, stream=True)
    return Response(
        r.iter_content(chunk_size=64 * 1024),
        status=r.status_code,
        content_type=r.headers.get("Content-Type", "application/octet-stream"),
    )


@app.route("/<path:unused>")
def block_everything(unused):
    abort(404)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
