import logging
import os

import requests

from libs.db_clients import get_redis
from libs.geo_utils import get_berlin_viewbox, in_berlin_radius, norm_key

NOMINATIM_URL = os.environ.get("NOMINATIM_URL", "http://nominatim:8080")
NOMINATIM_TIMEOUT = float(os.environ.get("NOMINATIM_TIMEOUT", "5"))
GEOCACHE_TTL_DAYS = int(os.environ.get("GEOCACHE_TTL_DAYS", "1"))
REDIS_DB_GEO = int(os.environ.get("REDIS_DB_GEO", "0"))


def geocode_place(versammlungsort: str, plz: str):
    q = f"{versammlungsort} {plz} Berlin"
    try:
        r = requests.get(
            f"{NOMINATIM_URL}/search",
            headers={"User-Agent": "berlin-protest-map/1.0"},
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
        logging.warning("Nominatim request failed for q=%r: %s", q, e)
        return None, None


def geocode_from_redis_cache(rgeo, k: str):
    try:
        v = rgeo.get(k)
        if not v:
            return None, None, False

        lat_s, lon_s = v.split(",", 1)
        lat = float(lat_s)
        lon = float(lon_s)
        if in_berlin_radius(lat, lon):
            return lat, lon, True

        rgeo.delete(k)
    except Exception as e:
        logging.warning("Redis geo cache read failed: %s", e)

    return None, None, False


def geocode_from_nominatim(versammlungsort: str, plz: str):
    lat, lon = geocode_place(versammlungsort, plz)
    if lat is None or lon is None:
        return None, None
    return lat, lon


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

    lat, lon, hit = geocode_from_redis_cache(rgeo, k)
    if hit:
        return lat, lon, True

    lat, lon = geocode_from_nominatim(versammlungsort, plz)
    if lat is None or lon is None:
        return None, None, False

    # cache geocoes in Redis
    try:
        rgeo.setex(k, ttl, f"{lat},{lon}")
    except Exception as e:
        logging.warning("Redis geo cache write failed: %s", e)

    return lat, lon, False
