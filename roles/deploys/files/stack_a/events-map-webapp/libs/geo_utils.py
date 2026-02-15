import math
import os

BERLIN_CENTER_LAT = float(os.environ.get("BERLIN_CENTER_LAT", "52.5200"))
BERLIN_CENTER_LON = float(os.environ.get("BERLIN_CENTER_LON", "13.4050"))
BERLIN_RADIUS_KM = float(os.environ.get("BERLIN_RADIUS_KM", "30.0"))


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


def norm_key(plz: str, ort: str) -> str:
    plz = (plz or "").strip().upper()
    ort = " ".join((ort or "").strip().split())
    return f"{plz}|{ort}"


def get_berlin_viewbox():
    _deg_lat = BERLIN_RADIUS_KM / 111.32
    _deg_lon = BERLIN_RADIUS_KM / (111.32 * math.cos(math.radians(BERLIN_CENTER_LAT)))

    BERLIN_MIN_LAT = BERLIN_CENTER_LAT - _deg_lat
    BERLIN_MAX_LAT = BERLIN_CENTER_LAT + _deg_lat
    BERLIN_MIN_LON = BERLIN_CENTER_LON - _deg_lon
    BERLIN_MAX_LON = BERLIN_CENTER_LON + _deg_lon

    NOMINATIM_VIEWBOX = f"{BERLIN_MIN_LON},{BERLIN_MAX_LAT},{BERLIN_MAX_LON},{BERLIN_MIN_LAT}"
    return NOMINATIM_VIEWBOX
