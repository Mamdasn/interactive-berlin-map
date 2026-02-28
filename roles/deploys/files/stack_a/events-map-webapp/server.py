import os
import json
import base64
import logging
from datetime import datetime, date, timedelta

from flask import Flask, render_template, request, abort
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.middleware.proxy_fix import ProxyFix

from libs.geolib import build_locations

app = Flask(__name__)
app.logger.setLevel(logging.INFO)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    storage_uri=os.environ.get("RATELIMIT_STORAGE_URI", "memory://"),
)

# --- Parameters ---
ROOT_RATE_LIMIT = os.environ.get("ROOT_RATE_LIMIT", "10 per minute")

# --- Utilities ---
def get_max_allowed_date(today: date) -> date:
    return today + timedelta(days=29)


def parse_date_param(today: date, max_date: date):
    allowed = {"date"}
    extra = set(request.args.keys()) - allowed
    if extra:
        abort(400, "Only 'date' query parameter is allowed")

    ds = request.args.get("date")
    if not ds:
        return today

    if len(ds) != 10 or ds[4] != "-" or ds[7] != "-":
        abort(400, "date must be YYYY-MM-DD")

    try:
        parsed_date = datetime.strptime(ds, "%Y-%m-%d").date()
    except ValueError:
        abort(400, "date must be YYYY-MM-DD")

    if parsed_date < today or parsed_date > max_date:
        abort(
            400,
            f"date must be between {today.isoformat()} and {max_date.isoformat()}",
        )

    return parsed_date

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

    today = date.today()
    max_date = get_max_allowed_date(today)
    d = parse_date_param(today, max_date)
    locations = build_locations(d)
    obf = make_locations_obf(locations, d.isoformat())
    return render_template(
        "index.html",
        date=d.isoformat(),
        min_date=today.isoformat(),
        max_date=max_date.isoformat(),
        locations_obf=obf,
    )

@app.route("/<path:unused>")
def block_everything(unused):
    abort(404)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
