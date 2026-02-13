import os
from functools import lru_cache

import redis
import requests
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")



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

@lru_cache(maxsize=None)
def initiate_nominatim_http_session():
    session = requests.Session()
    session.headers.update({"User-Agent": "berlin-protest-map/1.0"})
    return session

def pg_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)
