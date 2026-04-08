import json
import os
import time
import unicodedata
from decimal import Decimal

import redis
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text


# Class for JSON to understand the Decimal types of the database
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


app = FastAPI(title="CBD: Optimización con Redis")

# --- INFRASTRUCTURE CONFIG ---
DB_HOST = os.getenv("DB_HOST", "almacen-datos")
DB_USER = os.getenv("DB_USER", "user_cbd")
DB_PASS = os.getenv("DB_PASS", "password_cbd")
DB_NAME = os.getenv("DB_NAME", "bd_proyecto_cbd")
REDIS_HOST = os.getenv("REDIS_HOST", "cache-memoria")

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

# Security delay to allow Docker services to stabilize
time.sleep(5)

engine = create_engine(DB_URL)
cache = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)


def normalize_key(text: str) -> str:
    if text is None or str(text).strip() == "":
        return "all"
    text_str = str(text)
    normalized = unicodedata.normalize("NFKD", text_str)
    clean_text = normalized.encode("ascii", "ignore").decode("ascii")
    return clean_text.lower().strip().replace(" ", "_")


@app.get("/films/stats")
def get_film_stats(
    genre: str = Query(None, description="Get aggregate stats by genre"),
):
    """
    Perform heavy aggregation logic on the database and cache the result.
    Demonstrates CPU-saving via Cache-Aside.
    """
    start_time = time.time()

    # Normalize key specifically for stats
    normalized_genre = normalize_key(genre)
    cache_key = f"stats:{normalized_genre}"

    # 1. Cache lookup
    cached_stats = cache.get(cache_key)
    if cached_stats:
        return {
            "data": json.loads(cached_stats),
            "source": "Redis (Cache Hit - Stats)",
            "latency_ms": round((time.time() - start_time) * 1000, 4),
        }

    # 2. Heavy SQL Aggregation
    with engine.connect() as conn:
        query = text("""
            SELECT 
                COUNT(*) as total_count,
                AVG(rating) as avg_rating,
                MIN(release_year) as oldest_year,
                MAX(release_year) as newest_year
            FROM films
            WHERE (CAST(:genre AS TEXT) IS NULL OR genre ILIKE :genre)
        """)
        result = conn.execute(query, {"genre": genre}).fetchone()

        # Build stats dictionary with English keys
        stats = {
            "total_count": result[0],
            "avg_rating": round(float(result[1]), 2) if result[1] else 0,
            "oldest_year": result[2],
            "newest_year": result[3],
        }

        # 3. Store in cache (Cache-Aside)
        cache.setex(cache_key, 600, json.dumps(stats, cls=DecimalEncoder))

        execution_time = (time.time() - start_time) * 1000
        return {
            "data": stats,
            "source": "PostgreSQL (Cache Miss - Stats)",
            "latency_ms": round(execution_time, 4),
        }


@app.get("/films/{id}")
def get_film_by_id(id: int):
    start_time = time.time()

    # 1. Try to retrieve from cache
    cache_key = f"film:{id}"
    cached_data = cache.get(cache_key)

    if cached_data:
        execution_time = (time.time() - start_time) * 1000
        return {
            "data": json.loads(cached_data),
            "source": "Redis (Cache Hit)",
            "latency_ms": round(execution_time, 4),
        }

    # 2. If not in cache (Cache Miss), query the database
    with engine.connect() as conn:
        query = text(
            "SELECT id, title, genre, release_year, rating, director, synopsis "
            "FROM films "
            "WHERE id = :id"
        )
        result = conn.execute(query, {"id": id}).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Film not found")

        film = {
            "id": result[0],
            "title": result[1],
            "genre": result[2],
            "release_year": result[3],
            "rating": result[4],
            "director": result[5],
            "synopsis": result[6],
        }

        # 3. Save to cache for future requests (Cache-Aside)
        # Using DecimalEncoder to handle Decimal types when serializing
        cache.setex(cache_key, 600, json.dumps(film, cls=DecimalEncoder))

        execution_time = (time.time() - start_time) * 1000
        return {
            "data": film,
            "source": "PostgreSQL (Cache Miss)",
            "latency_ms": round(execution_time, 4),
        }


@app.get("/films")
def get_films(genre: str = Query(None, description="Filter films by genre")):
    start_time = time.time()
    cache_key = f"genre:{normalize_key(genre)}"

    # 1. Try to retrieve from cache
    cached_data = cache.get(cache_key)
    if cached_data:
        execution_time = (time.time() - start_time) * 1000
        return {
            "data": json.loads(cached_data),
            "source": "Redis (Cache Hit)",
            "latency_ms": round(execution_time, 4),
        }

    # 2.  If not in cache, query the database
    with engine.connect() as conn:
        query = text(
            "SELECT id, title, genre, release_year, rating, director, synopsis "
            "FROM films "
            "WHERE (CAST(:genre AS TEXT) IS NULL OR genre ILIKE :genre) "
            "LIMIT 500"
        )
        results = conn.execute(query, {"genre": genre}).fetchall()

        films = [
            {
                "id": r[0],
                "title": r[1],
                "genre": r[2],
                "release_year": r[3],
                "rating": r[4],
                "director": r[5],
                "synopsis": r[6],
            }
            for r in results
        ]

        # 3. Save to cache (only if result is not empty to avoid caching misses)
        if films:
            cache.setex(cache_key, 600, json.dumps(films, cls=DecimalEncoder))

        execution_time = (time.time() - start_time) * 1000
        return {
            "data": films,
            "source": "PostgreSQL (Cache Miss)",
            "latency_ms": round(execution_time, 4),
        }


@app.get("/clear-cache")
def clear_cache():
    cache.flushall()
    return {"msg": "Cache purged successfully"}


@app.get("/")
def health_check():
    return {"status": "Ready", "infrastructure": "Dockerized"}
