import json
import os
import time
from decimal import Decimal

import redis
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text


# Clase para que JSON entienda los tipos Decimal de la base de datos
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


app = FastAPI(title="CBD: Optimización con Redis")

# --- CONFIGURACIÓN DE INFRAESTRUCTURA ---
DB_HOST = os.getenv("DB_HOST", "almacen-datos")
REDIS_HOST = os.getenv("REDIS_HOST", "cache-memoria")
DB_USER = "user_cbd"
DB_PASS = "password_cbd"
DB_NAME = "bd_proyecto_cbd"

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

# Retardo de seguridad para permitir que los servicios de Docker estabilicen
time.sleep(5)

engine = create_engine(DB_URL)
# decode_responses=True para que Redis nos devuelva strings y no bytes
cache = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)


@app.get("/")
def health_check():
    return {"status": "Ready", "infrastructure": "Dockerized"}


@app.get("/producto/{id}")
def get_producto(id: int):
    """
    Implementación del patrón Cache-Aside.
    Busca primero en memoria (Redis) y solo baja a disco (SQL) si no existe.
    """
    start_time = time.time()

    # 1. Intentar recuperar de la caché
    cache_key = f"prod:{id}"
    cached_data = cache.get(cache_key)

    if cached_data:
        execution_time = (time.time() - start_time) * 1000
        return {
            "data": json.loads(cached_data),
            "source": "cache-memoria (Redis)",
            "latency_ms": round(execution_time, 4),
        }

    # 2. Si no está en caché (Cache Miss), consultar la base de datos
    with engine.connect() as conn:
        query = text(
            "SELECT id, nombre, categoria, precio, descripcion FROM productos WHERE id = :id"
        )
        result = conn.execute(query, {"id": id}).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Formatear el resultado
        producto = {
            "id": result[0],
            "nombre": result[1],
            "categoria": result[2],
            "precio": result[3],
            "descripcion": result[4],
        }

        # 3. Guardar en caché para futuras peticiones
        # Usamos DecimalEncoder para que el precio no de error al serializar
        cache.setex(cache_key, 600, json.dumps(producto, cls=DecimalEncoder))

        execution_time = (time.time() - start_time) * 1000
        return {
            "data": producto,
            "source": "almacen-datos (PostgreSQL)",
            "latency_ms": round(execution_time, 4),
        }


@app.get("/productos/categoria/{cat}")
def get_productos_por_categoria(cat: str):
    start_time = time.time()
    cache_key = f"cat:{cat}"

    # 1. Intentar Redis
    cached_data = cache.get(cache_key)
    if cached_data:
        return {
            "data": json.loads(cached_data),
            "source": "cache-memoria (Redis)",
            "latency_ms": round((time.time() - start_time) * 1000, 4),
        }

    # 2. Si falla, ir a Postgres
    with engine.connect() as conn:
        query = text(
            "SELECT id, nombre, categoria, precio FROM productos WHERE categoria = :cat LIMIT 500"
        )
        results = conn.execute(query, {"cat": cat}).fetchall()

        productos = [
            {"id": r[0], "nombre": r[1], "categoria": r[2], "precio": float(r[3])}
            for r in results
        ]

        # 3. Guardar en caché
        cache.setex(cache_key, 600, json.dumps(productos))

        return {
            "data": productos,
            "source": "almacen-datos (PostgreSQL)",
            "latency_ms": round((time.time() - start_time) * 1000, 4),
        }


# Endpoint para limpiar la caché y forzar el "sufrimiento" de la DB en las pruebas
@app.get("/clear-cache")
def clear_cache():
    cache.flushall()
    return {"msg": "Caché vaciada correctamente"}
