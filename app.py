import json
import os
import time
import unicodedata
from decimal import Decimal
from enum import Enum

import pandas as pd
import plotly.express as px
import redis
import streamlit as st
from sqlalchemy import create_engine, text

# ==========================================
# 1. CONFIGURACIÓN E INFRAESTRUCTURA
# ==========================================
st.set_page_config(page_title="CBD Panel - Optimización", layout="wide")


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


class GenreEnum(str, Enum):
    action = "Acción"
    drama = "Drama"
    comedy = "Comedia"
    sci_fi = "Ciencia Ficción"
    horror = "Terror"
    documentary = "Documental"
    thriller = "Suspense"
    adventure = "Aventura"


# Conexiones persistentes (Singleton para la sesión de Streamlit)
@st.cache_resource
def get_db_engine():
    # Intenta leer de secrets (Streamlit Cloud) o de variables de entorno (Docker local)
    db_url = st.secrets.get("DB_URL") if "DB_URL" in st.secrets else os.getenv("DB_URL")
    if not db_url:
        st.error("❌ No se encontró DB_URL en secrets ni en el entorno.")
        st.stop()
    return create_engine(db_url)


@st.cache_resource
def get_redis_client():
    redis_url = (
        st.secrets.get("REDIS_URL")
        if "REDIS_URL" in st.secrets
        else os.getenv("REDIS_URL")
    )
    if not redis_url:
        st.error("❌ No se encontró REDIS_URL en secrets ni en el entorno.")
        st.stop()
    return redis.Redis.from_url(redis_url, decode_responses=True)


engine = get_db_engine()
cache = get_redis_client()

MAX_SEARCH_LIMIT = 3125
CACHE_TTL_SHORT = 300
CACHE_TTL_LONG = 600

# ==========================================
# 2. LÓGICA DE NEGOCIO (EL ANTIGUO BACKEND)
# ==========================================
# Todos los tiempos de ejecución se miden aquí para simular el middleware anterior


def normalize_key(text_val: str) -> str:
    if text_val is None or str(text_val).strip() == "":
        return "all"
    normalized = unicodedata.normalize("NFKD", str(text_val))
    clean_text = normalized.encode("ascii", "ignore").decode("ascii")
    return clean_text.lower().strip().replace(" ", "_")


def invalidate_caches(
    film_id: int | None, old_genre: str | None, new_genre: str | None = None
):
    if film_id:
        cache.delete(f"film:{film_id}")
    cache.delete("genre:all")
    cache.delete("stats:all")

    if old_genre:
        norm_old = normalize_key(old_genre)
        cache.delete(f"genre:{norm_old}")
        cache.delete(f"stats:{norm_old}")

    if new_genre and old_genre != new_genre:
        norm_new = normalize_key(new_genre)
        cache.delete(f"genre:{norm_new}")
        cache.delete(f"stats:{norm_new}")

    search_keys = list(cache.scan_iter("search:*"))
    if search_keys:
        cache.delete(*search_keys)


# --- FUNCIONES DE ACCESO A DATOS ---


def clear_all_cache():
    cache.flushall()


def api_get_film(f_id: int):
    start = time.perf_counter()
    cache_key = f"film:{f_id}"
    cached = cache.get(cache_key)

    if cached:
        lat = (time.perf_counter() - start) * 1000
        return {
            "status": 200,
            "data": json.loads(cached),
            "source": "Redis (Cache Hit)",
            "latency": f"{lat:.4f}",
        }

    with engine.connect() as conn:
        query = text(
            "SELECT id, title, genre, release_year, rating, director, synopsis FROM films WHERE id = :id"
        )
        result = conn.execute(query, {"id": f_id}).fetchone()

        if not result:
            return {"status": 404, "msg": "Film not found"}

        film = {
            "id": result[0],
            "title": result[1],
            "genre": result[2],
            "release_year": result[3],
            "rating": result[4],
            "director": result[5],
            "synopsis": result[6],
        }
        cache.setex(cache_key, CACHE_TTL_LONG, json.dumps(film, cls=DecimalEncoder))

        lat = (time.perf_counter() - start) * 1000
        return {
            "status": 200,
            "data": film,
            "source": "PostgreSQL (Cache Miss)",
            "latency": f"{lat:.4f}",
        }


def api_search_films(q: str):
    start = time.perf_counter()
    clean_q = q.strip()
    cache_key = f"search:{normalize_key(clean_q)}"
    cached = cache.get(cache_key)

    if cached:
        lat = (time.perf_counter() - start) * 1000
        return {
            "status": 200,
            "data": json.loads(cached),
            "source": "Redis (Cache Hit)",
            "latency": f"{lat:.4f}",
        }

    with engine.connect() as conn:
        if not clean_q:
            query = text(
                f"SELECT id, title, genre, release_year, rating, director, synopsis FROM films LIMIT {MAX_SEARCH_LIMIT}"
            )
            results = conn.execute(query).fetchall()
        else:
            query = text(
                f"SELECT id, title, genre, release_year, rating, director, synopsis FROM films WHERE title ILIKE :term OR director ILIKE :term OR synopsis ILIKE :term LIMIT {MAX_SEARCH_LIMIT}"
            )
            results = conn.execute(query, {"term": f"%{clean_q}%"}).fetchall()

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
        cache.setex(cache_key, CACHE_TTL_SHORT, json.dumps(films, cls=DecimalEncoder))

        lat = (time.perf_counter() - start) * 1000
        return {
            "status": 200,
            "data": films,
            "source": "PostgreSQL (Cache Miss)",
            "latency": f"{lat:.4f}",
        }


def api_get_films_by_genre(genre_key: str):
    start = time.perf_counter()
    genre_value = GenreEnum[genre_key].value if genre_key else None
    cache_key = f"genre:{normalize_key(genre_value)}"
    cached = cache.get(cache_key)

    if cached:
        lat = (time.perf_counter() - start) * 1000
        return {
            "status": 200,
            "data": json.loads(cached),
            "source": "Redis (Cache Hit)",
            "latency": f"{lat:.4f}",
        }

    with engine.connect() as conn:
        query = text(
            f"SELECT id, title, genre, release_year, rating, director, synopsis FROM films WHERE (CAST(:genre AS TEXT) IS NULL OR genre ILIKE :genre) LIMIT {MAX_SEARCH_LIMIT}"
        )
        results = conn.execute(query, {"genre": genre_value}).fetchall()
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

        if films:
            cache.setex(
                cache_key, CACHE_TTL_LONG, json.dumps(films, cls=DecimalEncoder)
            )

        lat = (time.perf_counter() - start) * 1000
        return {
            "status": 200,
            "data": films,
            "source": "PostgreSQL (Cache Miss)",
            "latency": f"{lat:.4f}",
        }


def api_get_stats(genre_key: str):
    start = time.perf_counter()
    genre_value = GenreEnum[genre_key].value if genre_key else None
    cache_key = f"stats:{normalize_key(genre_value)}"
    cached = cache.get(cache_key)

    if cached:
        lat = (time.perf_counter() - start) * 1000
        return {
            "status": 200,
            "data": json.loads(cached),
            "source": "Redis (Cache Hit)",
            "latency": f"{lat:.4f}",
        }

    with engine.connect() as conn:
        query = text("""
            SELECT COUNT(*) as total_count, AVG(rating) as avg_rating, MIN(release_year) as oldest_year, MAX(release_year) as newest_year
            FROM films WHERE (CAST(:genre AS TEXT) IS NULL OR genre ILIKE :genre)
        """)
        result = conn.execute(query, {"genre": genre_value}).fetchone()
        stats = {
            "total_count": result[0],
            "avg_rating": round(float(result[1]), 2) if result[1] else 0,
            "oldest_year": result[2],
            "newest_year": result[3],
        }
        cache.setex(cache_key, CACHE_TTL_LONG, json.dumps(stats, cls=DecimalEncoder))
        lat = (time.perf_counter() - start) * 1000
        return {
            "status": 200,
            "data": stats,
            "source": "PostgreSQL (Cache Miss)",
            "latency": f"{lat:.4f}",
        }


def api_create_film(data: dict):
    with engine.connect() as conn:
        query = text("""
            INSERT INTO films (title, genre, release_year, rating, director, synopsis)
            VALUES (:title, :genre, :year, :rating, :director, :synopsis) RETURNING id
        """)
        result = conn.execute(
            query,
            {
                "title": data["title"],
                "genre": data["genre"],
                "year": data["release_year"],
                "rating": data["rating"],
                "director": data["director"],
                "synopsis": data["synopsis"],
            },
        )
        new_id = result.fetchone()[0]
        conn.commit()
    invalidate_caches(None, data["genre"], None)
    return {"status": 200, "id": new_id}


def api_update_film(f_id: int, data: dict):
    with engine.connect() as conn:
        old_data = conn.execute(
            text("SELECT genre FROM films WHERE id = :id"), {"id": f_id}
        ).fetchone()
        if not old_data:
            return {"status": 404, "msg": "Film not found"}

        old_genre = old_data.genre
        new_genre = data["genre"]

        query = text("""
            UPDATE films SET title = :title, genre = :genre, release_year = :year, 
            rating = :rating, director = :director, synopsis = :synopsis WHERE id = :id
        """)
        conn.execute(
            query,
            {
                "title": data["title"],
                "genre": new_genre,
                "year": data["release_year"],
                "rating": data["rating"],
                "director": data["director"],
                "synopsis": data["synopsis"],
                "id": f_id,
            },
        )
        conn.commit()
    invalidate_caches(f_id, old_genre, new_genre)
    return {"status": 200}


def api_delete_film(f_id: int):
    with engine.connect() as conn:
        row = (
            conn.execute(text("SELECT genre FROM films WHERE id = :id"), {"id": f_id})
            .mappings()
            .fetchone()
        )
        if not row:
            return {"status": 404, "msg": "Film not found"}
        genre = row["genre"]
        conn.execute(text("DELETE FROM films WHERE id = :id"), {"id": f_id})
        conn.commit()
    invalidate_caches(f_id, genre, None)
    return {"status": 200}


# ==========================================
# 3. INTERFAZ DE USUARIO (FRONTEND ESTRICTO)
# ==========================================

if "telemetry" not in st.session_state:
    st.session_state.telemetry = []
if "edit_film" not in st.session_state:
    st.session_state.edit_film = None
if "execute_delete_for_id" not in st.session_state:
    st.session_state.execute_delete_for_id = None


def log_telemetry(query_type: str, source: str, latency: str):
    source_clean = "Redis (Caché)" if "Redis" in source else "PostgreSQL (DB)"
    st.session_state.telemetry.append(
        {
            "Tipo de Consulta": query_type,
            "Origen": source_clean,
            "Latencia (ms)": float(latency),
        }
    )


def display_performance_tag(source: str, latency: str):
    if "Redis" in source:
        st.success(f"🚀 **{latency} ms** | Respuesta Instantánea (Caché Hit)")
    else:
        st.warning(f"⏳ **{latency} ms** | Procesamiento en BD (Caché Miss)")


def display_ultra_minimal_card(film: dict):
    with st.container(border=True):
        st.markdown(
            f"**{film['title']}** ({film['release_year']})  |  {film['genre']}\n\n"
            f"⭐ {film['rating']}  |  🎥 {film['director']}"
        )


def display_detailed_card(film: dict, source: str, latency: str):
    with st.container(border=True):
        c1, c2 = st.columns([0.85, 0.15])
        with c1:
            st.markdown(f"## 🎬 {film['title']}")
        with c2:
            if "Redis" in source:
                st.markdown(
                    f"<div style='text-align: right; color: #10b981; font-weight: bold;'>⚡ {latency} ms</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"<div style='text-align: right; color: #f59e0b; font-weight: bold;'>💾 {latency} ms</div>",
                    unsafe_allow_html=True,
                )
        col1, col2, col3 = st.columns(3)
        col1.metric("Año", film["release_year"])
        col2.metric("Puntuación", f"⭐ {film['rating']}")
        col3.metric("Género", film["genre"])
        st.markdown(f"**Director:** {film['director']}")
        st.markdown(f"**Sinopsis:** {film['synopsis']}")


@st.dialog("⚠️ Confirmar Borrado")
def confirm_delete_dialog(film_id):
    st.write(f"Vas a eliminar permanentemente la película con ID **{film_id}**.")
    st.warning("Esta acción no se puede deshacer.")
    c1, c2 = st.columns(2)
    if c1.button("Cancelar", use_container_width=True):
        st.rerun()
    if c2.button("Confirmar", type="primary", use_container_width=True):
        st.session_state.execute_delete_for_id = film_id
        st.rerun()


st.title("Sistema de Gestión de Catálogo")

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Infraestructura")
    if st.button("🧹 Purgar Caché Redis", use_container_width=True):
        try:
            clear_all_cache()
            st.success("Caché limpia.")
        except Exception as e:
            st.error(f"Error limpiando caché: {e}")
    if st.button("🗑️ Limpiar Telemetría", use_container_width=True):
        st.session_state.telemetry = []
        st.success("Historial de telemetría reestablecido.")

# --- TABS ---
tab_explore, tab_manage, tab_telemetry = st.tabs(
    ["🔍 Explorar", "🛠️ Gestión (CUD)", "📈 Telemetría"]
)

with tab_explore:
    search_type = st.radio(
        "Método:",
        ["ID", "Texto", "Género"],
        horizontal=True,
        label_visibility="collapsed",
    )
    col_input, col_btn = st.columns([3, 1])

    if search_type == "ID":
        f_id = col_input.number_input(
            "ID", min_value=1, value=100, label_visibility="collapsed"
        )
        if col_btn.button("🔍 Buscar ID", use_container_width=True):
            res = api_get_film(f_id)
            if res["status"] == 200:
                display_performance_tag(res["source"], res["latency"])
                display_detailed_card(res["data"], res["source"], res["latency"])
                log_telemetry("ID", res["source"], res["latency"])
            else:
                st.error("No existe ninguna película con ese ID.")

    elif search_type == "Texto":
        q = col_input.text_input(
            "Buscar...",
            label_visibility="collapsed",
            placeholder="Ej.: agua, aire, mar...",
        )
        if col_btn.button("🔍 Buscar Texto", use_container_width=True):
            res = api_search_films(q)
            if res["status"] == 200:
                display_performance_tag(res["source"], res["latency"])
                if q.strip():
                    st.markdown(
                        f"#### Coincidencias de '{q}' en título, director o sinopsis: {len(res['data'])}"
                    )
                else:
                    st.markdown(f"#### Películas disponibles: {len(res['data'])}")

                cols = st.columns(2)
                for idx, film in enumerate(res["data"][:10]):
                    with cols[idx % 2]:
                        display_ultra_minimal_card(film)

                if len(res["data"]) > 10:
                    st.caption(f"👀 *... y {len(res['data']) - 10:,} películas más.*")
                log_telemetry("Texto", res["source"], res["latency"])

    elif search_type == "Género":
        genre_map = {g.name: g.value for g in GenreEnum}
        selected_label = col_input.selectbox(
            "Género", list(genre_map.values()), label_visibility="collapsed"
        )
        if col_btn.button("🔍 Buscar Género", use_container_width=True):
            selected_key = next(k for k, v in genre_map.items() if v == selected_label)
            res_films = api_get_films_by_genre(selected_key)
            res_stats = api_get_stats(selected_key)

            if res_stats["status"] == 200 and res_films["status"] == 200:
                display_performance_tag(res_stats["source"], res_stats["latency"])

                sc1, sc2, sc3, sc4 = st.columns(4)
                total_films = res_stats["data"]["total_count"]
                sc1.metric("Películas", total_films)
                sc2.metric("Nota Media", res_stats["data"]["avg_rating"])
                sc3.metric("Primer estreno", res_stats["data"]["oldest_year"])
                sc4.metric("Último Estreno", res_stats["data"]["newest_year"])

                st.divider()
                st.markdown(f"#### Muestra del catálogo: {selected_label}")

                cols = st.columns(2)
                for idx, film in enumerate(res_films["data"][:10]):
                    with cols[idx % 2]:
                        display_ultra_minimal_card(film)

                if total_films > 10:
                    st.caption(f"👀 *... y {total_films - 10:,} películas más.*")

                log_telemetry("Stats", res_stats["source"], res_stats["latency"])
                log_telemetry("Lista Género", res_films["source"], res_films["latency"])

with tab_manage:
    st.caption(
        "Cualquier escritura aquí invalidará automáticamente las cachés globales en Redis."
    )
    t_create, t_update, t_delete = st.tabs(["Crear", "Actualizar", "Eliminar"])

    with t_create:
        with st.form("form_create", border=False):
            genre_map = {g.name: g.value for g in GenreEnum}
            c1, c2, c3 = st.columns([2, 1, 1])
            c_title = c1.text_input("Título")
            c_genre = c2.selectbox("Género", list(genre_map.values()))
            c_year = c3.number_input("Año", 1888, 2030, 2024)

            c4, c5 = st.columns([2, 1])
            c_director = c4.text_input("Director")
            c_rating = c5.slider("Puntuación", 0.0, 10.0, 5.0)
            c_synopsis = st.text_area("Sinopsis", height=80)

            if st.form_submit_button("➕ Crear Película", type="primary"):
                if (
                    c_title.strip()
                    and c_genre
                    and c_year
                    and c_rating
                    and c_director.strip()
                    and c_synopsis.strip()
                ):
                    payload = {
                        "title": c_title.strip(),
                        "genre": c_genre,
                        "release_year": c_year,
                        "rating": c_rating,
                        "director": c_director.strip(),
                        "synopsis": c_synopsis.strip(),
                    }
                    res = api_create_film(payload)
                    if res["status"] == 200:
                        st.toast("✅ Película Creada. Caché invalidada.")
                        st.success(f"ID Creado: {res['id']}")
                else:
                    st.warning("Por favor, completa todos los campos.")

    with t_update:
        c_id, c_btn = st.columns([1, 4])
        u_id = c_id.number_input(
            "ID", min_value=1, step=1, label_visibility="collapsed"
        )
        if c_btn.button("📥 Cargar Datos"):
            res = api_get_film(u_id)
            if res["status"] == 200:
                st.session_state.edit_film = res["data"]
            else:
                st.error("No existe ninguna película con ese ID.")

        if st.session_state.edit_film:
            f_edit = st.session_state.edit_film
            with st.form("form_update", border=False):
                genre_map = {g.name: g.value for g in GenreEnum}
                g_labels = list(genre_map.values())
                def_idx = (
                    g_labels.index(f_edit["genre"])
                    if f_edit["genre"] in g_labels
                    else 0
                )

                c1, c2, c3 = st.columns([2, 1, 1])
                u_title = c1.text_input("Título", value=f_edit["title"])
                u_genre = c2.selectbox("Género", g_labels, index=def_idx)
                u_year = c3.number_input(
                    "Año", 1888, 2030, value=f_edit["release_year"]
                )

                c4, c5 = st.columns([2, 1])
                u_director = c4.text_input("Director", value=f_edit["director"])
                u_rating = c5.slider(
                    "Puntuación", 0.0, 10.0, value=float(f_edit["rating"])
                )
                u_synopsis = st.text_area(
                    "Sinopsis", value=f_edit["synopsis"], height=80
                )

                if st.form_submit_button("💾 Guardar Cambios", type="primary"):
                    payload = {
                        "title": u_title.strip(),
                        "genre": u_genre,
                        "release_year": u_year,
                        "rating": u_rating,
                        "director": u_director.strip(),
                        "synopsis": u_synopsis.strip(),
                    }
                    res = api_update_film(f_edit["id"], payload)
                    if res["status"] == 200:
                        st.toast("♻️ Película actualizada. Caché invalidada.")
                        st.success(f"ID Actualizado: {u_id}")
                        st.session_state.edit_film.update(payload)

    with t_delete:
        c_id, c_btn = st.columns([1, 4])
        d_id = c_id.number_input(
            "ID", min_value=1, step=1, key="del_id", label_visibility="collapsed"
        )
        if c_btn.button("🗑️ Eliminar Película", type="primary"):
            res_check = api_get_film(d_id)
            if res_check["status"] == 200:
                confirm_delete_dialog(d_id)
            else:
                st.error("No existe ninguna película con ese ID.")

        if st.session_state.execute_delete_for_id:
            del_id = st.session_state.execute_delete_for_id
            st.session_state.execute_delete_for_id = None
            res = api_delete_film(del_id)
            if res["status"] == 200:
                st.toast("🗑️ Película eliminada. Caché invalidada.")
                st.success(f"ID Eliminado: {del_id}")

with tab_telemetry:
    st.info("""
    **Protocolo de Evaluación Automatizado:** Se ejecutará una ráfaga secuencial de 24 peticiones nativas. 
    El objetivo es forzar 'Cache Misses' (consulta a PostgreSQL) seguidos inmediatamente de 
    'Cache Hits' (Redis) para la misma consulta, demostrando la caída radical de latencia.
    """)

    battery_tests = [
        ("Búsqueda por ID", lambda: api_get_film(123), "PostgreSQL"),
        ("Búsqueda por ID", lambda: api_get_film(123), "Redis"),
        ("Búsqueda por ID", lambda: api_get_film(456), "PostgreSQL"),
        ("Búsqueda por ID", lambda: api_get_film(456), "Redis"),
        ("Búsqueda por ID", lambda: api_get_film(789), "PostgreSQL"),
        ("Búsqueda por ID", lambda: api_get_film(789), "Redis"),
        ("Búsqueda por Texto", lambda: api_search_films("agua"), "PostgreSQL"),
        ("Búsqueda por Texto", lambda: api_search_films("agua"), "Redis"),
        ("Búsqueda por Texto", lambda: api_search_films("aire"), "PostgreSQL"),
        ("Búsqueda por Texto", lambda: api_search_films("aire"), "Redis"),
        ("Búsqueda por Texto", lambda: api_search_films("mar"), "PostgreSQL"),
        ("Búsqueda por Texto", lambda: api_search_films("mar"), "Redis"),
        ("Búsqueda por Género", lambda: api_get_films_by_genre("drama"), "PostgreSQL"),
        ("Búsqueda por Género", lambda: api_get_films_by_genre("drama"), "Redis"),
        ("Búsqueda por Género", lambda: api_get_films_by_genre("horror"), "PostgreSQL"),
        ("Búsqueda por Género", lambda: api_get_films_by_genre("horror"), "Redis"),
        (
            "Búsqueda por Género",
            lambda: api_get_films_by_genre("adventure"),
            "PostgreSQL",
        ),
        ("Búsqueda por Género", lambda: api_get_films_by_genre("adventure"), "Redis"),
        ("Cálculo de Stats por Género", lambda: api_get_stats("drama"), "PostgreSQL"),
        ("Cálculo de Stats por Género", lambda: api_get_stats("drama"), "Redis"),
        ("Cálculo de Stats por Género", lambda: api_get_stats("horror"), "PostgreSQL"),
        ("Cálculo de Stats por Género", lambda: api_get_stats("horror"), "Redis"),
        (
            "Cálculo de Stats por Género",
            lambda: api_get_stats("adventure"),
            "PostgreSQL",
        ),
        ("Cálculo de Stats por Género", lambda: api_get_stats("adventure"), "Redis"),
    ]

    with st.expander("📄 Detalle de las peticiones a ejecutar", expanded=False):
        df_battery = pd.DataFrame(
            [(t[0], "Función Interna", t[2]) for t in battery_tests],
            columns=["Tipo de Petición", "Ejecución", "Origen Esperado"],
        )
        st.table(df_battery)

    if st.button("▶️ Ejecutar Batería de Pruebas Automatizada", type="primary"):
        progress = st.progress(0)
        for i, (test_name, func, _) in enumerate(battery_tests):
            res = func()
            if res.get("status") == 200:
                log_telemetry(
                    test_name,
                    res.get("source", "PostgreSQL (DB)"),
                    res.get("latency", "0"),
                )
            time.sleep(0.1)  # Pequeño delay visual para que se vea el progreso
            progress.progress((i + 1) / len(battery_tests))

    st.divider()

    if not st.session_state.telemetry:
        st.warning(
            "⚠️ Sin datos. Ejecuta la batería de pruebas o realiza consultas manualmente para visualizar el análisis."
        )
    else:
        df = pd.DataFrame(st.session_state.telemetry)
        st.subheader("📊 Resultados de Rendimiento")

        st.markdown("**1. Resumen Estadístico por Tipo de Operación**")
        stast_summary = (
            df.groupby(["Tipo de Consulta", "Origen"])["Latencia (ms)"]
            .agg(["count", "mean", "median", "min", "max"])
            .reset_index()
        )
        stast_summary.columns = [
            "Operación",
            "Origen",
            "Nº Peticiones",
            "Media (ms)",
            "Mediana (ms)",
            "T. Mínimo (ms)",
            "T. Máximo (ms)",
        ]
        for col in ["Media (ms)", "Mediana (ms)", "T. Mínimo (ms)", "T. Máximo (ms)"]:
            stast_summary[col] = stast_summary[col].round(3)

        st.dataframe(stast_summary, hide_index=True, use_container_width=True)

        st.markdown("**2. Análisis de Aceleración por Operación**")
        pivot_df = stast_summary.pivot(
            index="Operación", columns="Origen", values="Mediana (ms)"
        ).reset_index()
        if (
            "PostgreSQL (DB)" in pivot_df.columns
            and "Redis (Caché)" in pivot_df.columns
        ):
            pivot_df["Mejora (x)"] = (
                pivot_df["PostgreSQL (DB)"] / pivot_df["Redis (Caché)"]
            ).round(1)
            pivot_df["Mejora (x)"] = pivot_df["Mejora (x)"].apply(lambda x: f"🚀 {x}x")
            st.dataframe(pivot_df, hide_index=True, use_container_width=True)

            median_arq_upgrade = (
                pivot_df["PostgreSQL (DB)"] / pivot_df["Redis (Caché)"]
            ).median()
            if pd.notna(median_arq_upgrade) and median_arq_upgrade > 0:
                st.info(
                    f"🏆 **Capacidad de Arquitectura:** En promedio (mediana), la caché acelera una operación típica en **{median_arq_upgrade:.1f}x**."
                )

        st.divider()

        col_a, col_b = st.columns(2)
        color_map = {"Redis (Caché)": "#10b981", "PostgreSQL (DB)": "#f59e0b"}

        with col_a:
            st.markdown("**3. Comparativa de Latencia por Petición**")
            fig_bar = px.bar(
                stast_summary,
                x="Operación",
                y="Media (ms)",
                color="Origen",
                barmode="group",
                text_auto=".1f",
                color_discrete_map=color_map,
            )
            fig_bar.update_traces(textposition="outside", textfont_size=12)
            fig_bar.update_layout(
                showlegend=True,
                height=380,
                margin=dict(t=10, b=10),
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                ),
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with col_b:
            st.markdown("**4. Slope Chart**")
            st.caption(
                "Líneas de conexión directas entre las consultas a la BD y a la Caché."
            )
            fig_slope = px.line(
                stast_summary,
                x="Origen",
                y="Media (ms)",
                color="Operación",
                markers=True,
            )
            fig_slope.update_traces(line=dict(width=4), marker=dict(size=12))
            fig_slope.update_layout(
                height=380,
                margin=dict(t=10, b=10),
                xaxis={
                    "categoryorder": "array",
                    "categoryarray": ["PostgreSQL (DB)", "Redis (Caché)"],
                },
            )
            st.plotly_chart(fig_slope, use_container_width=True)
