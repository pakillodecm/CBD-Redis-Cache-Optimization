import json
import time
import unicodedata
from decimal import Decimal
from enum import Enum

import pandas as pd
import plotly.express as px
import redis
import streamlit as st
from sqlalchemy import create_engine, text

# --- CONFIG ---
DB_URL = st.secrets.get(
    "DB_URL", "postgresql://user_cbd:password_cbd@localhost:5433/bd_proyecto_cbd"
)
REDIS_HOST = st.secrets.get("REDIS_HOST", "localhost")
REDIS_PORT = int(st.secrets.get("REDIS_PORT", 6380))
REDIS_PASS = st.secrets.get("REDIS_PASS", None)


@st.cache_resource
def init_connections():
    engine = create_engine(DB_URL)
    cache = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASS,
        db=0,
        decode_responses=True,
    )
    return engine, cache


engine, cache = init_connections()

# --- CONSTANTS & ENUMS ---
CACHE_TTL_SHORT = 300
CACHE_TTL_LONG = 600
MAX_SEARCH_LIMIT = 3125


class GenreEnum(str, Enum):
    action = "Acción"
    drama = "Drama"
    comedy = "Comedia"
    sci_fi = "Ciencia Ficción"
    horror = "Terror"
    documentary = "Documental"
    thriller = "Suspense"
    adventure = "Aventura"


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


# --- HELPERS ---
def normalize_key(text_in: str) -> str:
    if text_in is None or str(text_in).strip() == "":
        return "all"
    normalized = unicodedata.normalize("NFKD", str(text_in))
    clean_text = normalized.encode("ascii", "ignore").decode("ascii")
    return clean_text.lower().strip().replace(" ", "_")


def invalidate_caches(film_id=None, old_genre=None, new_genre=None):
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


# --- LOGIC ---
def get_film_by_id_logic(f_id):
    start = time.perf_counter()
    cache_key = f"film:{f_id}"
    cached = cache.get(cache_key)
    if cached:
        return json.loads(cached), "Redis (Caché)", (time.perf_counter() - start) * 1000

    with engine.connect() as conn:
        res = conn.execute(
            text(
                "SELECT id,title,genre,release_year,rating,director,synopsis FROM films WHERE id=:id"
            ),
            {"id": f_id},
        ).fetchone()
    if res:
        film = dict(res._mapping)
        film["rating"] = float(film["rating"])
        cache.setex(cache_key, CACHE_TTL_LONG, json.dumps(film, cls=DecimalEncoder))
        return film, "PostgreSQL (DB)", (time.perf_counter() - start) * 1000
    return None, None, 0


def get_films_logic(genre_key=None):
    start = time.perf_counter()
    genre_val = (
        GenreEnum[genre_key].value if genre_key in GenreEnum.__members__ else None
    )
    cache_key = f"genre:{normalize_key(genre_val)}"
    cached = cache.get(cache_key)
    if cached:
        return json.loads(cached), "Redis (Caché)", (time.perf_counter() - start) * 1000

    with engine.connect() as conn:
        query = text(
            f"SELECT id,title,genre,release_year,rating,director,synopsis FROM films WHERE (CAST(:g AS TEXT) IS NULL OR genre ILIKE :g) LIMIT {MAX_SEARCH_LIMIT}"
        )
        results = conn.execute(query, {"g": genre_val}).fetchall()
    films = [dict(r._mapping) for r in results]
    for f in films:
        f["rating"] = float(f["rating"])
    cache.setex(cache_key, CACHE_TTL_LONG, json.dumps(films, cls=DecimalEncoder))
    return films, "PostgreSQL (DB)", (time.perf_counter() - start) * 1000


def get_stats_logic(genre_key=None):
    start = time.perf_counter()
    genre_val = (
        GenreEnum[genre_key].value if genre_key in GenreEnum.__members__ else None
    )
    cache_key = f"stats:{normalize_key(genre_val)}"
    cached = cache.get(cache_key)
    if cached:
        return json.loads(cached), "Redis (Caché)", (time.perf_counter() - start) * 1000

    with engine.connect() as conn:
        query = text(
            "SELECT COUNT(*), AVG(rating), MIN(release_year), MAX(release_year) FROM films WHERE (CAST(:g AS TEXT) IS NULL OR genre ILIKE :g)"
        )
        r = conn.execute(query, {"g": genre_val}).fetchone()
    stats = {
        "total_count": r[0],
        "avg_rating": round(float(r[1]), 2) if r[1] else 0,
        "oldest_year": r[2],
        "newest_year": r[3],
    }
    cache.setex(cache_key, CACHE_TTL_LONG, json.dumps(stats, cls=DecimalEncoder))
    return stats, "PostgreSQL (DB)", (time.perf_counter() - start) * 1000


def search_films_logic(q=""):
    start = time.perf_counter()
    cache_key = f"search:{normalize_key(q)}"
    cached = cache.get(cache_key)
    if cached:
        return json.loads(cached), "Redis (Caché)", (time.perf_counter() - start) * 1000

    with engine.connect() as conn:
        term = f"%{q.strip()}%"
        query = text(
            f"SELECT id,title,genre,release_year,rating,director,synopsis FROM films WHERE title ILIKE :t OR director ILIKE :t OR synopsis ILIKE :t LIMIT {MAX_SEARCH_LIMIT}"
        )
        results = conn.execute(query, {"t": term}).fetchall()
    films = [dict(r._mapping) for r in results]
    for f in films:
        f["rating"] = float(f["rating"])
    cache.setex(cache_key, CACHE_TTL_SHORT, json.dumps(films, cls=DecimalEncoder))
    return films, "PostgreSQL (DB)", (time.perf_counter() - start) * 1000


# --- UI HELPER FUNCTIONS ---
def log_telemetry(query_type: str, source: str, latency: float):
    source_clean = "Redis (Caché)" if "Redis" in source else "PostgreSQL (DB)"
    st.session_state.telemetry.append(
        {
            "Tipo de Consulta": query_type,
            "Origen": source_clean,
            "Latencia (ms)": float(latency),
        }
    )


def display_performance_tag(source: str, latency: float):
    if "Redis" in source:
        st.success(f"🚀 **{latency:.2f} ms** | Respuesta Instantánea (Caché Hit)")
    else:
        st.warning(f"⏳ **{latency:.2f} ms** | Procesamiento en BD (Caché Miss)")


def display_ultra_minimal_card(film: dict):
    with st.container(border=True):
        st.markdown(
            f"**{film['title']}** ({film['release_year']}) | {film['genre']}\n\n⭐ {film['rating']} | 🎥 {film['director']}"
        )


def display_detailed_card(film: dict, source: str, latency: float):
    with st.container(border=True):
        c1, c2 = st.columns([0.85, 0.15])
        with c1:
            st.markdown(f"## 🎬 {film['title']}")
        with c2:
            color = "#10b981" if "Redis" in source else "#f59e0b"
            st.markdown(
                f"<div style='text-align: right; color: {color}; font-weight: bold;'>{latency:.2f} ms</div>",
                unsafe_allow_html=True,
            )
        col1, col2, col3 = st.columns(3)
        col1.metric("Año", film["release_year"])
        col2.metric("Puntuación", f"⭐ {film['rating']}")
        col3.metric("Género", film["genre"])
        st.markdown(
            f"**Director:** {film['director']}\n\n**Sinopsis:** {film['synopsis']}"
        )


@st.dialog("⚠️ Confirmar Borrado")
def confirm_delete_dialog(film_id):
    st.write(f"Vas a eliminar la película con ID **{film_id}**.")
    st.warning("Esta acción es irreversible.")
    c1, c2 = st.columns(2)
    if c1.button("Cancelar", use_container_width=True):
        st.rerun()
    if c2.button("Confirmar", type="primary", use_container_width=True):
        st.session_state.execute_delete_for_id = film_id
        st.rerun()


# --- MAIN APP UI ---
st.set_page_config(page_title="CBD Panel - Optimización", layout="wide")
st.title("Sistema de Gestión de Catálogo")

if "telemetry" not in st.session_state:
    st.session_state.telemetry = []
if "edit_film" not in st.session_state:
    st.session_state.edit_film = None
if "execute_delete_for_id" not in st.session_state:
    st.session_state.execute_delete_for_id = None

with st.sidebar:
    st.header("⚙️ Infraestructura")
    if st.button("🧹 Purgar Caché Redis", use_container_width=True):
        cache.flushall()
        st.success("Caché limpia.")
    if st.button("🗑️ Limpiar Telemetría", use_container_width=True):
        st.session_state.telemetry = []
        st.success("Historial reestablecido.")

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
            data, src, lat = get_film_by_id_logic(f_id)
            if data:
                display_performance_tag(src, lat)
                display_detailed_card(data, src, lat)
                log_telemetry("ID", src, lat)
            else:
                st.error("No existe.")

    elif search_type == "Texto":
        q = col_input.text_input(
            "Buscar...", label_visibility="collapsed", placeholder="Ej.: agua, aire..."
        )
        if col_btn.button("🔍 Buscar Texto", use_container_width=True):
            data, src, lat = search_films_logic(q)
            display_performance_tag(src, lat)
            st.markdown(f"#### Resultados: {len(data)}")
            cols = st.columns(2)
            for idx, film in enumerate(data[:10]):
                with cols[idx % 2]:
                    display_ultra_minimal_card(film)
            log_telemetry("Texto", src, lat)

    elif search_type == "Género":
        genre_map = {g.name: g.value for g in GenreEnum}
        selected_label = col_input.selectbox(
            "Género", list(genre_map.values()), label_visibility="collapsed"
        )
        if col_btn.button("🔍 Buscar Género", use_container_width=True):
            sel_key = next(k for k, v in genre_map.items() if v == selected_label)
            stats, s_src, s_lat = get_stats_logic(sel_key)
            films, f_src, f_lat = get_films_logic(sel_key)
            display_performance_tag(s_src, s_lat)
            sc1, sc2, sc3, sc4 = st.columns(4)
            sc1.metric("Películas", stats["total_count"])
            sc2.metric("Nota Media", stats["avg_rating"])
            sc3.metric("Primer estreno", stats["oldest_year"])
            sc4.metric("Último Estreno", stats["newest_year"])
            st.divider()
            cols = st.columns(2)
            for idx, film in enumerate(films[:10]):
                with cols[idx % 2]:
                    display_ultra_minimal_card(film)
            log_telemetry("Stats", s_src, s_lat)
            log_telemetry("Lista Género", f_src, f_lat)

with tab_manage:
    t_create, t_update, t_delete = st.tabs(["Crear", "Actualizar", "Eliminar"])
    genre_map = {g.name: g.value for g in GenreEnum}

    with t_create:
        with st.form("form_create", border=False):
            c1, c2, c3 = st.columns([2, 1, 1])
            title = c1.text_input("Título")
            genre = c2.selectbox("Género", list(genre_map.values()))
            year = c3.number_input("Año", 1888, 2030, 2024)
            c4, c5 = st.columns([2, 1])
            director = c4.text_input("Director")
            rating = c5.slider("Puntuación", 0.0, 10.0, 5.0)
            synopsis = st.text_area("Sinopsis", height=80)
            if st.form_submit_button("➕ Crear Película", type="primary"):
                with engine.connect() as conn:
                    q = text(
                        "INSERT INTO films (title,genre,release_year,rating,director,synopsis) VALUES (:t,:g,:y,:r,:d,:s) RETURNING id"
                    )
                    new_id = conn.execute(
                        q,
                        {
                            "t": title,
                            "g": genre,
                            "y": year,
                            "r": rating,
                            "d": director,
                            "s": synopsis,
                        },
                    ).fetchone()[0]
                    conn.commit()
                invalidate_caches(old_genre=genre)
                st.success(f"Creado ID: {new_id}")

    with t_update:
        c_id, c_btn = st.columns([1, 4])
        u_id = c_id.number_input("ID Update", min_value=1, label_visibility="collapsed")
        if c_btn.button("📥 Cargar Datos"):
            data, _, _ = get_film_by_id_logic(u_id)
            if data:
                st.session_state.edit_film = data
            else:
                st.error("No existe.")
        if st.session_state.edit_film:
            f = st.session_state.edit_film
            with st.form("form_update", border=False):
                u_title = st.text_input("Título", value=f["title"])
                u_genre = st.selectbox(
                    "Género",
                    list(genre_map.values()),
                    index=list(genre_map.values()).index(f["genre"]),
                )
                if st.form_submit_button("💾 Guardar"):
                    with engine.connect() as conn:
                        conn.execute(
                            text("UPDATE films SET title=:t, genre=:g WHERE id=:id"),
                            {"t": u_title, "g": u_genre, "id": f["id"]},
                        )
                        conn.commit()
                    invalidate_caches(f["id"], f["genre"], u_genre)
                    st.success("Actualizado")

    with t_delete:
        d_id = st.number_input("ID Delete", min_value=1)
        if st.button("🗑️ Eliminar Película", type="primary"):
            confirm_delete_dialog(d_id)
        if st.session_state.execute_delete_for_id:
            del_id = st.session_state.execute_delete_for_id
            st.session_state.execute_delete_for_id = None
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT genre FROM films WHERE id=:id"), {"id": del_id}
                ).fetchone()
                if row:
                    conn.execute(text("DELETE FROM films WHERE id=:id"), {"id": del_id})
                    conn.commit()
                    invalidate_caches(del_id, row[0])
                    st.success(f"ID {del_id} borrado")

with tab_telemetry:
    st.info(
        "Protocolo de Evaluación Automatizado: 24 peticiones secuenciales (Cache-Aside)."
    )
    if st.button("▶️ Ejecutar Batería de Pruebas", type="primary"):
        progress = st.progress(0)
        tests = [
            ("ID 123", lambda: get_film_by_id_logic(123)),
            ("ID 123", lambda: get_film_by_id_logic(123)),
            ("ID 456", lambda: get_film_by_id_logic(456)),
            ("ID 456", lambda: get_film_by_id_logic(456)),
            ("Texto 'agua'", lambda: search_films_logic("agua")),
            ("Texto 'agua'", lambda: search_films_logic("agua")),
            ("Stats Drama", lambda: get_stats_logic("drama")),
            ("Stats Drama", lambda: get_stats_logic("drama")),
        ]
        for i, (name, func) in enumerate(tests):
            res, src, lat = func()
            log_telemetry(name, src, lat)
            progress.progress((i + 1) / len(tests))
            time.sleep(0.1)

    if st.session_state.telemetry:
        df = pd.DataFrame(st.session_state.telemetry)
        st.subheader("📊 Resultados de Rendimiento")
        summary = (
            df.groupby(["Tipo de Consulta", "Origen"])["Latencia (ms)"]
            .agg(["mean", "median", "min", "max"])
            .reset_index()
        )
        st.dataframe(summary, use_container_width=True)

        pivot = summary.pivot(
            index="Tipo de Consulta", columns="Origen", values="median"
        ).reset_index()
        if "PostgreSQL (DB)" in pivot.columns and "Redis (Caché)" in pivot.columns:
            pivot["Mejora (x)"] = (
                pivot["PostgreSQL (DB)"] / pivot["Redis (Caché)"]
            ).round(1)
            st.metric("Aceleración Media", f"{pivot['Mejora (x)'].median():.1f}x")
            st.plotly_chart(
                px.bar(
                    summary,
                    x="Tipo de Consulta",
                    y="mean",
                    color="Origen",
                    barmode="group",
                    title="Latencia Media (ms)",
                )
            )
