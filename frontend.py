import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# --- CONFIG ---
st.set_page_config(page_title="CBD Panel - Optimización", layout="wide")
BACKEND_URL = "http://app:8000"

# --- UTILITIES & STATE ---
if "telemetry" not in st.session_state:
    st.session_state.telemetry = []
if "edit_film" not in st.session_state:
    st.session_state.edit_film = None
if "execute_delete_for_id" not in st.session_state:
    st.session_state.execute_delete_for_id = None


@st.cache_data(show_spinner=False)
def get_genre_map():
    try:
        response = requests.get(f"{BACKEND_URL}/genres")
        if response.status_code == 200:
            return response.json().get("data", {})
    except Exception:
        return {}


def log_telemetry(query_type: str, source: str, latency: float):
    source_clean = "Redis (Caché)" if "Redis" in source else "PostgreSQL (DB)"
    st.session_state.telemetry.append(
        {
            "Tipo de Consulta": query_type,
            "Origen": source_clean,
            "Latencia (ms)": float(latency),
        }
    )


# --- UX/UI ---
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
            res = requests.get(f"{BACKEND_URL}/clear-cache")
            if res.status_code == 200:
                st.success("Caché limpia.")
            else:
                st.error("Error al limpiar caché.")
        except Exception:
            st.error(f"Error del Backend ({res.status_code}): {res.text}")
    if st.button("🗑️ Limpiar Telemetría", use_container_width=True):
        st.session_state.telemetry = []
        st.success("Historial de telemetría reestablecido.")

# --- MAIN TABS ---
tab_explore, tab_manage, tab_telemetry = st.tabs(
    ["🔍 Explorar", "🛠️ Gestión (CUD)", "📈 Telemetría"]
)

# ==========================================
# TAB 1: EXPLORE (QUERYING)
# ==========================================
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
            res = requests.get(f"{BACKEND_URL}/films/{f_id}")
            if res.status_code == 200:
                data = res.json()
                lat = res.headers.get("X-Process-Time", "0")
                display_performance_tag(data["source"], lat)
                display_detailed_card(data["data"], data["source"], lat)
                log_telemetry("ID", data["source"], lat)
            else:
                st.error("No existe ninguna película con ese ID.")

    elif search_type == "Texto":
        q = col_input.text_input(
            "Buscar...",
            label_visibility="collapsed",
            placeholder="Ej.: agua, aire, mar...",
        )
        if col_btn.button("🔍 Buscar Texto", use_container_width=True):
            res = requests.get(f"{BACKEND_URL}/films/search", params={"q": q})
            if res.status_code == 200:
                data = res.json()
                lat = res.headers.get("X-Process-Time", "0")
                display_performance_tag(data["source"], lat)

                if q.strip():
                    st.markdown(
                        f"#### Coincidencias de '{q}' en título, director o sinopsis: {len(data['data'])}"
                    )
                else:
                    st.markdown(f"#### Películas disponibles: {len(data['data'])}")

                films_list = data["data"][:10]
                cols = st.columns(2)
                for idx, film in enumerate(films_list):
                    with cols[idx % 2]:
                        display_ultra_minimal_card(film)

                if len(data["data"]) > 10:
                    st.caption(f"👀 *... y {len(data['data']) - 10:,} películas más.*")

                log_telemetry("Texto", data["source"], lat)
            else:
                st.error(f"Error del Backend ({res.status_code}): {res.text}")

    elif search_type == "Género":
        genre_map = get_genre_map()
        selected_label = col_input.selectbox(
            "Género",
            list(genre_map.values()) if genre_map else [],
            label_visibility="collapsed",
        )
        if col_btn.button("🔍 Buscar Género", use_container_width=True) and genre_map:
            selected_key = next(k for k, v in genre_map.items() if v == selected_label)

            res_films = requests.get(
                f"{BACKEND_URL}/films", params={"genre": selected_key}
            )
            res_stats = requests.get(
                f"{BACKEND_URL}/films/stats", params={"genre": selected_key}
            )

            if res_stats.status_code == 200 and res_films.status_code == 200:
                s_data = res_stats.json()
                s_lat = res_stats.headers.get("X-Process-Time", "0")

                f_data = res_films.json()
                f_lat = res_films.headers.get("X-Process-Time", "0")

                display_performance_tag(s_data["source"], s_lat)

                sc1, sc2, sc3, sc4 = st.columns(4)
                total_films = s_data["data"]["total_count"]
                sc1.metric("Películas", total_films)
                sc2.metric("Nota Media", s_data["data"]["avg_rating"])
                sc3.metric("Primer estreno", s_data["data"]["oldest_year"])
                sc4.metric("Último Estreno", s_data["data"]["newest_year"])

                st.divider()
                st.markdown(f"#### Muestra del catálogo: {selected_label}")

                films_list = f_data["data"]
                cols = st.columns(2)
                for idx, film in enumerate(films_list[:10]):
                    with cols[idx % 2]:
                        display_ultra_minimal_card(film)

                if total_films > 10:
                    st.caption(f"👀 *... y {total_films - 10:,} películas más.*")

                log_telemetry("Stats", s_data["source"], s_lat)
                log_telemetry("Lista Género", f_data["source"], f_lat)
            else:
                st.error(f"Error del Backend ({res.status_code}): {res.text}")

# ==========================================
# TAB 2: MANAGE (CUD FORMS)
# ==========================================
with tab_manage:
    st.caption(
        "Cualquier escritura aquí invalidará automáticamente las cachés globales en Redis."
    )
    t_create, t_update, t_delete = st.tabs(["Crear", "Actualizar", "Eliminar"])

    # --- CREATE ---
    with t_create:
        with st.form("form_create", border=False):
            genre_map = get_genre_map()
            c1, c2, c3 = st.columns([2, 1, 1])
            c_title = c1.text_input("Título")
            c_genre = c2.selectbox(
                "Género", list(genre_map.values()) if genre_map else []
            )
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
                    res = requests.post(f"{BACKEND_URL}/films", json=payload)
                    if res.status_code == 200:
                        st.toast("✅ Película Creada. Caché invalidada.")
                        st.success(f"ID Creado: {res.json().get('id')}")
                    else:
                        st.error(f"Error del Backend ({res.status_code}): {res.text}")
                else:
                    st.warning(
                        "Por favor, completa todos los campos para crear la película."
                    )

    # --- UPDATE ---
    with t_update:
        c_id, c_btn = st.columns([1, 4])
        u_id = c_id.number_input(
            "ID", min_value=1, step=1, label_visibility="collapsed"
        )
        if c_btn.button("📥 Cargar Datos"):
            res = requests.get(f"{BACKEND_URL}/films/{u_id}")
            if res.status_code == 200:
                st.session_state.edit_film = res.json()["data"]
            else:
                st.error("No existe ninguna película con ese ID.")

        if st.session_state.edit_film:
            f_edit = st.session_state.edit_film
            with st.form("form_update", border=False):
                genre_map = get_genre_map()
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
                    res = requests.put(
                        f"{BACKEND_URL}/films/{f_edit['id']}", json=payload
                    )
                    if res.status_code == 200:
                        st.toast("♻️ Película actualizada. Caché invalidada.")
                        st.success(f"ID Actualizado: {u_id}")
                        st.session_state.edit_film.update(payload)
                    else:
                        st.error(f"Error del Backend ({res.status_code}): {res.text}")

    # --- DELETE ---
    with t_delete:
        c_id, c_btn = st.columns([1, 4])
        d_id = c_id.number_input(
            "ID", min_value=1, step=1, key="del_id", label_visibility="collapsed"
        )
        if c_btn.button("🗑️ Eliminar Película", type="primary"):
            res_check = requests.get(f"{BACKEND_URL}/films/{d_id}")
            if res_check.status_code == 200:
                confirm_delete_dialog(d_id)
            else:
                st.error("No existe ninguna película con ese ID.")

        if st.session_state.execute_delete_for_id:
            del_id = st.session_state.execute_delete_for_id
            st.session_state.execute_delete_for_id = None

            res = requests.delete(f"{BACKEND_URL}/films/{del_id}")
            if res.status_code == 200:
                st.toast("🗑️ Película eliminada. Caché invalidada.")
                st.success(f"ID Eliminado: {del_id}")
            else:
                st.error(f"Error del Backend ({res.status_code}): {res.text}")

# ==========================================
# TAB 3: TELEMETRY (PERFORMANCE ANALYSIS)
# ==========================================
with tab_telemetry:
    st.info("""
    **Protocolo de Evaluación Automatizado:** Se ejecutará una ráfaga secuencial de 24 peticiones. 
    El objetivo es forzar 'Cache Misses' (consulta a PostgreSQL) seguidos inmediatamente de 
    'Cache Hits' (Redis) para la misma consulta, demostrando la caída radical de latencia.
    """)

    battery = [
        ("Búsqueda por ID", "/films/123", "PostgreSQL"),
        ("Búsqueda por ID", "/films/123", "Redis"),
        ("Búsqueda por ID", "/films/456", "PostgreSQL"),
        ("Búsqueda por ID", "/films/456", "Redis"),
        ("Búsqueda por ID", "/films/789", "PostgreSQL"),
        ("Búsqueda por ID", "/films/789", "Redis"),
        ("Búsqueda por Texto", "/films/search?q=agua", "PostgreSQL"),
        ("Búsqueda por Texto", "/films/search?q=agua", "Redis"),
        ("Búsqueda por Texto", "/films/search?q=aire", "PostgreSQL"),
        ("Búsqueda por Texto", "/films/search?q=aire", "Redis"),
        ("Búsqueda por Texto", "/films/search?q=mar", "PostgreSQL"),
        ("Búsqueda por Texto", "/films/search?q=mar", "Redis"),
        ("Búsqueda por Género", "/films?genre=drama", "PostgreSQL"),
        ("Búsqueda por Género", "/films?genre=drama", "Redis"),
        ("Búsqueda por Género", "/films?genre=horror", "PostgreSQL"),
        ("Búsqueda por Género", "/films?genre=horror", "Redis"),
        ("Búsqueda por Género", "/films?genre=adventure", "PostgreSQL"),
        ("Búsqueda por Género", "/films?genre=adventure", "Redis"),
        ("Cálculo de Stats por Género", "/films/stats?genre=drama", "PostgreSQL"),
        ("Cálculo de Stats por Género", "/films/stats?genre=drama", "Redis"),
        ("Cálculo de Stats por Género", "/films/stats?genre=horror", "PostgreSQL"),
        ("Cálculo de Stats por Género", "/films/stats?genre=horror", "Redis"),
        ("Cálculo de Stats por Género", "/films/stats?genre=adventure", "PostgreSQL"),
        ("Cálculo de Stats por Género", "/films/stats?genre=adventure", "Redis"),
    ]

    with st.expander("📄 Detalle de las peticiones a ejecutar", expanded=False):
        df_battery = pd.DataFrame(
            battery, columns=["Tipo de Petición", "Endpoint", "Origen Esperado"]
        )
        st.table(df_battery)

    if st.button("▶️ Ejecutar Batería de Pruebas Automatizada", type="primary"):
        urls = [
            f"{BACKEND_URL}/films/123",
            f"{BACKEND_URL}/films/123",
            f"{BACKEND_URL}/films/456",
            f"{BACKEND_URL}/films/456",
            f"{BACKEND_URL}/films/789",
            f"{BACKEND_URL}/films/789",
            f"{BACKEND_URL}/films/search?q=agua",
            f"{BACKEND_URL}/films/search?q=agua",
            f"{BACKEND_URL}/films/search?q=aire",
            f"{BACKEND_URL}/films/search?q=aire",
            f"{BACKEND_URL}/films/search?q=mar",
            f"{BACKEND_URL}/films/search?q=mar",
            f"{BACKEND_URL}/films?genre=drama",
            f"{BACKEND_URL}/films?genre=drama",
            f"{BACKEND_URL}/films?genre=horror",
            f"{BACKEND_URL}/films?genre=horror",
            f"{BACKEND_URL}/films?genre=adventure",
            f"{BACKEND_URL}/films?genre=adventure",
            f"{BACKEND_URL}/films/stats?genre=drama",
            f"{BACKEND_URL}/films/stats?genre=drama",
            f"{BACKEND_URL}/films/stats?genre=horror",
            f"{BACKEND_URL}/films/stats?genre=horror",
            f"{BACKEND_URL}/films/stats?genre=adventure",
            f"{BACKEND_URL}/films/stats?genre=adventure",
        ]

        progress = st.progress(0)
        import time

        for i, url in enumerate(urls):
            res = requests.get(url)
            if res.status_code == 200:
                data = res.json()
                lat = res.headers.get("X-Process-Time", "0")
                log_telemetry(
                    battery[i][0], data.get("source", "PostgreSQL (DB)"), float(lat)
                )
            else:
                st.error(
                    f"❌ Error {res.status_code} en la petición a {url}: {res.text}"
                )
            time.sleep(0.3)
            progress.progress((i + 1) / len(urls))

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
        stast_summary["Media (ms)"] = stast_summary["Media (ms)"].round(3)
        stast_summary["Mediana (ms)"] = stast_summary["Mediana (ms)"].round(3)
        stast_summary["T. Mínimo (ms)"] = stast_summary["T. Mínimo (ms)"].round(3)
        stast_summary["T. Máximo (ms)"] = stast_summary["T. Máximo (ms)"].round(3)

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

            pure_multipliers = pivot_df["PostgreSQL (DB)"] / pivot_df["Redis (Caché)"]
            median_arq_upgrade = pure_multipliers.median()

            if pd.notna(median_arq_upgrade) and median_arq_upgrade > 0:
                st.info(
                    f"🏆 **Capacidad de Arquitectura:** En promedio (mediana), la caché acelera una operación típica en **{median_arq_upgrade:.1f}x**."
                )
        else:
            st.caption("Faltan datos de BD o Caché para calcular los multiplicadores.")

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
                "Líneas de conexión directas entre Las consultas a la BD y a la Caché."
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
