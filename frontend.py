import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# --- SETTINGS ---
st.set_page_config(page_title="Dashboard CBD - Optimización", layout="wide")
BACKEND_URL = "http://app:8000"

# Helper function to map technical keys to Spanish labels
# This keeps our "internals" in English and "soul" in Spanish
LABEL_MAP = {
    "id": "ID",
    "title": "Título",
    "genre": "Género",
    "release_year": "Año de Estreno",
    "rating": "Puntuación",
    "director": "Director",
    "synopsis": "Sinopsis",
}


def clear_redis_cache():
    """Call backend to flush all Redis keys."""
    try:
        response = requests.get(f"{BACKEND_URL}/clear-cache")
        if response.status_code == 200:
            st.sidebar.success("✅ Caché vaciada correctamente")
        else:
            st.sidebar.error("❌ Error al vaciar la caché")
    except Exception as e:
        st.sidebar.error(f"⚠️ Error de conexión: {e}")


@st.cache_data(show_spinner=False)
def get_genre_map():
    response = requests.get(f"{BACKEND_URL}/genres")
    if response.status_code != 200:
        raise RuntimeError("No se pudo cargar la lista de géneros")
    data = response.json().get("data", {})
    if not isinstance(data, dict) or not data:
        raise RuntimeError("Formato de géneros inválido")
    return data


# --- UI HEADER ---
st.title("🚀 Panel de Control de Rendimiento (Redis vs SQL)")
st.markdown("""
Esta herramienta permite visualizar la diferencia de rendimiento al aplicar el patrón **Cache-Aside**.
Las consultas a PostgreSQL (Miss) calculan el dato, mientras que Redis (Hit) entrega el resultado instantáneamente.
""")
st.markdown("---")

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Ajustes de Sistema")
    if st.button("🧹 Limpiar Caché de Redis"):
        clear_redis_cache()

    st.markdown("---")
    st.info(
        "**Nota:** Los tiempos de latencia reflejan el procesamiento del Backend + acceso a datos."
    )

# --- SECTION 1: SEARCH BY ID ---
st.header("🔍 Consulta Individual (Búsqueda por ID)")
col_id_1, col_id_2 = st.columns([1, 2])

with col_id_1:
    film_id = st.number_input(
        "Introduce el ID de la película", min_value=1, step=1, value=100
    )
    if st.button("Consultar Película"):
        try:
            # Plural endpoint as discussed: /films/{id}
            response = requests.get(f"{BACKEND_URL}/films/{film_id}")
            if response.status_code == 200:
                res = response.json()

                # Visual metrics in Spanish
                source_label = (
                    "Base de Datos"
                    if "PostgreSQL" in res["source"]
                    else "Memoria Caché"
                )
                st.metric(
                    "Latencia",
                    f"{res['latency_ms']} ms",
                    delta=source_label,
                    delta_color="inverse",
                )

                # Transform English keys to Spanish for display
                film_data = res["data"]
                spanish_details = {
                    LABEL_MAP[k]: v for k, v in film_data.items() if k in LABEL_MAP
                }

                st.write("### Detalles del Film")
                st.json(spanish_details)
            else:
                st.error(f"❌ Película no encontrada (ID: {film_id})")
        except Exception as e:
            st.error(f"⚠️ Error de conexión con el servidor: {e}")

# --- SECTION 2: SEARCH BY GENRE ---
st.header("📊 Análisis por Género")
try:
    genre_key_to_label = get_genre_map()
    genre_labels = list(genre_key_to_label.values())
    selected_genre_label = st.selectbox(
        "Selecciona un género para filtrar la colección",
        genre_labels,
    )
    selected_genre_key = next(
        key
        for key, value in genre_key_to_label.items()
        if value == selected_genre_label
    )
except Exception as e:
    st.error(f"⚠️ Error cargando géneros desde el backend: {e}")
    st.stop()

if st.button("Cargar Catálogo"):
    try:
        # Using Query Parameters as discussed: /films?genre=...
        params = {"genre": selected_genre_key}
        response = requests.get(f"{BACKEND_URL}/films", params=params)

        if response.status_code == 200:
            res = response.json()
            st.info(f"**Origen de los datos:** {res['source']}")

            # Prepare DataFrame for visualization
            if res["data"]:
                # --- NEW SECTION: HEAVY ANALYTICS ---
                st.subheader(f"📈 Resumen Estadístico: {selected_genre_label}")

                try:
                    stats_res = requests.get(
                        f"{BACKEND_URL}/films/stats", params=params
                    ).json()
                    stats_data = stats_res["data"]

                    # Create 4 cards for the metrics
                    s_col1, s_col2, s_col3, s_col4 = st.columns(4)

                    with s_col1:
                        st.metric("Total Películas", f"{stats_data['total_count']:,}")
                    with s_col2:
                        st.metric("Puntuación Media", f"{stats_data['avg_rating']} ⭐")
                    with s_col3:
                        st.metric("Año más antiguo", stats_data["oldest_year"])
                    with s_col4:
                        st.metric("Último estreno", stats_data["newest_year"])

                    st.caption(
                        f"Latencia de cálculo: {stats_res['latency_ms']} ms ({stats_res['source']})"
                    )
                    st.markdown("---")
                except Exception as e:
                    st.warning(f"No se pudieron cargar las estadísticas: {e}")

                df = pd.DataFrame(res["data"])
                # Rename columns for the user
                df_display = df.rename(columns=LABEL_MAP)

                st.dataframe(df_display.head(10), use_container_width=True)

                # Performance tracking (Session State)
                if "history" not in st.session_state:
                    st.session_state.history = []

                st.session_state.history.append(
                    {
                        "Consulta": len(st.session_state.history) + 1,
                        "Latencia (ms)": res["latency_ms"],
                        "Origen": "PostgreSQL (Miss)"
                        if "PostgreSQL" in res["source"]
                        else "Redis (Hit)",
                    }
                )

                # Latency Chart
                fig = px.line(
                    pd.DataFrame(st.session_state.history),
                    x="Consulta",
                    y="Latencia (ms)",
                    color="Origen",
                    title="Evolución de Latencia por Consulta",
                    markers=True,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para este género.")
        else:
            st.error("Error al obtener datos del género.")
    except Exception as e:
        st.error(f"⚠️ Error de comunicación: {e}")
