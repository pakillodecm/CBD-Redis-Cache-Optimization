import pandas as pd
import plotly.express as px
import requests
import streamlit as st

st.set_page_config(page_title="Dashboard CBD - Optimización Redis", layout="wide")

st.title("🚀 Panel de Control de Rendimiento (Redis vs SQL)")
st.markdown("---")

# Sidebar para acciones globales
with st.sidebar:
    st.header("⚙️ Configuración")
    if st.button("🧹 Vaciar Caché (Redis)"):
        requests.get("http://app:8000/clear-cache")
        st.success("Caché vaciada")

# --- SECCIÓN 1: BÚSQUEDA POR ID ---
st.header("🔍 Consulta Individual (Cache-Aside)")
col1, col2 = st.columns([1, 2])

with col1:
    prod_id = st.number_input("Introduce ID del Producto", min_value=1, value=100)
    if st.button("Buscar Producto"):
        res = requests.get(f"http://app:8000/producto/{prod_id}").json()

        st.metric(
            "Latencia",
            f"{res['latency_ms']} ms",
            delta=f"{res['source']}",
            delta_color="normal",
        )
        st.json(res["data"])

# --- SECCIÓN 2: BÚSQUEDA POR CATEGORÍA ---
st.header("📊 Consulta por Categoría (Benchmarking)")
categoria = st.selectbox(
    "Selecciona Categoría",
    ["Electrónica", "Hogar", "Jardín", "Libros", "Ropa", "Deportes"],
)

if st.button("Cargar Categoría"):
    # Hacemos la petición a la API
    res = requests.get(f"http://app:8000/productos/categoria/{categoria}").json()

    st.write(f"**Fuente de los datos:** {res['source']}")

    # Mostrar tabla de resultados
    df = pd.DataFrame(res["data"])
    st.dataframe(df.head(10), use_container_width=True)

    # Lógica de la gráfica (esto es lo que va a la memoria)
    # Guardamos los tiempos en el estado de la sesión para comparar
    if "historico_tiempos" not in st.session_state:
        st.session_state.historico_tiempos = []

    st.session_state.historico_tiempos.append(
        {
            "Fuente": res["source"],
            "Latencia (ms)": res["latency_ms"],
            "Evento": len(st.session_state.historico_tiempos) + 1,
        }
    )

    fig = px.bar(
        pd.DataFrame(st.session_state.historico_tiempos),
        x="Evento",
        y="Latencia (ms)",
        color="Fuente",
        title="Comparativa de Latencia en Tiempo Real",
    )
    st.plotly_chart(fig, use_container_width=True)
