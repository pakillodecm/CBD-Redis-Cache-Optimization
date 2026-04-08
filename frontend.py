import pandas as pd
import plotly.express as px
import requests
import streamlit as st

st.set_page_config(page_title="Dashboard CBD - Optimización", layout="wide")
st.title("🚀 Panel de Control de Rendimiento (Redis vs SQL)")
st.markdown("---")

with st.sidebar:
    st.header("⚙️ Ajustes")
    if st.button("🧹 Limpiar Caché de Redis"):
        requests.get("http://app:8000/clear-cache")
        st.success("Caché vaciada correctamente")

st.header("🔍 Búsqueda de Película por ID")
col1, col2 = st.columns([1, 2])

with col1:
    film_id = st.number_input("Introduce el ID de la película", min_value=1, value=100)
    if st.button("Consultar Película"):
        res = requests.get(f"http://app:8000/film/{film_id}").json()

        # Traducimos visualmente la fuente para el usuario
        fuente = (
            "Base de Datos (Lenta)"
            if "PostgreSQL" in res["source"]
            else "Memoria Redis (Rápida)"
        )

        st.metric("Latencia de respuesta", f"{res['latency_ms']} ms", delta=fuente)

        # Mapeamos las claves inglesas a etiquetas españolas para mostrar el JSON bonito
        datos_es = {
            "Título": res["data"]["title"],
            "Género": res["data"]["genre"],
            "Año": res["data"]["release_year"],
            "Puntuación": res["data"]["rating"],
            "Director": res["data"]["director"],
            "Sinopsis": res["data"]["synopsis"],
        }
        st.write("### Detalles del film")
        st.json(datos_es)

st.header("📊 Comparativa por Género")
genre = st.selectbox(
    "Selecciona un género para analizar",
    [
        "Acción",
        "Drama",
        "Comedia",
        "Ciencia Ficción",
        "Terror",
        "Documental",
        "Suspense",
        "Aventura",
    ],
)

if st.button("Cargar Datos del Género"):
    res = requests.get(f"http://app:8000/films/genre/{genre}").json()
    st.write(f"**Origen de los datos:** {res['source']}")

    # Preparamos el DataFrame con nombres de columna en español
    df = pd.DataFrame(res["data"])
    df.columns = [
        "ID",
        "Título",
        "Género",
        "Año de estreno",
        "Puntuación",
        "Director",
        "Sinopsis",
    ]
    st.dataframe(df.head(10), use_container_width=True)

    if "performance_history" not in st.session_state:
        st.session_state.performance_history = []

    st.session_state.performance_history.append(
        {
            "Origen": res["source"],
            "Latencia (ms)": res["latency_ms"],
            "Consulta Nº": len(st.session_state.performance_history) + 1,
        }
    )

    fig = px.bar(
        pd.DataFrame(st.session_state.performance_history),
        x="Consulta Nº",
        y="Latencia (ms)",
        color="Origen",
        title="Comparativa de Latencia en Tiempo Real",
        labels={
            "Latencia (ms)": "Milisegundos (ms)",
            "Consulta Nº": "Número de Petición",
        },
    )
    st.plotly_chart(fig, use_container_width=True)
