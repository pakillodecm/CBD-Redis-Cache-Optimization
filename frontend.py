import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# --- CONFIG ---
BACKEND_URL = "http://app:8000"

LABEL_MAP = {
    "id": "ID",
    "title": "Título",
    "genre": "Género",
    "release_year": "Año",
    "rating": "Rating",
    "director": "Director",
    "synopsis": "Sinopsis",
}


# --- HELPERS ---
def get_genre_map():
    """Fetch available genres from backend."""
    try:
        response = requests.get(f"{BACKEND_URL}/genres")
        if response.status_code == 200:
            return response.json().get("data", {})
    except:
        pass
    return {}


def clear_redis_cache():
    """Clear all Redis cache."""
    try:
        response = requests.get(f"{BACKEND_URL}/clear-cache")
        return response.status_code == 200
    except:
        return False


def get_performance_tag(source: str):
    """Display performance badge based on source."""
    if "Redis" in source:
        st.success("🚀 Recuperado de Memoria (Instantáneo)")
    else:
        st.warning("⏳ Consultando Base de Datos (Procesamiento)")


def display_film(film: dict, latency_ms: str, source: str):
    """Display a film card with improved design."""
    is_cache = "Redis" in source
    badge_color = "✅" if is_cache else "⚙️"

    with st.container(border=True):
        # Title + Latency badge
        col_title, col_latency = st.columns([0.8, 0.2])
        with col_title:
            st.markdown(f"### {film['title']}")
        with col_latency:
            st.markdown(f"**{latency_ms} ms** {badge_color}")

        # Director, Year, Rating
        st.markdown(
            f"**{film['director']}** • {film['release_year']} • ⭐ {film['rating']}"
        )

        # Genre
        st.caption(f"📁 {film['genre']}")

        # Synopsis
        synopsis_text = film["synopsis"]
        if len(synopsis_text) > 180:
            synopsis_text = synopsis_text[:177] + "..."
        st.markdown(f"*{synopsis_text}*")


# --- PAGE SETUP ---
st.set_page_config(page_title="Film Database - Telemetry", layout="wide")
st.title("🎬 Film Database")
st.markdown("""
    **Advanced Search • Real-Time Telemetry • Cache Performance Visualization**
    
    Experience the power of Redis caching. Every query is tracked and compared to database performance.
""")
st.divider()

# --- SIDEBAR ---
with st.sidebar:
    st.header("☁️ Cache Management")
    st.markdown("Control and monitor Redis cache behavior.")

    if st.button("🧹 Clear All Cache", use_container_width=True, type="primary"):
        if clear_redis_cache():
            st.success("✅ Cache cleared successfully")
        else:
            st.error("❌ Failed to clear cache")

    st.divider()
    st.caption("**📊 About Latency**")
    st.caption(
        "Latency measures total time: backend processing + database/cache access."
    )

    st.divider()
    st.markdown("### 🚀 How it works")
    st.markdown("""
    - **First Query**: Database lookup (slow)
    - **Next Queries**: Cache hit (fast)
    - **Visual Proof**: See the difference in the chart
    """)


# Initialize session state for telemetry tracking
if "telemetry" not in st.session_state:
    st.session_state.telemetry = []

# Initialize session state for Update tab
if "update_film_data" not in st.session_state:
    st.session_state.update_film_data = None


# --- MAIN: UNIFIED SEARCH (READ) ---
st.header("🔍 Search Films")
st.markdown("Find films by ID, keyword, or genre. Watch how Redis caches results.")

search_mode = st.radio("Search by:", ["ID", "Text", "Genre"], horizontal=True)

if search_mode == "ID":
    film_id = st.number_input("Film ID", min_value=1, step=1, value=1)
    if st.button("Search by ID", use_container_width=True):
        try:
            response = requests.get(f"{BACKEND_URL}/films/{film_id}")
            if response.status_code == 200:
                latency = response.headers.get("X-Process-Time", "0")
                source = response.json()["source"]
                film = response.json()["data"]

                # Track telemetry
                is_cache = "Redis" in source
                st.session_state.telemetry.append(
                    {
                        "Query": len(st.session_state.telemetry) + 1,
                        "Source": "Cache" if is_cache else "Database",
                        "Latency": float(latency),
                    }
                )

                get_performance_tag(source)
                display_film(film, latency, source)
            else:
                st.error(f"❌ Film {film_id} not found")
        except Exception as e:
            st.error(f"❌ Connection error: {e}")

elif search_mode == "Text":
    search_query = st.text_input(
        "Search in title or synopsis", placeholder="e.g., space, love, mystery"
    )
    if st.button("Search by Text", use_container_width=True):
        if not search_query.strip():
            st.warning("Enter a search term")
        else:
            try:
                response = requests.get(
                    f"{BACKEND_URL}/films/search", params={"q": search_query}
                )
                if response.status_code == 200:
                    latency = response.headers.get("X-Process-Time", "0")
                    source = response.json()["source"]
                    films = response.json()["data"]

                    # Track telemetry
                    is_cache = "Redis" in source
                    st.session_state.telemetry.append(
                        {
                            "Query": len(st.session_state.telemetry) + 1,
                            "Source": "Cache" if is_cache else "Database",
                            "Latency": float(latency),
                        }
                    )

                    st.info(f"🔍 Found {len(films)} result(s) in {latency}ms")
                    get_performance_tag(source)

                    for film in films:
                        display_film(film, latency, source)
                else:
                    st.error("❌ Search failed")
            except Exception as e:
                st.error(f"❌ Connection error: {e}")

elif search_mode == "Genre":
    try:
        genre_map = get_genre_map()
        if genre_map:
            genre_labels = list(genre_map.values())
            selected_label = st.selectbox("Select genre", genre_labels)
            selected_key = next(k for k, v in genre_map.items() if v == selected_label)

            if st.button("Filter by Genre", use_container_width=True):
                try:
                    response = requests.get(
                        f"{BACKEND_URL}/films", params={"genre": selected_key}
                    )
                    if response.status_code == 200:
                        latency = response.headers.get("X-Process-Time", "0")
                        source = response.json()["source"]
                        films = response.json()["data"]

                        # Track telemetry
                        is_cache = "Redis" in source
                        st.session_state.telemetry.append(
                            {
                                "Query": len(st.session_state.telemetry) + 1,
                                "Source": "Cache" if is_cache else "Database",
                                "Latency": float(latency),
                            }
                        )

                        st.info(f"📽️ Found {len(films)} film(s) in {latency}ms")
                        get_performance_tag(source)

                        for film in films[:10]:  # Show first 10
                            display_film(film, latency, source)
                        if len(films) > 10:
                            st.caption(f"... y {len(films) - 10} películas más")
                    else:
                        st.error("❌ Genre search failed")
                except Exception as e:
                    st.error(f"❌ Connection error: {e}")
        else:
            st.error("❌ Could not load genres")
    except Exception as e:
        st.error(f"❌ Genre error: {e}")

st.divider()

# --- MAIN: PERFORMANCE VISUALIZATION ---
st.header("Cache Performance Evolution")

if len(st.session_state.telemetry) >= 2:
    df_telemetry = pd.DataFrame(st.session_state.telemetry)

    # Create line chart showing latency evolution
    fig = px.line(
        df_telemetry,
        x="Query",
        y="Latency",
        color="Source",
        markers=True,
        color_discrete_map={"Cache": "#00AA00", "Database": "#FF9900"},
        title="Latency Evolution Over Queries",
        labels={"Query": "Query Number", "Latency": "Latency (ms)"},
        height=350,
    )

    fig.update_traces(line=dict(width=2), marker=dict(size=8))
    fig.update_layout(hovermode="x unified", showlegend=True)

    st.plotly_chart(fig, use_container_width=True)

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Queries", len(st.session_state.telemetry))

    cache_data = df_telemetry[df_telemetry["Source"] == "Cache"]
    db_data = df_telemetry[df_telemetry["Source"] == "Database"]

    with col2:
        if len(cache_data) > 0:
            avg_cache = cache_data["Latency"].mean()
            st.metric("🚀 Avg Cache", f"{avg_cache:.2f}ms")
        else:
            st.metric("🚀 Avg Cache", "—")

    with col3:
        if len(db_data) > 0:
            avg_db = db_data["Latency"].mean()
            st.metric("⏳ Avg Database", f"{avg_db:.2f}ms")
        else:
            st.metric("⏳ Avg Database", "—")

    with col4:
        if len(cache_data) > 0 and len(db_data) > 0:
            speedup = db_data["Latency"].mean() / cache_data["Latency"].mean()
            st.metric("⚡ Speedup", f"{speedup:.1f}x")
        else:
            st.metric("⚡ Speedup", "—")
else:
    st.info("💡 Perform searches to visualize cache performance")

st.divider()

# --- EXPANDABLE: CREATE / UPDATE / DELETE ---
with st.expander("🎬 Manage Films (Create/Update/Delete)", expanded=False):
    tab1, tab2, tab3 = st.tabs(["Create", "Update", "Delete"])

    # CREATE
    with tab1:
        st.subheader("Create New Film")

        try:
            genre_map = get_genre_map()
            if genre_map:
                genre_labels = list(genre_map.values())
                genre_key_to_label = {v: k for k, v in genre_map.items()}

                # Compact form layout
                col1, col2 = st.columns(2)
                with col1:
                    title = st.text_input("Title")
                    genre_label = st.selectbox(
                        "Genre", genre_labels, key="create_genre"
                    )
                    director = st.text_input("Director")

                with col2:
                    year = st.number_input(
                        "Release Year", min_value=1888, max_value=2026, value=2025
                    )
                    rating = st.slider("Rating", 0.0, 10.0, 5.0)

                synopsis = st.text_area("Synopsis", height=80)

                if st.button("✨ Create Film", use_container_width=True):
                    if not all([title, director, synopsis]):
                        st.warning("📋 Fill all fields")
                    else:
                        try:
                            response = requests.post(
                                f"{BACKEND_URL}/films",
                                json={
                                    "title": title,
                                    "genre": genre_key_to_label[genre_label],
                                    "release_year": year,
                                    "rating": rating,
                                    "director": director,
                                    "synopsis": synopsis,
                                },
                            )
                            if response.status_code == 200:
                                film_id = response.json()["id"]
                                st.success(f"✅ Film created with ID {film_id}")
                                st.toast(
                                    "🗑️ Cache: Genre & stats invalidated",
                                    icon="⚡",
                                )
                            else:
                                st.error("❌ Creation failed")
                        except Exception as e:
                            st.error(f"❌ Error: {e}")
            else:
                st.error("❌ Could not load genres")
        except Exception as e:
            st.error(f"❌ Error: {e}")

    # UPDATE
    with tab2:
        st.subheader("Update Film")

        try:
            genre_map = get_genre_map()
            if genre_map:
                genre_labels = list(genre_map.values())
                genre_key_to_label = {v: k for k, v in genre_map.items()}

                col_id1, col_id2 = st.columns([0.7, 0.3])
                with col_id1:
                    update_id = st.number_input(
                        "Film ID to Update", min_value=1, step=1, key="update_id"
                    )

                with col_id2:
                    if st.button("🔍 Load", use_container_width=True):
                        try:
                            response = requests.get(f"{BACKEND_URL}/films/{update_id}")
                            if response.status_code == 200:
                                film_data = response.json()["data"]
                                st.session_state.update_film_data = film_data
                                st.success(f"✅ Loaded: {film_data['title']}")
                            else:
                                st.error(f"❌ Film {update_id} not found")
                        except Exception as e:
                            st.error(f"❌ Error: {e}")

                st.divider()

                # Get values from session state or use defaults
                current_data = st.session_state.update_film_data

                # Compact form layout
                col1, col2 = st.columns(2)
                with col1:
                    title = st.text_input(
                        "Title",
                        value=current_data["title"] if current_data else "",
                        key="update_title",
                    )
                    genre_current = (
                        current_data["genre"] if current_data else genre_labels[0]
                    )
                    genre_index = (
                        genre_labels.index(genre_current)
                        if genre_current in genre_labels
                        else 0
                    )
                    genre_label = st.selectbox(
                        "Genre", genre_labels, index=genre_index, key="update_genre"
                    )
                    director = st.text_input(
                        "Director",
                        value=current_data["director"] if current_data else "",
                        key="update_director",
                    )

                with col2:
                    year = st.number_input(
                        "Year",
                        min_value=1888,
                        max_value=2026,
                        value=current_data["release_year"] if current_data else 2025,
                        key="update_year",
                    )
                    rating = st.slider(
                        "Rating",
                        0.0,
                        10.0,
                        value=current_data["rating"] if current_data else 5.0,
                        key="update_rating",
                    )

                synopsis = st.text_area(
                    "Synopsis",
                    value=current_data["synopsis"] if current_data else "",
                    height=80,
                    key="update_synopsis",
                )

                if st.button("💾 Update Film", use_container_width=True):
                    if not all([title, director, synopsis]):
                        st.warning("📋 Fill all fields")
                    else:
                        try:
                            response = requests.put(
                                f"{BACKEND_URL}/films/{update_id}",
                                json={
                                    "title": title,
                                    "genre": genre_key_to_label[genre_label],
                                    "release_year": year,
                                    "rating": rating,
                                    "director": director,
                                    "synopsis": synopsis,
                                },
                            )
                            if response.status_code == 200:
                                genre_changed = response.json().get(
                                    "genre_changed", False
                                )
                                st.success("✅ Film updated")

                                if genre_changed:
                                    st.toast(
                                        "🗑️ Cache: Old & new genre invalidated",
                                        icon="⚡",
                                    )
                                else:
                                    st.toast("🗑️ Cache: Global invalidated", icon="⚡")

                                st.session_state.update_film_data = None
                            else:
                                st.error("❌ Update failed")
                        except Exception as e:
                            st.error(f"❌ Error: {e}")
            else:
                st.error("❌ Could not load genres")
        except Exception as e:
            st.error(f"❌ Error: {e}")

    # DELETE
    with tab3:
        st.subheader("Delete Film")

        col_del1, col_del2 = st.columns([0.7, 0.3])
        with col_del1:
            delete_id = st.number_input(
                "Film ID to Delete", min_value=1, step=1, key="delete_id"
            )

        with col_del2:
            if st.button("🗑️ Delete", use_container_width=True, type="secondary"):
                try:
                    response = requests.delete(f"{BACKEND_URL}/films/{delete_id}")
                    if response.status_code == 200:
                        st.success("✅ Film deleted")
                        st.toast("🗑️ Cache: All invalidated", icon="⚡")
                    else:
                        st.error("❌ Deletion failed")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

st.divider()

# --- EXPANDABLE: ADVANCED FEATURES ---
with st.expander("⚙️ Advanced Features", expanded=False):
    col_adv1, col_adv2 = st.columns(2)

    with col_adv1:
        st.subheader("📊 Aggregate Statistics")
        try:
            genre_map = get_genre_map()
            if genre_map:
                genre_labels = list(genre_map.values())
                selected_label = st.selectbox(
                    "Select genre", genre_labels, key="stats_genre"
                )
                selected_key = next(
                    k for k, v in genre_map.items() if v == selected_label
                )

                if st.button("📈 Load Stats", use_container_width=True):
                    try:
                        response = requests.get(
                            f"{BACKEND_URL}/films/stats", params={"genre": selected_key}
                        )
                        if response.status_code == 200:
                            latency = response.headers.get("X-Process-Time", "0")
                            source = response.json()["source"]
                            stats = response.json()["data"]

                            # Track telemetry
                            is_cache = "Redis" in source
                            st.session_state.telemetry.append(
                                {
                                    "Query": len(st.session_state.telemetry) + 1,
                                    "Source": "Cache" if is_cache else "Database",
                                    "Latency": float(latency),
                                }
                            )

                            get_performance_tag(source)

                            st.metric("Total Films", stats["total_count"])
                            st.metric("Avg Rating", f"{stats['avg_rating']} ⭐")
                            st.metric("Oldest Year", stats["oldest_year"])
                            st.metric("Newest Year", stats["newest_year"])
                        else:
                            st.error("❌ Failed to load stats")
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
            else:
                st.error("❌ Could not load genres")
        except Exception as e:
            st.error(f"❌ Error: {e}")

    with col_adv2:
        st.subheader("🔍 Technical Telemetry Log")
        if len(st.session_state.telemetry) > 0:
            df_display = pd.DataFrame(st.session_state.telemetry)
            st.dataframe(df_display, use_container_width=True, hide_index=True)

            # Export button
            csv = df_display.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name="telemetry_log.csv",
                mime="text/csv",
            )
        else:
            st.info("No telemetry data yet")

st.divider()

# --- FOOTER ---
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 20px;'>
        <small>🚀 Redis Cache-Aside Pattern Demonstration • FastAPI + PostgreSQL + Redis + Streamlit</small>
    </div>
""",
    unsafe_allow_html=True,
)
