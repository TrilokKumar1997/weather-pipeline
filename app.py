import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

st.set_page_config(
    page_title="Global Weather Pipeline",
    layout="wide",
    page_icon="🌤️"
)

@st.cache_resource
def get_engine():
    url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(url)

@st.cache_data(ttl=300)
def load_latest():
    engine = get_engine()
    query = """
        SELECT DISTINCT ON (city)
            city, temperature, feels_like,
            humidity, pressure, wind_speed,
            weather_condition, timestamp
        FROM weather_data
        ORDER BY city, timestamp DESC
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=300)
def load_history():
    engine = get_engine()
    query = """
        SELECT city, temperature, humidity,
               wind_speed, timestamp
        FROM weather_data
        ORDER BY timestamp DESC
        LIMIT 1000
    """
    return pd.read_sql(query, engine)

st.title("🌤️ Global Weather Pipeline")
st.caption(f"Real-time weather data for 15 cities · Updated every hour · Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
st.divider()

try:
    latest = load_latest()
    history = load_history()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Cities tracked",   f"{len(latest)}")
    col2.metric("Avg temperature",  f"{latest['temperature'].mean():.1f}°C")
    col3.metric("Avg humidity",     f"{latest['humidity'].mean():.0f}%")
    col4.metric("Total records",    f"{len(history):,}")
    st.divider()

    st.subheader("Current temperature by city")
    fig_bar = px.bar(
        latest.sort_values("temperature", ascending=True),
        x="temperature",
        y="city",
        orientation="h",
        color="temperature",
        color_continuous_scale="RdYlBu_r",
        text="temperature",
        labels={"temperature": "Temperature (°C)", "city": ""}
    )
    fig_bar.update_traces(texttemplate="%{text:.1f}°C", textposition="outside")
    fig_bar.update_layout(margin=dict(l=0, r=0))
    st.plotly_chart(fig_bar, use_container_width=True)
    st.divider()

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Temperature vs humidity")
        fig_sc = px.scatter(
            latest,
            x="temperature",
            y="humidity",
            size="wind_speed",
            color="weather_condition",
            hover_name="city",
            labels={
                "temperature": "Temperature (°C)",
                "humidity": "Humidity (%)",
                "wind_speed": "Wind speed"
            }
        )
        st.plotly_chart(fig_sc, use_container_width=True)

    with col_b:
        st.subheader("Weather conditions")
        condition_counts = latest["weather_condition"].value_counts().reset_index()
        condition_counts.columns = ["condition", "count"]
        fig_pie = px.pie(
            condition_counts,
            values="count",
            names="condition"
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    st.divider()
    st.subheader("Temperature trends over time")
    cities = st.multiselect(
        "Select cities",
        sorted(history["city"].unique()),
        default=sorted(history["city"].unique())[:5]
    )
    if cities:
        hist_filtered = history[history["city"].isin(cities)]
        fig_line = px.line(
            hist_filtered,
            x="timestamp",
            y="temperature",
            color="city",
            labels={"temperature": "Temperature (°C)", "timestamp": "Time"}
        )
        st.plotly_chart(fig_line, use_container_width=True)

    st.divider()
    st.subheader("Current conditions — all cities")
    st.dataframe(
        latest[[
            "city", "temperature", "feels_like",
            "humidity", "wind_speed", "weather_condition",
            "timestamp"
        ]].sort_values("temperature", ascending=False),
        use_container_width=True
    )

except Exception as e:
    st.error(f"Database connection error: {e}")
    st.info("Make sure PostgreSQL is running and the pipeline has been executed at least once.")

st.divider()
st.caption("Built by Trilok Kumar · Data Science MS, University of New Haven · Stack: Python · PostgreSQL · Streamlit · Open-Meteo API")
