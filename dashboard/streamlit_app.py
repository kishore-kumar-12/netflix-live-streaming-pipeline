import streamlit as st
import pandas as pd
import numpy as np
import time
import os
import glob

st.set_page_config(
    page_title="Live Netflix Dashboard",
    page_icon="🔥",
    layout="wide"
)

# Folder where Spark writes parquet files
PARQUET_PATH = "/home/kishore_kumar_/netflix_live_project/output_parquet"

st.title("🔥 Netflix Live Dashboard — Real-Time Pipeline")
st.caption("Kafka → Spark Streaming → Parquet → Streamlit")

REFRESH_INTERVAL = 5  # seconds

@st.cache_data(ttl=3)
def load_latest_data():
    """Load all Parquet files from the folder."""
    parquet_files = glob.glob(os.path.join(PARQUET_PATH, "*.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    
    # Keep only the latest update per title
    df['last_updated'] = pd.to_datetime(df['last_updated'])
    df = df.sort_values('last_updated').drop_duplicates('id', keep='last')

    return df

# Auto-refresh
st.toast("Refreshing live data...", icon="🔄")
time.sleep(1)

df = load_latest_data()

if df.empty:
    st.warning("No data yet — waiting for Spark stream…")
    st.stop()

# Columns layout
col1, col2 = st.columns(2)

# 🔥 TOP TRENDING TITLES
with col1:
    st.subheader("🔥 Top Trending Titles (Live)")
    top_trending = df.sort_values("trending_score", ascending=False).head(10)
    st.dataframe(top_trending[['title', 'genre', 'current_viewers', 'trending_score', 'last_updated']])

# 👁️ MOST VIEWED RIGHT NOW
with col2:
    st.subheader("👁️ Highest Viewers (Right Now)")
    top_viewers = df.sort_values("current_viewers", ascending=False).head(10)
    st.dataframe(top_viewers[['title', 'current_viewers', 'total_views', 'last_updated']])

st.divider()

# 📈 Viewer Count Chart
st.subheader("📈 Live Viewer Count Distribution")
st.line_chart(df[['current_viewers']])

# 🎭 Genre Distribution
st.subheader("🎭 Genre Spread")
genre_counts = df['genre'].value_counts()
st.bar_chart(genre_counts)

st.info("Dashboard refreshes every 5 seconds. Leave it running to watch the pipeline in action.")

