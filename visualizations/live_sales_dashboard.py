import streamlit as st
import pandas as pd
import sqlite3
import pathlib
import seaborn as sns
import matplotlib.pyplot as plt
from streamlit_autorefresh import st_autorefresh

# --- Configuration ---
st.set_page_config(page_title="Live Coffee Sales Dashboard", layout="wide")
st.title("☕ Live Coffee Sales Dashboard")

# Database and table
db_path = pathlib.Path("/mnt/c/Projects/custom_pipeline_clean-1/data/buzz.sqlite")
table_name = "coffee_sales"

# Refresh interval in seconds
refresh_interval = st.slider("Refresh interval (seconds):", 5, 30, 10)

# Auto-refresh
st_autorefresh(interval=refresh_interval * 1000, key="datarefresh")

placeholder = st.empty()  # Container for charts

# --- Function to load data safely ---
def load_data():
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to read SQLite table '{table_name}': {e}")
        return pd.DataFrame()  # empty DF to avoid errors

# --- Load data ---
df = load_data()

if df.empty:
    st.warning("No data available yet. Make sure the table exists in your database.")
else:
    # --- Preprocess ---
    df['sales'] = pd.to_numeric(df['sales'], errors='coerce').fillna(0)
    
    # Create 'hour_of_day' if missing but 'timestamp' exists
    if 'hour_of_day' not in df.columns and 'timestamp' in df.columns:
        df['hour_of_day'] = pd.to_datetime(df['timestamp']).dt.hour

    # --- Display raw data ---
    st.subheader("First 10 rows")
    st.dataframe(df.head(10))
    
    # --- Sales by Coffee Type ---
    if 'coffee_name' in df.columns:
        st.subheader("Sales by Coffee Type")
        coffee_sales = df.groupby("coffee_name")["sales"].sum().sort_values(ascending=False)
        st.bar_chart(coffee_sales)

        st.subheader("Top 5 Coffee Ranking")
        st.bar_chart(coffee_sales.head(5))  # Top 5 coffees

        st.subheader("Percentage Contribution of Each Coffee")
        if 'timestamp' in df.columns:
            stacked = df.pivot_table(index='timestamp', columns='coffee_name', values='sales', aggfunc='sum')
            st.area_chart(stacked.fillna(0))
    
    # --- Sales by Weekday ---
    if 'Weekday' in df.columns:
        st.subheader("Sales by Weekday")
        weekday_order = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
        weekday_sales = df.groupby("Weekday")["sales"].sum().reindex(weekday_order)
        st.bar_chart(weekday_sales)
    
    # --- Heatmap of Hour × Weekday ---
    if 'hour_of_day' in df.columns and 'Weekday' in df.columns:
        st.subheader("Hourly Sales Heatmap by Weekday")
        pivot = df.pivot_table(index='hour_of_day', columns='Weekday', values='sales', aggfunc='sum')
        pivot = pivot[["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]]
        fig, ax = plt.subplots(figsize=(10,6))
        sns.heatmap(pivot.fillna(0), cmap="YlOrRd", linewidths=0.5, ax=ax)
        ax.set_xlabel("Weekday")
        ax.set_ylabel("Hour of Day")
        st.pyplot(fig)