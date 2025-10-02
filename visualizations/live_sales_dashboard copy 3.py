import streamlit as st
import pandas as pd
import sqlite3
import pathlib

# Config
db_path = pathlib.Path("/mnt/c/Projects/custom_pipeline_clean-1/data/buzz.sqlite")
table_name = "coffee_sales"

st.set_page_config(page_title="Live Coffee Sales Dashboard", layout="wide")
st.title("☕ Live Coffee Sales Dashboard")

# --- Try to read table ---
try:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
except Exception as e:
    st.error(f"Failed to read SQLite table '{table_name}': {e}")
    df = pd.DataFrame()  # empty dataframe to prevent further errors

# --- Display data if available ---
if not df.empty:
    st.subheader("First 10 rows")
    st.dataframe(df.head(10))

    # Sales by coffee
    if "coffee_name" in df.columns and "sales" in df.columns:
        coffee_sales = df.groupby("coffee_name")["sales"].sum().sort_values(ascending=False)
        st.subheader("Sales by Coffee Type")
        st.bar_chart(coffee_sales)

    # Sales by weekday
    if "Weekday" in df.columns and "sales" in df.columns:
        weekday_order = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
        weekday_sales = df.groupby("Weekday")["sales"].sum().reindex(weekday_order)
        st.subheader("Sales by Weekday")
        st.bar_chart(weekday_sales)
else:
    st.warning("No data available yet. Make sure the table exists in your database.")