import streamlit as st
import pandas as pd
import sqlite3
import time

import sqlite3
import pandas as pd

conn = sqlite3.connect("buzz.sqlite")
df = pd.read_sql_query("SELECT Weekday, coffee_name, sales FROM coffee_sales", conn)
print(df.head())
print(df.groupby("Weekday")["sales"].sum())

st.set_page_config(page_title="Live Coffee Sales Dashboard", layout="wide")
st.title("☕ Live Coffee Sales Dashboard")

# --- Configuration ---
db_path = st.text_input("Path to SQLite DB:", "data/buzz.sqlite")
table_name = st.text_input("Table name:", "coffee_sales")
refresh_interval = st.slider("Refresh interval (seconds):", 1, 30, 5)

placeholder = st.empty()  # Container for live charts

# --- Main loop ---
while True:
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
    except Exception as e:
        st.error(f"Failed to read SQLite table: {e}")
        time.sleep(refresh_interval)
        continue

    if df.empty:
        st.warning("No sales data yet!")
        time.sleep(refresh_interval)
        continue

    with placeholder.container():
        st.subheader("Sales by Coffee Type")
        if "coffee_name" in df.columns and "sales" in df.columns:
            coffee_sales = df.groupby("coffee_name")["sales"].sum().sort_values(ascending=False)
            st.bar_chart(coffee_sales)
        else:
            st.write("Missing columns: 'coffee_name' and 'sales' required")

        st.subheader("Sales by Weekday")
        if "Weekday" in df.columns and "sales" in df.columns:
            weekday_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            weekday_sales = df.groupby("Weekday")["sales"].sum().reindex(weekday_order)
            st.bar_chart(weekday_sales)
        else:
            st.write("Missing columns: 'Weekday' and 'sales' required")

        st.subheader("Sales by Hour of Day")
        if "hour_of_day" in df.columns and "sales" in df.columns:
            hourly_sales = df.groupby("hour_of_day")["sales"].sum().sort_index()
            st.line_chart(hourly_sales)
        else:
            st.write("Missing columns: 'hour_of_day' and 'sales' required")

    time.sleep(refresh_interval)