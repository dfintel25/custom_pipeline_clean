import streamlit as st
import pandas as pd
import time
import sqlite3
import matplotlib.pyplot as plt

st.set_page_config(page_title="Live Sales Dashboard", layout="wide")

st.title("☕ Live Coffee Sales Dashboard")

# --- Choose data source ---
data_source = st.radio("Data Source:", ["CSV", "SQLite"])

if data_source == "CSV":
    file_path = st.text_input("Path to CSV file:", "data/coffee.csv")
else:
    db_path = st.text_input("Path to SQLite DB:", "data/buzz.sqlite")
    table_name = st.text_input("Table name:", "coffee_sales")

# Refresh interval
refresh_interval = st.slider("Refresh interval (seconds):", 1, 30, 5)

placeholder = st.empty()  # Container for live chart

while True:
    # --- Load data ---
    if data_source == "CSV":
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            st.error(f"Failed to read CSV: {e}")
            time.sleep(refresh_interval)
            continue
    else:
        try:
            conn = sqlite3.connect(db_path)
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            conn.close()
        except Exception as e:
            st.error(f"Failed to read SQLite table: {e}")
            time.sleep(refresh_interval)
            continue

    # --- Display charts ---
    with placeholder.container():
        st.subheader("Sales by Time of Day")
        if 'time' in df.columns and 'sales' in df.columns:
            st.line_chart(df.set_index('time')['sales'])
        else:
            st.write("CSV/DB must have columns: 'time' and 'sales'")

        st.subheader("Sales by Weekday")
        if 'weekday' in df.columns and 'sales' in df.columns:
            weekday_sales = df.groupby('weekday')['sales'].sum().reindex([
                "Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"
            ])
            st.bar_chart(weekday_sales)
        else:
            st.write("CSV/DB must have columns: 'weekday' and 'sales'")

    time.sleep(refresh_interval)