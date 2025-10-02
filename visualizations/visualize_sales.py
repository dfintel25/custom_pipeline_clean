import sqlite3
import pathlib
import matplotlib.pyplot as plt
import pandas as pd

# --- Project Paths ---
project_root = pathlib.Path(__file__).resolve().parent.parent
buzz_sqlite = project_root / "data" / "buzz.sqlite"
table_name = "coffee_sales"

print(f"Looking for database at: {buzz_sqlite}")

# --- Check that the table exists ---
with sqlite3.connect(buzz_sqlite) as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("Tables in DB:", tables)

# --- Coffee by Weekday Bar Chart ---
def plot_coffee_by_weekday(db_path: pathlib.Path, table_name: str = "coffee_sales"):
    with sqlite3.connect(db_path) as conn:
        query = f"""
        SELECT coffee_name, Weekday, COUNT(*) as count
        FROM {table_name}
        GROUP BY coffee_name, Weekday
        ORDER BY Weekday;
        """
        df = pd.read_sql_query(query, conn)

    if df.empty:
        print("No data available to plot coffee by weekday.")
        return

    # Ensure proper weekday order
    weekday_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    df['Weekday'] = pd.Categorical(df['Weekday'], categories=weekday_order, ordered=True)

    # Pivot for grouped bar chart
    pivot_df = df.pivot(index="Weekday", columns="coffee_name", values="count").fillna(0)

    # Plot
    ax = pivot_df.plot(kind="bar", figsize=(12, 6))
    plt.title("Coffee Counts by Weekday")
    plt.xlabel("Weekday")
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.legend(title="Coffee Name", loc="upper right")
    plt.tight_layout()

    # Save chart
    charts_dir = project_root / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    output_path = charts_dir / "coffee_by_weekday.png"
    plt.savefig(output_path)
    plt.close()
    print(f"✅ Coffee by weekday chart saved at: {output_path}")

# --- Average Sales by Time of Day Stacked Line Chart ---
def plot_sales_by_time_of_day(db_path: pathlib.Path, table_name: str = "coffee_sales"):
    with sqlite3.connect(db_path) as conn:
        query = f"""
        SELECT Weekday, Time_of_Day, COUNT(*) as count
        FROM {table_name}
        GROUP BY Weekday, Time_of_Day
        ORDER BY Weekday;
        """
        df = pd.read_sql_query(query, conn)

    if df.empty:
        print("No data available to plot sales by time of day.")
        return

    # Time of day order
    time_order = ["Morning", "Afternoon", "Evening", "Night"]
    df['Time_of_Day'] = pd.Categorical(df['Time_of_Day'], categories=time_order, ordered=True)

    # Weekday order
    weekday_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    df['Weekday'] = pd.Categorical(df['Weekday'], categories=weekday_order, ordered=True)

    # Pivot for stacked line chart
    pivot_df = df.pivot(index="Time_of_Day", columns="Weekday", values="count").fillna(0)

    ax = pivot_df.plot(kind="line", stacked=True, figsize=(12, 6), marker='o')
    plt.title("Average Coffee Sales by Time of Day Across Weekdays")
    plt.xlabel("Time of Day")
    plt.ylabel("Average Sales")
    plt.xticks(rotation=45)
    ax.legend(title="Weekday", labels=weekday_order)
    plt.tight_layout()

    # Save chart
    charts_dir = project_root / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    output_path = charts_dir / "sales_by_time_of_day.png"
    plt.savefig(output_path)
    plt.close()
    print(f"✅ Stacked line chart saved at: {output_path}")

# --- Generate Both Charts ---
plot_coffee_by_weekday(buzz_sqlite)
plot_sales_by_time_of_day(buzz_sqlite)