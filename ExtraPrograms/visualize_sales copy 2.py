import sqlite3
import pathlib
import matplotlib.pyplot as plt
import pandas as pd

# --- Path to your SQLite database ---
project_root = pathlib.Path(__file__).resolve().parent.parent  # Adjust if script is in visualizations/
buzz_sqlite = project_root / "data" / "buzz.sqlite"
table_name = "coffee_sales"

print(f"Looking for database at: {buzz_sqlite}")

# --- Check that the table exists ---
with sqlite3.connect(buzz_sqlite) as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("Tables in DB:", tables)

# --- Read data from the table ---
with sqlite3.connect(buzz_sqlite) as conn:
    query = f"""
    SELECT coffee_name, Weekday, COUNT(*) as count
    FROM {table_name}
    GROUP BY coffee_name, Weekday
    ORDER BY Weekday;
    """
    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Error reading table: {e}")
        df = pd.DataFrame()

print("Rows returned:", len(df))
print(df.head())

# --- Visualization function ---
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
        print("No data available to plot.")
        return

    # Pivot for grouped bar chart
    pivot_df = df.pivot(index="Weekday", columns="coffee_name", values="count").fillna(0)

    # Plot
    ax = pivot_df.plot(kind="bar", figsize=(12, 6))
    plt.title("Coffee Counts by Weekday")
    plt.xlabel("Weekday")
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.legend(title="Coffee Name")
    plt.tight_layout()

    # Create charts folder in project root
    charts_dir = project_root / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    # Save chart
    output_path = charts_dir / "coffee_by_weekday.png"
    plt.savefig(output_path)
    plt.close()
    print(f"✅ Chart saved at: {output_path}")

# --- Generate the chart ---
plot_coffee_by_weekday(buzz_sqlite)