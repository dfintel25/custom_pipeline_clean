import sqlite3
import pathlib
import matplotlib.pyplot as plt
import pandas as pd

# --- Path to your SQLite database ---
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

# --- Backfill coffee_name for older rows ---
with sqlite3.connect(buzz_sqlite) as conn:
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE coffee_sales
        SET coffee_name = keyword_mentioned
        WHERE coffee_name IS NULL OR coffee_name = ''
    """)
    conn.commit()
    print("✅ Backfilled coffee_name for existing rows.")

# --- Read data from the table ---
with sqlite3.connect(buzz_sqlite) as conn:
    query = f"""
    SELECT coffee_name, Weekday, COUNT(*) as count
    FROM {table_name}
    WHERE coffee_name IS NOT NULL AND coffee_name != ''
    GROUP BY coffee_name, Weekday
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
        WHERE coffee_name IS NOT NULL AND coffee_name != ''
        GROUP BY coffee_name, Weekday
        """
        df = pd.read_sql_query(query, conn)

    if df.empty:
        print("No data available to plot.")
        return

    # Order weekdays Monday → Sunday
    weekday_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    df['Weekday'] = pd.Categorical(df['Weekday'], categories=weekday_order, ordered=True)
    df = df.sort_values('Weekday')

    # Pivot for grouped bar chart
    pivot_df = df.pivot(index="Weekday", columns="coffee_name", values="count").fillna(0)
    pivot_df['Total'] = pivot_df.sum(axis=1)  # sum all coffee types per weekday
    pivot_df['Total'].plot(kind='line', marker='o', color='black', linewidth=2, label='Total Trend', ax=ax)
    
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