import sqlite3
import pandas as pd

conn = sqlite3.connect("buzz.sqlite")
df = pd.read_sql_query("SELECT Weekday, coffee_name, sales FROM coffee_sales", conn)
print(df.head())
print(df.groupby("Weekday")["sales"].sum())