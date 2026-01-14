# Basic ETL Pipeline Script
# This script extracts sales data from a CSV file, transforms it by cleaning and adding useful columns
import pandas as pd
import sqlite3
from datetime import datetime

print("Starting basic ETL pipeline...")

# ──────────────── EXTRACT ────────────────
print("1. Extracting data from CSV...")
df = pd.read_csv("data/raw_sales.csv")

# Show what we got
print(df)

# ──────────────── TRANSFORM ────────────────
print("\n2. Transforming / cleaning data...")

# Fix missing values (fill missing Quantity with 1)
df["Quantity"] = df["Quantity"].fillna(1)

# Convert Date to real date type
df["Date"] = pd.to_datetime(df["Date"])

# Create new useful columns
df["Total"] = df["Quantity"] * df["Price"]               # revenue per order
df["Month"] = df["Date"].dt.strftime("%Y-%m")            # for grouping later

# Make everything consistent (e.g. capitalize Region)
df["Region"] = df["Region"].str.title()

# Optional: remove unrealistic rows (example rule)
df = df[df["Total"] > 0]

print("Cleaned data:")
print(df)

# ──────────────── LOAD ────────────────
print("\n3. Loading to SQLite database...")

# Connect to (or create) a local database file
conn = sqlite3.connect("my_warehouse.db")

# Save the table (if exists → replace it for simplicity)
df.to_sql("sales_clean", conn, if_exists="replace", index=False)

# Optional: also save as new CSV for checking
df.to_csv("data/clean_sales_2025.csv", index=False)

print("Done! Check 'my_warehouse.db' or 'data/clean_sales_2025.csv'")

# Bonus: quick check from database
print("\nQuick preview from database:")
check = pd.read_sql("SELECT * FROM sales_clean LIMIT 3", conn)
print(check)

conn.close()