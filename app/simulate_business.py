import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost",
    database="appdb",
    user="app",
    password="app"
)

cur = conn.cursor()

# Create tables
cur.execute("""
CREATE TABLE IF NOT EXISTS customers(
    customer_id INT PRIMARY KEY,
    name TEXT,
    email TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS products(
    product_id INT PRIMARY KEY,
    name TEXT,
    price FLOAT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS orders(
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT
)
""")

# Load CSV
customers = pd.read_csv("data/customers.csv")
products = pd.read_csv("data/products.csv")
orders = pd.read_csv("data/orders.csv")

# Insert data
for _, row in customers.iterrows():
    cur.execute(
        "INSERT INTO customers VALUES (%s,%s,%s)",
        (
            int(row["customer_id"]),
            str(row["name"]),
            str(row["email"])
        )
    )

for _, row in products.iterrows():
    cur.execute(
        "INSERT INTO products VALUES (%s,%s,%s)",
        (
            int(row["product_id"]),
            str(row["name"]),
            float(row["price"])
        )
    )

for _, row in orders.iterrows():
    cur.execute(
        "INSERT INTO orders VALUES (%s,%s,%s,%s)",
        (
            int(row["order_id"]),
            int(row["customer_id"]),
            int(row["product_id"]),
            int(row["quantity"])
        )
    )

conn.commit()

cur.close()
conn.close()

print("Data successfully inserted into PostgreSQL")