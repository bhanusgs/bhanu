#COMBINED PYTHON CODE TO EXTRACT CSV AND JSON FILE:

import json
import psycopg2
import csv

# Database connection parameters
DB_HOST = 'localhost'
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'Appaji@1970'
DB_PORT = '5432'

# Function to connect to PostgreSQL
def connect_to_db():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

# Function to ingest CSV and JSON data
def ingest_data():
    conn = connect_to_db()
    cur = conn.cursor()

    # Ingest orders from CSV
    with open('orders.csv', 'r') as file:
        data_reader = csv.reader(file)
        next(data_reader)  # Skip header
        for row in data_reader:
            cur.execute("""
                INSERT INTO orders(order_id, customer_id, product_id, quantity, order_date)
                VALUES (%s, %s, %s, %s, %s)
            """, row)

    # Ingest products from JSON
    with open(r"C:\Users\tvrka\products.json", 'r') as f:
        products = json.load(f)
        for product in products:
            cur.execute("""
                INSERT INTO products(product_id, product_name,category,price)
                VALUES (%s, %s, %s,%s)
            """, (
                product.get('product_id'),
                product.get('product_name'),
                product.get('category'),
                product.get('price')
            ))

    conn.commit()
    cur.close()
    conn.close()
    print("Data ingested successfully")

INGEST_DATA()
