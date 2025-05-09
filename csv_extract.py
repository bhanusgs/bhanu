#INITIAL CODE FOR EXCTRACTING CSV

import psycopg2
import csv

#database connection paramaters
DB_HOST='localhost'
DB_NAME='postgres'
DB_USER='postgres'
DB_PASSWORD='Appaji@1970'
DB_PORT='5432'

#function to connect to postgres
def connect_to_db():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
#main function to ingest data
def ingest_data():
    conn=connect_to_db()
    cur=conn.cursor()

#open CSV file
    with open('orders.csv','r') as file:
        data_reader=csv.reader(file)
        next(data_reader)
        for row in data_reader:
            cur.execute("INSERT INTO orders(order_id,customer_id,product_id,quantity,order_date) values (%s,%s,%s,%s,%s)",row)
            print(row)
    conn.commit()
    cur.close()
    conn.close()
    print("Data ingested successfully")
