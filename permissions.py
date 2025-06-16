import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")

ports = [7021]

with open("permissions.sql", "r", encoding="UTF-8") as file:
        sql = file.read()

with open("permissions-aux.sql", "r", encoding="UTF-8") as file:
        sql_aux = file.read()

for port in ports:
    try:
        print(f"\n Connecting at port {port}.")
        connection = psycopg2.connect(
            host = host,
            port = port,
            database = database,
            user = user,
            password = password,
            connect_timeout = 10
        )

        cursor = connection.cursor()

        cursor.execute(sql)
        array_queries = [row[0] for row in cursor.fetchall()]
        
        for sql in array_queries:
            print(f"Executing: {sql}")
            cursor.execute(sql)

        print(f"Executing: {sql_aux}")
        cursor.execute(sql_aux)
        
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Connection fail, error: {e}")