import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")

ports = []

with open("./sql/execute.sql", "r", encoding="UTF-8") as file:
        sql = file.read()

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

        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Connection fail, error: {e}")