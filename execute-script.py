from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")

ports = [5432]

with open("execute.sql", "r", encoding="UTF-8") as file:
        sql = file.read()

for port in ports:
    try:
        print(f"\n Conectando na porta {port}.")
        connection = psycopg2.connect(
            host = host,
            port = port,
            database = database,
            user = user,
            password = password,
            connect_timeout = 15
        )

        cursor = connection.cursor()
        cursor.execute(sql)

        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"A conexao falhou: {e}")