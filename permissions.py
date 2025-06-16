from db_utils import execute_sql

with open("permissions.sql", "r", encoding="UTF-8") as file:
        sql = file.read()

ports = [7021]

execute_sql(sql, ports)