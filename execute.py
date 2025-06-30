from db_utils import execute_sql

with open("./sql/correcao.sql", "r", encoding="UTF-8") as file:
        sql = file.read()

ports = [7007, 7015, 7029]

execute_sql(sql, ports)