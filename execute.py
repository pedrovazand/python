from db_utils import execute_sql

with open("./sql/correcao.sql", "r", encoding="UTF-8") as file:
        sql = file.read()

#7001, 7002, 7003, 7005, 7006, 7007, 7008, 7010, 7011, 7012, 7014, 7015, 7017, 7018, 7019, 7020, 7021, 7022, 7023, 7024, 7026, 7027, 7028, 7029, 7030, 7031

ports = []

execute_sql(sql, ports)