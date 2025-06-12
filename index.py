import psycopg2

# Executados: 7021, 7028, 7023, 7030, 7006, 7003, 7008, 7011

# Pendente: 7005

ports = []
user = "pedro.andrade"
password = "xGyBlEU6cdC3y5PIw1iFbh0qEkcla8R2"

with open("execute.sql", "r", encoding="UTF-8") as file:
        sql = file.read()

for port in ports:
    try:
        print(f"\n Conectando na porta {port}.")
        connection = psycopg2.connect(
            host="127.0.0.1",
            port=port,
            database="siscobraweb",
            user=user,
            password=password,
            connect_timeout = 15
        )

        cursor = connection.cursor()
        cursor.execute(sql)

        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"A conexao falhou: {e}")