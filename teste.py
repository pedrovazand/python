from dotenv import load_dotenv
import os

load_dotenv(dotenv_path=".env.test")

print("User:", os.getenv("DB_USER"))
print("Password:", os.getenv("DB_PASSWORD"))
print("Host:", os.getenv("DB_HOST"))
print("Database:", os.getenv("DB_NAME"))