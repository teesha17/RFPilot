import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    DB_HOST = "localhost"
    DB_NAME = "rfpilot"
    DB_USER = "postgres"
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    SECRET_KEY = os.getenv("SECRET_KEY")
    ALGORITHM = "HS256"

settings = Settings()