import os
import pytz
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
ALLOWED_USER_ID = int(os.environ["ALLOWED_USER_ID"])
LOCAL_TZ = os.getenv("TZ", "Europe/Moscow")
# Путь к БД - используем /app/data внутри контейнера где у пользователя есть права
default_db = os.getenv("DB_PATH", "/app/data/daily_pilot.db")
DB_PATH = default_db
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

TZINFO = pytz.timezone(LOCAL_TZ)
