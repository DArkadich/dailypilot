import os
import pytz
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
ALLOWED_USER_ID = int(os.environ["ALLOWED_USER_ID"])
LOCAL_TZ = os.getenv("TZ", "Europe/Moscow")
# Путь к БД - используем /app/db где у пользователя есть права
DB_PATH = os.getenv("DB_PATH", "/app/db/daily_pilot.db")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

TZINFO = pytz.timezone(LOCAL_TZ)
