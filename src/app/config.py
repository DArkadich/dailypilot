import os
import pytz
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
ALLOWED_USER_ID = int(os.environ["ALLOWED_USER_ID"])
LOCAL_TZ = os.getenv("TZ", "Europe/Moscow")
# Если DB_PATH не задан, используем /data, если его нет - текущую директорию
default_db = "/data/daily_pilot.db" if os.path.exists("/data") else "./daily_pilot.db"
DB_PATH = os.getenv("DB_PATH", default_db)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

TZINFO = pytz.timezone(LOCAL_TZ)
