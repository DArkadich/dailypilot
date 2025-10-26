import threading
import time
import logging
from datetime import datetime, timezone, timedelta
from telegram.constants import ParseMode
from telegram.error import TelegramError
from .db import due_overdues
from .config import TZINFO
from .backup import create_backup

logger = logging.getLogger(__name__)
# Хранилище уже отправленных напоминаний (id задачи -> время)
_sent_reminders = {}

def backup_scheduler():
    """Отдельный поток для бэкапов БД каждый час"""
    def backup_loop():
        while True:
            try:
                # Делаем бэкап каждый час
                create_backup()
                time.sleep(3600)  # 1 час
            except Exception as e:
                logger.error(f"Error in backup scheduler: {e}", exc_info=True)
                time.sleep(3600)
    
    logger.info("Starting backup scheduler")
    th = threading.Thread(target=backup_loop, daemon=True)
    th.start()

def start_reminder_loop(app):
    # Запускаем бэкап-поток
    backup_scheduler()
    
    def loop():
        global _sent_reminders
        
        while True:
            try:
                now_utc_iso = datetime.now(timezone.utc).isoformat()
                rows = due_overdues(now_utc_iso, limit=10)
                
                for row in rows:
                    tid, chat_id, title, due_iso = row["id"], row["chat_id"], row["title"], row["due_at"]
                    
                    # Проверяем, не отправляли ли уже напоминание за последний час
                    last_sent = _sent_reminders.get(tid, 0)
                    if time.time() - last_sent < 3600:  # 1 час
                        continue
                    
                    try:
                        app.bot.send_message(
                            chat_id=chat_id,
                            text=f"⏰ Срок: задача #{tid} — *{title}*",
                            parse_mode=ParseMode.MARKDOWN
                        )
                        _sent_reminders[tid] = time.time()
                        logger.info(f"Reminder sent for task #{tid}: {title}")
                    except TelegramError as e:
                        logger.warning(f"Failed to send reminder for task #{tid}: {e}")
                    except Exception as e:
                        logger.error(f"Unexpected error sending reminder for task #{tid}: {e}", exc_info=True)
                
                # Очистка старых записей (старше суток)
                current_time = time.time()
                _sent_reminders = {tid: t for tid, t in _sent_reminders.items() if current_time - t < 86400}
                
                time.sleep(60)
            except Exception as e:
                logger.error(f"Error in reminder loop: {e}", exc_info=True)
                time.sleep(60)
    
    logger.info("Starting reminder loop")
    th = threading.Thread(target=loop, daemon=True)
    th.start()

def start_nudges_loop(app):
    """Напоминания в определённое время дня (лягушка утром, рефлексия вечером)"""
    logger.info("Starting nudges loop")
    
    def loop():
        from datetime import datetime
        from .config import ALLOWED_USER_ID
        
        sent_today = {"frog": False, "reflect": False}
        while True:
            now = datetime.now(TZINFO)
            try:
                if now.hour == 8 and now.minute == 0 and not sent_today["frog"]:
                    app.bot.send_message(chat_id=ALLOWED_USER_ID, text="🐸 Напомнить: отметь лягушку дня (/plan)")
                    sent_today["frog"] = True
                    logger.info("Frog nudge sent")
                if now.hour == 21 and now.minute == 0 and not sent_today["reflect"]:
                    app.bot.send_message(chat_id=ALLOWED_USER_ID, text="🪞 Рефлексия 5 минут: открой лист Days и ответь на 5 вопросов.")
                    sent_today["reflect"] = True
                    logger.info("Reflection nudge sent")
                if now.hour == 0 and now.minute == 0:
                    sent_today = {"frog": False, "reflect": False}
                    logger.info("Nudges reset for new day")
            except Exception as e:
                logger.error(f"Error in nudges loop: {e}", exc_info=True)
            time.sleep(60)
    
    th = threading.Thread(target=loop, daemon=True)
    th.start()
