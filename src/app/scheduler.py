import threading
import time
import logging
from datetime import datetime, timezone, timedelta
from telegram.constants import ParseMode
from telegram.error import TelegramError
from .db import due_overdues
from .config import TZINFO, ALLOWED_USER_ID
from .backup import create_backup

logger = logging.getLogger(__name__)
# Хранилище уже отправленных напоминаний (id задачи -> время)
_sent_reminders = {}
_weekend_manual_date = None
_weekend_last_sent = None

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

def mark_weekend_manual_invoked():
    from datetime import datetime
    global _weekend_manual_date
    _weekend_manual_date = datetime.now(TZINFO).date()

def start_weekend_scheduler(app):
    """По воскресеньям в 22:00 отправляет weekend-отчёт, если не запускали вручную до 21:30."""
    def loop():
        global _weekend_last_sent
        from datetime import datetime, time
        while True:
            try:
                now = datetime.now(TZINFO)
                if now.weekday() == 6:  # Sunday
                    today = now.date()
                    # если ещё не отправляли
                    if _weekend_last_sent != today:
                        # если не было ручного запуска (или дата не сегодняшняя) и время >= 22:00
                        if not (_weekend_manual_date == today) and (now.hour >= 22):
                            # Сформировать краткий отчёт (облегчённый, без GPT при сбое)
                            try:
                                from .integrations.sheets import get_week_tasks_done_last_7d, get_reflections_last_7d
                                tasks = get_week_tasks_done_last_7d()
                                refl = get_reflections_last_7d()
                                by_ctx = {}
                                for t in tasks:
                                    ctx = (t.get("Direction") or "").strip()
                                    by_ctx[ctx] = by_ctx.get(ctx, 0) + 1
                                ctx_lines = [f"- {k}: {v}" for k, v in sorted(by_ctx.items(), key=lambda x: (-x[1], x[0]))] or ["(no data)"]
                                out = ["📅 Weekend summary", "\n".join(ctx_lines)]
                                app.bot.send_message(chat_id=ALLOWED_USER_ID, text="\n".join(out))
                            except Exception:
                                pass
                            _weekend_last_sent = today
                time.sleep(60)
            except Exception:
                time.sleep(60)
    th = threading.Thread(target=loop, daemon=True)
    th.start()

def start_nudges_loop(app):
    """Напоминания в определённое время дня (лягушка утром, рефлексия вечером)"""
    logger.info("Starting nudges loop")
    
    def loop():
        from datetime import datetime
        
        sent_today = {"frog": False, "reflect": False, "commit_week": False}
        while True:
            now = datetime.now(TZINFO)
            try:
                # Лягушка (08:00)
                if now.hour == 8 and now.minute == 0 and not sent_today["frog"]:
                    app.bot.send_message(chat_id=ALLOWED_USER_ID, text="🐸 Напомнить: отметь лягушку дня (/plan)")
                    sent_today["frog"] = True
                    logger.info("Frog nudge sent")
                
                # Рефлексия (21:00)
                if now.hour == 21 and now.minute == 0 and not sent_today["reflect"]:
                    app.bot.send_message(chat_id=ALLOWED_USER_ID, text="🪞 Рефлексия 5 минут: используй /reflect для ежедневной рефлексии.")
                    sent_today["reflect"] = True
                    logger.info("Reflection nudge sent")
                
                # Авто-обновление commit_week (03:00)
                if now.hour == 3 and now.minute == 0 and not sent_today["commit_week"]:
                    try:
                        from .integrations.sheets import import_week_from_sheets_to_bot
                        added = import_week_from_sheets_to_bot()
                        app.bot.send_message(
                            chat_id=ALLOWED_USER_ID,
                            text=f"✅ Авто-синхронизация: добавлено задач из Week_Tasks: {added}"
                        )
                        logger.info(f"Auto commit_week: added {added} tasks")
                    except Exception as e:
                        logger.error(f"Error in auto commit_week: {e}", exc_info=True)
                    sent_today["commit_week"] = True
                
                # Сброс флагов в полночь
                if now.hour == 0 and now.minute == 0:
                    sent_today = {"frog": False, "reflect": False, "commit_week": False}
                    logger.info("Nudges reset for new day")
            except Exception as e:
                logger.error(f"Error in nudges loop: {e}", exc_info=True)
            time.sleep(60)
    
    th = threading.Thread(target=loop, daemon=True)
    th.start()

import asyncio

async def schedule_daily_plan(app):
    """Ежедневная отправка плана в 08:00 по TZINFO через asyncio."""
    from .db import list_today, iso_utc, db_connect
    from .handlers import _pick_plan
    while True:
        try:
            now = datetime.now(TZINFO)
            target = now.replace(hour=8, minute=0, second=0, microsecond=0)
            if target <= now:
                target = target + timedelta(days=1)
            await asyncio.sleep((target - now).total_seconds())

            # Сбор плана (эквивалент логики /plan)
            start = target.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=1)
            rows = list_today(ALLOWED_USER_ID, iso_utc(target), iso_utc(start), iso_utc(end))
            if not rows:
                # fallback к открытым топ задачам
                conn = db_connect()
                rows = conn.cursor().execute(
                    "SELECT id,title,context,due_at,priority,est_minutes FROM tasks WHERE chat_id=? AND status='open' ORDER BY priority DESC LIMIT 10",
                    (ALLOWED_USER_ID,)
                ).fetchall()
                conn.close()

            frog, stones, sand = _pick_plan(rows)
            lines = ["📅 *План на сегодня*"]
            if frog:
                lines.append("\n🐸 *ЛЯГУШКА*")
                lines += [f"#{r['id']} {r['title']} — [{r['context']}]" for r in frog]
            if stones:
                lines.append("\n◼︎ *КАМНИ*")
                lines += [f"#{r['id']} {r['title']} — [{r['context']}]" for r in stones[:3]]
            if sand:
                lines.append("\n▫︎ *ПЕСОК*")
                lines += [f"#{r['id']} {r['title']} — [{r['context']}]" for r in sand[:3]]

            msg = await app.bot.send_message(chat_id=ALLOWED_USER_ID, text="\n".join(lines), parse_mode=ParseMode.MARKDOWN)
            logger.info(f"[INFO] Daily plan sent at {datetime.now(TZINFO).strftime('%H:%M')}, message_id={msg.message_id}")
        except Exception:
            logger.exception("[ERROR] Daily plan failed")
            # продолжим цикл, не падаем
            await asyncio.sleep(60)
