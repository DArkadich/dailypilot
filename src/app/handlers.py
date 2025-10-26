import json
import logging
from datetime import datetime, timedelta, timezone
import dateparser
from dateutil import tz as dateutil_tz
from telegram import Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import ContextTypes
from telegram.error import TelegramError
from .config import ALLOWED_USER_ID, TZINFO
from .db import (
    add_task, list_inbox, list_open_tasks, list_today,
    mark_done, snooze_task, iso_utc, list_week_tasks
)
from .ai import transcribe_ogg_to_text, parse_task
from .metrics import Metrics

logger = logging.getLogger(__name__)
metrics = Metrics()

def ensure_allowed(update: Update) -> bool:
    user_id = update.effective_user.id if update.effective_user else 0
    return user_id == ALLOWED_USER_ID

def now_local():
    return datetime.now(TZINFO)

def parse_human_dt(text: str):
    settings = {
        "TIMEZONE": TZINFO.zone,
        "RETURN_AS_TIMEZONE_AWARE": True,
        "PREFER_DATES_FROM": "future",
        "RELATIVE_BASE": now_local()
    }
    return dateparser.parse(text, settings=settings)

def estimate_minutes(title: str) -> int:
    low = ["позвон", "звонок", "письмо", "написать", "отправ", "созвон", "счёт", "напомнить"]
    mid = ["собрать", "настро", "загруз", "оформ", "опис", "документ", "провер"]
    high = ["разработ", "бот", "проект", "декомпоз", "презентац", "архитектур"]
    t = title.lower()
    if any(k in t for k in high): return 90
    if any(k in t for k in mid):  return 45
    if any(k in t for k in low):  return 15
    return 30

IMPORTANT = [
    "клиент","доход","выручка","счёт","оплата",
    "дет","здоров","сон","гзт","банк","налог","юрист","легал",
    "ai","бот","horien","вб","озон","поставка","логист","oos"
]

def urgency_score(due):
    if not due: return 10.0
    now = now_local()
    delta_h = (due - now).total_seconds() / 3600.0
    if delta_h <= 0: return 100.0
    # 12-часовая шкала
    val = 100.0 * (1.0 / (1.0 + delta_h / 12.0))
    return max(10.0, min(100.0, val))

def importance_boost(title: str) -> float:
    t = title.lower()
    score = 0.0
    for kw in IMPORTANT:
        if kw in t:
            score += 8.0
    if "лягушк" in t:
        score += 12.0
    return score

def duration_bonus(minutes_: int) -> float:
    if minutes_ <= 25: return +10.0
    if minutes_ <= 50: return 0.0
    return -10.0

def compute_priority(title: str, due, est_min: int) -> float:
    u = urgency_score(due)
    i = importance_boost(title)
    d = duration_bonus(est_min)
    raw = 0.5*u + 0.4*i + 0.1*(50 + d)
    return max(0.0, min(100.0, raw))

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    await update.message.reply_text(
        "Привет! Я DailyPilot — голос → задачи → план.\n\n"
        "📋 *Команды:*\n"
        "/add - добавить задачу\n"
        "/inbox - инбокс\n"
        "/plan - план на сегодня\n"
        "/done <id> - выполнить задачу\n"
        "/snooze <id> <время> - отложить\n"
        "/week - неделя\n"
        "/export - экспорт CSV\n"
        "/stats - статистика\n"
        "/health - проверка\n\n"
        "🎙 Отправь голосовое сообщение для добавления задачи.",
        parse_mode=ParseMode.MARKDOWN
    )

async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    try:
        text = " ".join(context.args).strip()
        if not text:
            await update.message.reply_text("Формат: /add <задача> (можно добавить срок: «сегодня 19:00», «завтра», «через 2 часа»)")
            return
        parsed = parse_task(text)
        due_dt = parse_human_dt(parsed.get("due")) if parsed.get("due") else None
        est = estimate_minutes(parsed["title"])
        pr = compute_priority(parsed["title"], due_dt, est)
        tid = add_task(
            update.effective_chat.id,
            parsed["title"], parsed["description"],
            parsed["context"],
            iso_utc(due_dt), iso_utc(now_local()), pr, est, "text"
        )
        msg = f"✅ Добавлено #{tid}: *{parsed['title']}*\n"
        if due_dt:
            msg += f"🗓 {due_dt.astimezone(TZINFO).strftime('%d.%m %H:%M')}\n"
        msg += f"📎 [{parsed['context']}] • ⏱~{est} мин • ⚡{int(pr)}"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error in cmd_add: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при добавлении задачи. Попробуйте ещё раз.")

async def msg_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    if not update.message.voice: return
    try:
        await update.message.chat.send_action(ChatAction.TYPING)
        file = await context.bot.get_file(update.message.voice.file_id)
        ogg_bytes = await file.download_as_bytearray()
        text = transcribe_ogg_to_text(bytes(ogg_bytes))
        parsed = parse_task(text)
        due_dt = parse_human_dt(parsed.get("due")) if parsed.get("due") else None
        est = estimate_minutes(parsed["title"])
        pr = compute_priority(parsed["title"], due_dt, est)
        tid = add_task(
            update.effective_chat.id,
            parsed["title"], parsed["description"],
            parsed["context"],
            iso_utc(due_dt), iso_utc(now_local()), pr, est, "voice"
        )
        msg = (f"🎙 Распознано: _{text}_\n\n"
               f"✅ Добавлено #{tid}: *{parsed['title']}*\n")
        if due_dt:
            msg += f"🗓 {due_dt.astimezone(TZINFO).strftime('%d.%m %H:%M')}\n"
        msg += f"📎 [{parsed['context']}] • ⏱~{est} мин • ⚡{int(pr)}"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error in msg_voice: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при обработке голосового сообщения. Попробуйте ещё раз.")

async def cmd_inbox(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    try:
        rows = list_inbox(update.effective_chat.id)
        if not rows:
            await update.message.reply_text("📥 Инбокс пуст.")
            return
        lines = ["📥 *Инбокс*:"]
        for r in rows:
            tid, title, ctx, due, pr = r["id"], r["title"], r["context"], r["due_at"], r["priority"]
            lines.append(f"#{tid} • {title} — [{ctx}] • ⚡{int(pr)}")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error in cmd_inbox: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при получении задач.")

def _pick_plan(rows):
    if not rows: return [], [], []
    frog = rows[0:1]
    stones = rows[1:4]
    sand = rows[4:]
    return frog, stones, sand

async def cmd_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    try:
        now = now_local()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        rows = list_today(update.effective_chat.id, iso_utc(now), iso_utc(start), iso_utc(end))
        if not rows:
            rows = list_open_tasks(update.effective_chat.id)[:10]
        frog, stones, sand = _pick_plan(rows)
        def fmt(r):
            due_str = ""
            if r["due_at"]:
                from datetime import datetime
                dt = datetime.fromisoformat(r["due_at"]).astimezone(TZINFO)
                due_str = f" • 🗓 {dt.strftime('%H:%M')}"
            return f"#{r['id']} {r['title']} — [{r['context']}] • ⚡{int(r['priority'])} • ⏱~{r['est_minutes']}м{due_str}"

        out = ["📅 *План на сегодня*"]
        if frog:
            out.append("\n🐸 *ЛЯГУШКА*")
            out += [fmt(x) for x in frog]
        if stones:
            out.append("\n◼︎ *КАМНИ*")
            out += [fmt(x) for x in stones]
        if sand:
            out.append("\n▫︎ *ПЕСОК*")
            out += [fmt(x) for x in sand[:10]]
        await update.message.reply_text("\n".join(out), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error in cmd_plan: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при формировании плана.")

async def cmd_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    try:
        if not context.args:
            await update.message.reply_text("Формат: /done <id>")
            return
        try:
            tid = int(context.args[0])
        except ValueError:
            await update.message.reply_text("id должен быть числом.")
            return
        ok = mark_done(update.effective_chat.id, tid)
        await update.message.reply_text("✅ Готово." if ok else "Не нашёл открытую задачу с таким id.")
    except Exception as e:
        logger.error(f"Error in cmd_done: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при выполнении задачи.")

async def cmd_snooze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    try:
        if len(context.args) < 2:
            await update.message.reply_text("Формат: /snooze <id> <когда> (пример: /snooze 12 завтра 10:00)")
            return
        try:
            tid = int(context.args[0])
        except ValueError:
            await update.message.reply_text("id должен быть числом.")
            return
        when = " ".join(context.args[1:])
        new_due = parse_human_dt(when)
        if not new_due:
            await update.message.reply_text("Не понял дату. Пример: завтра 10:00")
            return
        ok = snooze_task(update.effective_chat.id, tid, iso_utc(new_due))
        await update.message.reply_text("⏳ Перенёс." if ok else "Не нашёл задачу.")
    except Exception as e:
        logger.error(f"Error in cmd_snooze: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при переносе задачи.")

async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    try:
        now = now_local().replace(hour=0, minute=0, second=0, microsecond=0)
        end = now + timedelta(days=7)
        # Используем SQL фильтрацию вместо Python
        rows = list_week_tasks(update.effective_chat.id, iso_utc(now), iso_utc(end))
        if not rows:
            await update.message.reply_text("На неделю пока пусто.")
            return
        lines = ["🗓 *Неделя (7 дней)*"]
        current = ""
        for r in rows:
            from datetime import datetime
            dt = datetime.fromisoformat(r["due_at"]).astimezone(TZINFO)
            day = dt.strftime("%a %d.%m")
            if day != current:
                current = day
                lines.append(f"\n*{day}*")
            lines.append(f"#{r['id']} {r['title']} — [{r['context']}] • ⏱~{r['est_minutes']}м • ⚡{int(r['priority'])} • {dt.strftime('%H:%M')}")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error in cmd_week: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при получении плана на неделю.")

async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    conn = None
    try:
        import csv, io
        from .db import db_connect
        conn = db_connect()
        c = conn.cursor()
        c.execute("SELECT id,title,description,context,due_at,added_at,status,priority,est_minutes,source FROM tasks ORDER BY id;")
        rows = c.fetchall()
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["id","title","description","context","due_at","added_at","status","priority","est_minutes","source"])
        for r in rows:
            w.writerow([r["id"],r["title"],r["description"],r["context"],r["due_at"],r["added_at"],r["status"],r["priority"],r["est_minutes"],r["source"]])
        await update.message.reply_document(document=buf.getvalue().encode("utf-8"), filename="daily_pilot_export.csv", caption="Экспорт задач (CSV)")
    except Exception as e:
        logger.error(f"Error in cmd_export: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при экспорте данных.")
    finally:
        if conn:
            conn.close()

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    try:
        stats = metrics.get_stats(update.effective_chat.id)
        if not stats:
            await update.message.reply_text("❌ Ошибка при получении статистики.")
            return
        
        productivity = metrics.get_productivity_score(update.effective_chat.id)
        
        lines = ["📊 *Статистика*"]
        lines.append(f"\n📝 Всего задач: {stats['total_tasks']}")
        lines.append(f"✅ Выполнено: {stats['done_tasks']}")
        lines.append(f"🔄 Открыто: {stats['open_tasks']}")
        lines.append(f"⏰ С дедлайном: {stats['tasks_with_deadline']}")
        lines.append(f"🎙 Голосовых: {stats['voice_tasks']}")
        
        lines.append(f"\n📈 *За неделю*")
        lines.append(f"Добавлено: {stats['tasks_added_week']}")
        lines.append(f"Выполнено: {stats['tasks_done_week']}")
        
        if productivity > 0:
            lines.append(f"\n⚡ Productivity score: {productivity}%")
        
        if stats['top_contexts']:
            lines.append(f"\n🏷 *Топ контексты*")
            for ctx in stats['top_contexts']:
                lines.append(f"{ctx['context']}: {ctx['count']}")
        
        lines.append(f"\n💾 Размер БД: {stats['db_size_kb']} КБ")
        
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error in cmd_stats: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при получении статистики.")

async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    try:
        import sys
        import platform
        from .config import DB_PATH
        import os
        
        lines = ["🏥 *Health Check*"]
        lines.append(f"\n🐍 Python: {sys.version.split()[0]}")
        lines.append(f"💻 OS: {platform.system()}")
        lines.append(f"📍 Timezone: {TZINFO}")
        
        # Проверка БД
        if os.path.exists(DB_PATH):
            db_size = os.path.getsize(DB_PATH)
            lines.append(f"✅ DB: {round(db_size/1024, 1)} КБ")
        else:
            lines.append("❌ DB: не найдена")
        
        # Проверка бэкапов
        from .backup import list_backups
        backups = list_backups(1)
        if backups:
            lines.append(f"💾 Backups: {len(backups)} последний")
        else:
            lines.append("⚠️ Backups: нет бэкапов")
        
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error in cmd_health: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при проверке здоровья.")

async def cmd_push_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    try:
        # import pandas as pd
        from .integrations.sheets import export_week_from_bot_to_sheets
        
        wk_count, days_count = export_week_from_bot_to_sheets()
        await update.message.reply_text(f"✅ В Sheets отправлено: Week_Tasks={wk_count}, Days={days_count}")
    except Exception as e:
        logger.error(f"Error in cmd_push_week: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка экспорта в Sheets: {e}")

async def cmd_pull_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    try:
        from .integrations.sheets import import_week_from_sheets_to_bot
        
        added = import_week_from_sheets_to_bot()
        await update.message.reply_text(f"✅ Из Sheets подтянуто задач: {added}")
    except Exception as e:
        logger.error(f"Error in cmd_pull_week: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка импорта из Sheets: {e}")

async def cmd_sync_notion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Берём актуальные таблицы из Sheets и шьём в Notion базы (если настроены IDs)."""
    if not ensure_allowed(update): return
    try:
        from .integrations.sheets import _open_sheet, SHEET_WEEK_TASKS, SHEET_DAYS
        from .integrations.notion import push_week_tasks, push_days
        
        sh = _open_sheet()
        wk = sh.worksheet(SHEET_WEEK_TASKS).get_all_records()
        ds = sh.worksheet(SHEET_DAYS).get_all_records()
        t1 = push_week_tasks(wk) if wk else 0
        # подготовим минимальные поля для Days
        days_rows = [{"Date": r["Date"], "Day": r["Day"], "Frog": r["Frog"], "Stone1": r["Stone1"], "Stone2": r["Stone2"]} for r in ds]
        t2 = push_days(days_rows) if ds else 0
        await update.message.reply_text(f"✅ В Notion добавлено: Week_Tasks={t1}, Days={t2}")
    except Exception as e:
        logger.error(f"Error in cmd_sync_notion: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка синхронизации Notion: {e}")

async def cmd_generate_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Генерирует неделю из Goals/Projects в Sheets."""
    if not ensure_allowed(update): return
    try:
        from .integrations.planner import generate_week_from_goals
        
        wk_count, days_count, added = generate_week_from_goals()
        await update.message.reply_text(
            f"✅ Неделя сгенерирована:\n"
            f"📋 Week_Tasks: {wk_count}\n"
            f"🗓 Days: {days_count}\n"
            f"🎯 Задач в боте: {added}"
        )
    except Exception as e:
        logger.error(f"Error in cmd_generate_week: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка генерации недели: {e}")

async def cmd_unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ensure_allowed(update): return
    await update.message.reply_text("Команды: /add /inbox /plan /done /snooze /week /export /stats /health /push_week /pull_week /sync_notion /generate_week")
