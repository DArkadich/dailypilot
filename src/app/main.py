import logging
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, filters
)
from .config import TELEGRAM_BOT_TOKEN, LOG_LEVEL
from .db import db_init
from .scheduler import start_reminder_loop, start_nudges_loop, start_weekend_scheduler, schedule_daily_plan
from .handlers import (
    cmd_start, cmd_add, msg_voice, cmd_inbox, cmd_plan, cmd_plan_date,
    cmd_done, cmd_snooze, cmd_week, cmd_export, cmd_unknown, cmd_stats, cmd_health,
    cmd_push_week, cmd_pull_week, cmd_sync_notion, cmd_generate_week,
    cmd_merge_inbox, cmd_commit_week, cmd_drop, cmd_writeback_ids, cmd_reflect, msg_text_any, cmd_ai_review, cmd_weekend, cmd_calendar_advice, cmd_can_take, callback_can_take, cmd_fix_times, cmd_roll_over
)

def main():
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")

    db_init()

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler("inbox", cmd_inbox))
    app.add_handler(CommandHandler("plan", cmd_plan))
    app.add_handler(CommandHandler("plan_date", cmd_plan_date))
    app.add_handler(CommandHandler("done", cmd_done))
    app.add_handler(CommandHandler("snooze", cmd_snooze))
    app.add_handler(CommandHandler("drop", cmd_drop))
    app.add_handler(CommandHandler("week", cmd_week))
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("health", cmd_health))
    app.add_handler(CommandHandler("push_week", cmd_push_week))
    app.add_handler(CommandHandler("pull_week", cmd_pull_week))
    app.add_handler(CommandHandler("sync_notion", cmd_sync_notion))
    app.add_handler(CommandHandler("generate_week", cmd_generate_week))
    app.add_handler(CommandHandler("merge_inbox", cmd_merge_inbox))
    app.add_handler(CommandHandler("commit_week", cmd_commit_week))
    app.add_handler(CommandHandler("writeback_ids", cmd_writeback_ids))
    app.add_handler(CommandHandler("reflect", cmd_reflect))
    app.add_handler(CommandHandler("ai_review", cmd_ai_review))
    app.add_handler(CommandHandler("weekend", cmd_weekend))
    app.add_handler(CommandHandler("calendar_advice", cmd_calendar_advice))
    app.add_handler(CommandHandler("can_take", cmd_can_take))
    app.add_handler(CommandHandler("fix_times", cmd_fix_times))
    app.add_handler(CommandHandler("roll_over", cmd_roll_over))
    
    # Обработчик callback для кнопок /can_take
    app.add_handler(CallbackQueryHandler(callback_can_take, pattern="^can_take_"))

    # Текстовый ответ для /reflect
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), msg_text_any))
    app.add_handler(MessageHandler(filters.VOICE & (~filters.COMMAND), msg_voice))
    app.add_handler(MessageHandler(filters.COMMAND, cmd_unknown))

    # Уведомления о сроках
    start_reminder_loop(app)
    
    # Пинки (утренняя лягушка, вечерняя рефлексия)
    start_nudges_loop(app)
    # Авто-weekend отчёт
    start_weekend_scheduler(app)

    # Планировщик ежедневного плана 08:00
    try:
        import asyncio
        asyncio.get_event_loop().create_task(schedule_daily_plan(app))
    except Exception:
        logging.exception("Failed to start schedule_daily_plan")

    app.run_polling()

if __name__ == "__main__":
    main()
