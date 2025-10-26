import logging
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, filters
)
from .config import TELEGRAM_BOT_TOKEN, LOG_LEVEL
from .db import db_init
from .scheduler import start_reminder_loop
from .handlers import (
    cmd_start, cmd_add, msg_voice, cmd_inbox, cmd_plan,
    cmd_done, cmd_snooze, cmd_week, cmd_export, cmd_unknown, cmd_stats, cmd_health,
    cmd_push_week, cmd_pull_week, cmd_sync_notion, cmd_generate_week
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
    app.add_handler(CommandHandler("done", cmd_done))
    app.add_handler(CommandHandler("snooze", cmd_snooze))
    app.add_handler(CommandHandler("week", cmd_week))
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("health", cmd_health))
    app.add_handler(CommandHandler("push_week", cmd_push_week))
    app.add_handler(CommandHandler("pull_week", cmd_pull_week))
    app.add_handler(CommandHandler("sync_notion", cmd_sync_notion))
    app.add_handler(CommandHandler("generate_week", cmd_generate_week))

    app.add_handler(MessageHandler(filters.VOICE & (~filters.COMMAND), msg_voice))
    app.add_handler(MessageHandler(filters.COMMAND, cmd_unknown))

    # Уведомления о сроках
    start_reminder_loop(app)

    app.run_polling()

if __name__ == "__main__":
    main()
