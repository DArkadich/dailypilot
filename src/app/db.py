import sqlite3
import logging
import os
from datetime import datetime, timezone
from .config import DB_PATH

logger = logging.getLogger(__name__)

def db_connect():
    # Создаем директорию если её нет
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        logger.info(f"Created database directory: {db_dir}")
    
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def db_init():
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS tasks(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            context TEXT,
            due_at TEXT,          -- ISO UTC
            added_at TEXT,        -- ISO UTC
            status TEXT,          -- open/done/snoozed
            priority REAL,        -- 0..100
            est_minutes INTEGER,  -- оценка длительности
            source TEXT           -- voice/text
        );
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_chat_status ON tasks(chat_id, status);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_due_at ON tasks(due_at);")
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}", exc_info=True)
        raise

def iso_utc(dt):
    if not dt:
        return None
    return dt.astimezone(timezone.utc).isoformat()

def add_task(chat_id, title, description, context_tag, due_at_iso, added_at_iso, priority, est_minutes, source):
    conn = None
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("""
            INSERT INTO tasks(chat_id,title,description,context,due_at,added_at,status,priority,est_minutes,source)
            VALUES (?,?,?,?,?,?,?,?,?,?);
        """, (chat_id, title, description, context_tag, due_at_iso, added_at_iso, "open", priority, est_minutes, source))
        task_id = c.lastrowid
        conn.commit()
        logger.info(f"Task #{task_id} added successfully")
        return task_id
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed to add task: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()

def list_open_tasks(chat_id):
    conn = None
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("""
          SELECT id,title,context,due_at,priority,est_minutes FROM tasks
          WHERE chat_id=? AND status='open'
          ORDER BY priority DESC, id DESC
        """, (chat_id,))
        rows = c.fetchall()
        return rows
    except Exception as e:
        logger.error(f"Failed to list open tasks: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()

def list_inbox(chat_id):
    conn = None
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("""
          SELECT id,title,context,due_at,priority FROM tasks
          WHERE chat_id=? AND status='open' AND (due_at IS NULL)
        """, (chat_id,))
        rows = c.fetchall()
        return rows
    except Exception as e:
        logger.error(f"Failed to list inbox: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()

def list_today(chat_id, now_iso, start_iso, end_iso):
    conn = None
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("""
          SELECT id,title,context,due_at,priority,est_minutes FROM tasks
          WHERE chat_id=? AND status='open'
            AND (
                (due_at IS NOT NULL AND due_at < ?)
                OR (due_at IS NOT NULL AND due_at >= ? AND due_at < ?)
            )
          ORDER BY priority DESC
        """, (chat_id, now_iso, start_iso, end_iso))
        rows = c.fetchall()
        return rows
    except Exception as e:
        logger.error(f"Failed to list today tasks: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()

def mark_done(chat_id, task_id):
    conn = None
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("UPDATE tasks SET status='done' WHERE chat_id=? AND id=? AND status!='done';", (chat_id, task_id))
        changed = c.rowcount
        conn.commit()
        if changed > 0:
            logger.info(f"Task #{task_id} marked as done")
        return changed > 0
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed to mark task done: {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()

def snooze_task(chat_id, task_id, new_due_iso):
    conn = None
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("UPDATE tasks SET due_at=? WHERE chat_id=? AND id=?;", (new_due_iso, chat_id, task_id))
        changed = c.rowcount
        conn.commit()
        if changed > 0:
            logger.info(f"Task #{task_id} snoozed to {new_due_iso}")
        return changed > 0
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed to snooze task: {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()

def due_overdues(now_iso, limit=5):
    conn = None
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("""
          SELECT id, chat_id, title, due_at
          FROM tasks
          WHERE status='open' AND due_at IS NOT NULL AND due_at <= ?
          ORDER BY due_at ASC
          LIMIT ?
        """, (now_iso, limit))
        rows = c.fetchall()
        return rows
    except Exception as e:
        logger.error(f"Failed to get overdue tasks: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()

def list_week_tasks(chat_id, start_iso, end_iso):
    """Список задач на неделю (SQL фильтрация вместо Python)"""
    conn = None
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("""
          SELECT id, title, context, due_at, priority, est_minutes
          FROM tasks
          WHERE chat_id=? AND status='open' AND due_at IS NOT NULL
            AND due_at >= ? AND due_at < ?
          ORDER BY due_at ASC, priority DESC
        """, (chat_id, start_iso, end_iso))
        rows = c.fetchall()
        return rows
    except Exception as e:
        logger.error(f"Failed to list week tasks: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()
