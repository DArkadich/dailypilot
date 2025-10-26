import logging
from datetime import datetime, timedelta
from .db import db_connect
from .config import TZINFO, DB_PATH
import os

logger = logging.getLogger(__name__)

class Metrics:
    """Сбор и отображение метрик бота"""
    
    def __init__(self):
        self.conn = None
    
    def _get_connection(self):
        """Получает соединение с БД"""
        if not self.conn:
            self.conn = db_connect()
        return self.conn
    
    def get_stats(self, chat_id):
        """Возвращает статистику для пользователя"""
        try:
            conn = self._get_connection()
            c = conn.cursor()
            
            # Общая статистика
            c.execute("""
                SELECT 
                    COUNT(*) as total_tasks,
                    SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as done_tasks,
                    SUM(CASE WHEN status='open' THEN 1 ELSE 0 END) as open_tasks,
                    SUM(CASE WHEN status='open' AND due_at IS NOT NULL THEN 1 ELSE 0 END) as tasks_with_deadline,
                    SUM(CASE WHEN status='done' AND source='voice' THEN 1 ELSE 0 END) as voice_tasks
                FROM tasks
                WHERE chat_id=?
            """, (chat_id,))
            total_stats = c.fetchone()
            
            # Статистика за последние 7 дней
            week_ago = (datetime.now(TZINFO) - timedelta(days=7)).isoformat()
            c.execute("""
                SELECT 
                    COUNT(*) as tasks_added_week,
                    SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as tasks_done_week
                FROM tasks
                WHERE chat_id=? AND added_at >= ?
            """, (chat_id, week_ago))
            week_stats = c.fetchone()
            
            # Топ контексты
            c.execute("""
                SELECT context, COUNT(*) as count
                FROM tasks
                WHERE chat_id=? AND status='open'
                GROUP BY context
                ORDER BY count DESC
                LIMIT 5
            """, (chat_id,))
            top_contexts = c.fetchall()
            
            # Размер БД
            db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
            
            return {
                "total_tasks": total_stats["total_tasks"],
                "done_tasks": total_stats["done_tasks"],
                "open_tasks": total_stats["open_tasks"],
                "tasks_with_deadline": total_stats["tasks_with_deadline"],
                "voice_tasks": total_stats["voice_tasks"],
                "tasks_added_week": week_stats["tasks_added_week"],
                "tasks_done_week": week_stats["tasks_done_week"],
                "top_contexts": [{"context": r["context"], "count": r["count"]} for r in top_contexts],
                "db_size_kb": round(db_size / 1024, 1)
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}", exc_info=True)
            return None
    
    def get_productivity_score(self, chat_id, days=7):
        """Вычисляет productivity score за период"""
        try:
            conn = self._get_connection()
            c = conn.cursor()
            
            start_date = (datetime.now(TZINFO) - timedelta(days=days)).isoformat()
            c.execute("""
                SELECT 
                    SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as done,
                    SUM(CASE WHEN status='open' THEN 1 ELSE 0 END) as open
                FROM tasks
                WHERE chat_id=? AND added_at >= ?
            """, (chat_id, start_date))
            
            result = c.fetchone()
            done = result["done"] or 0
            open = result["open"] or 0
            
            if done + open == 0:
                return 0
            
            score = (done / (done + open)) * 100
            return round(score, 1)
        except Exception as e:
            logger.error(f"Failed to calculate productivity score: {e}", exc_info=True)
            return 0
