import os
import shutil
import logging
from datetime import datetime
from .config import DB_PATH

logger = logging.getLogger(__name__)

def get_backup_dir():
    """Возвращает директорию для бэкапов"""
    base_dir = os.path.dirname(DB_PATH)
    backup_dir = os.path.join(base_dir, "backups")
    os.makedirs(backup_dir, exist_ok=True)
    return backup_dir

def create_backup():
    """Создает бэкап БД"""
    try:
        if not os.path.exists(DB_PATH):
            logger.warning("Database file not found, skipping backup")
            return None
        
        backup_dir = get_backup_dir()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"daily_pilot_backup_{timestamp}.db"
        backup_path = os.path.join(backup_dir, backup_filename)
        
        shutil.copy2(DB_PATH, backup_path)
        logger.info(f"Backup created: {backup_path}")
        
        # Удаляем старые бэкапы (старше 7 дней)
        cleanup_old_backups()
        
        return backup_path
    except Exception as e:
        logger.error(f"Failed to create backup: {e}", exc_info=True)
        return None

def cleanup_old_backups(days=7):
    """Удаляет бэкапы старше N дней"""
    try:
        backup_dir = get_backup_dir()
        if not os.path.exists(backup_dir):
            return
        
        from datetime import timedelta
        cutoff_time = datetime.now() - timedelta(days=days)
        
        for filename in os.listdir(backup_dir):
            if filename.startswith("daily_pilot_backup_"):
                file_path = os.path.join(backup_dir, filename)
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_time < cutoff_time:
                    os.remove(file_path)
                    logger.info(f"Removed old backup: {filename}")
    except Exception as e:
        logger.error(f"Failed to cleanup old backups: {e}", exc_info=True)

def list_backups(limit=10):
    """Возвращает список последних бэкапов"""
    try:
        backup_dir = get_backup_dir()
        if not os.path.exists(backup_dir):
            return []
        
        backups = []
        for filename in os.listdir(backup_dir):
            if filename.startswith("daily_pilot_backup_"):
                file_path = os.path.join(backup_dir, filename)
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                size = os.path.getsize(file_path)
                backups.append({
                    "filename": filename,
                    "path": file_path,
                    "size": size,
                    "created": file_time
                })
        
        backups.sort(key=lambda x: x["created"], reverse=True)
        return backups[:limit]
    except Exception as e:
        logger.error(f"Failed to list backups: {e}", exc_info=True)
        return []
