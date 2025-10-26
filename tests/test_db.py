import unittest
import os
import tempfile
from datetime import datetime, timezone
from src.app.db import (
    db_init, add_task, list_open_tasks, mark_done,
    iso_utc, list_inbox
)

class TestDB(unittest.TestCase):
    
    def setUp(self):
        """Создаем временную БД для тестов"""
        self.temp_db = tempfile.mktemp(suffix='.db')
        os.environ['DB_PATH'] = self.temp_db
        db_init()
    
    def tearDown(self):
        """Удаляем временную БД"""
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)
    
    def test_add_task(self):
        """Тест добавления задачи"""
        task_id = add_task(
            chat_id=123,
            title="Test task",
            description="Test description",
            context_tag="AI",
            due_at_iso=None,
            added_at_iso=iso_utc(datetime.now(timezone.utc)),
            priority=50.0,
            est_minutes=30,
            source="text"
        )
        self.assertIsNotNone(task_id)
        self.assertGreater(task_id, 0)
    
    def test_list_open_tasks(self):
        """Тест получения открытых задач"""
        # Добавляем несколько задач
        add_task(123, "Task 1", "", "AI", None, iso_utc(datetime.now()), 80, 15, "text")
        add_task(123, "Task 2", "", "AI", None, iso_utc(datetime.now()), 60, 45, "text")
        
        tasks = list_open_tasks(123)
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0]["title"], "Task 1")
    
    def test_mark_done(self):
        """Тест выполнения задачи"""
        task_id = add_task(123, "Task", "", "AI", None, iso_utc(datetime.now()), 50, 30, "text")
        self.assertTrue(mark_done(123, task_id))
        
        # Проверяем, что задача больше не в открытых
        tasks = list_open_tasks(123)
        self.assertEqual(len(tasks), 0)
    
    def test_list_inbox(self):
        """Тест инбокса (задачи без дедлайна)"""
        add_task(123, "No deadline", "", "AI", None, iso_utc(datetime.now()), 50, 30, "text")
        add_task(123, "With deadline", "", "AI", iso_utc(datetime.now(timezone.utc)), 
                iso_utc(datetime.now()), 50, 30, "text")
        
        inbox = list_inbox(123)
        self.assertEqual(len(inbox), 1)
        self.assertEqual(inbox[0]["title"], "No deadline")

if __name__ == '__main__':
    unittest.main()
