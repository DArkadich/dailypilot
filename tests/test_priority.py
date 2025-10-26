import unittest
from datetime import datetime, timedelta
from src.app.handlers import estimate_minutes, compute_priority, importance_boost
from src.app.config import TZINFO

class TestPriority(unittest.TestCase):
    
    def test_estimate_minutes_low(self):
        """Тест оценки времени для коротких задач"""
        self.assertEqual(estimate_minutes("Позвонить клиенту"), 15)
        self.assertEqual(estimate_minutes("Написать письмо"), 15)
    
    def test_estimate_minutes_mid(self):
        """Тест оценки времени для средних задач"""
        self.assertEqual(estimate_minutes("Собрать документацию"), 45)
        self.assertEqual(estimate_minutes("Оформить отчет"), 45)
    
    def test_estimate_minutes_high(self):
        """Тест оценки времени для длинных задач"""
        self.assertEqual(estimate_minutes("Разработать новый бот"), 90)
        self.assertEqual(estimate_minutes("Декомпозировать проект"), 90)
    
    def test_importance_boost(self):
        """Тест бонуса важности"""
        self.assertGreater(importance_boost("Клиент звонок"), 0)
        self.assertGreater(importance_boost("Разработать бот"), 0)
        # Лягушка должна иметь большой бонус
        self.assertGreater(importance_boost("Лягушка срочная задача"), 12)
    
    def test_compute_priority(self):
        """Тест расчета приоритета"""
        # Задача без дедлайна
        pr1 = compute_priority("Обычная задача", None, 30)
        self.assertGreater(pr1, 0)
        self.assertLess(pr1, 100)
        
        # Срочная задача (сегодня)
        due_soon = datetime.now(TZINFO) + timedelta(hours=2)
        pr2 = compute_priority("Срочная задача", due_soon, 30)
        self.assertGreater(pr2, pr1)
        
        # Важная задача
        pr3 = compute_priority("Клиент звонок", None, 15)
        self.assertGreater(pr3, pr1)

if __name__ == '__main__':
    unittest.main()
