"""
AI-планировщик задач: анализ целей, приоритетов и автоматическое распределение.
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
from ..db import db_connect
from ..config import TZINFO, ALLOWED_USER_ID
from ..ai import get_client
from ..config import OPENAI_API_KEY

logger = logging.getLogger(__name__)

def get_goals_and_projects():
    """Получает Goals и Projects из Google Sheets."""
    try:
        from .sheets import _open_sheet
        sh = _open_sheet()
        goals = sh.worksheet("Goals").get_all_records()
        projects = sh.worksheet("Projects").get_all_records()
        # Фильтруем активные проекты
        active_projects = [p for p in projects if (p.get("Status") or "").strip().lower() == "active"]
        return goals, active_projects
    except Exception as e:
        logger.error(f"Error loading goals/projects: {e}", exc_info=True)
        return [], []

def get_tasks_context(chat_id: int, days: int = 7):
    """Получает контекст задач за последние N дней и открытые задачи."""
    conn = db_connect()
    c = conn.cursor()
    
    # Открытые задачи
    c.execute("""
        SELECT id, title, description, context, due_at, priority, est_minutes, status
        FROM tasks
        WHERE chat_id=? AND status='open'
        ORDER BY priority DESC, due_at ASC
    """, (chat_id,))
    open_tasks = c.fetchall()
    
    # Выполненные за последние дни
    since = (datetime.now(TZINFO) - timedelta(days=days)).isoformat()
    c.execute("""
        SELECT id, title, context, due_at, priority, status
        FROM tasks
        WHERE chat_id=? AND status='done' AND due_at >= ?
        ORDER BY due_at DESC
        LIMIT 50
    """, (chat_id, since))
    done_tasks = c.fetchall()
    
    conn.close()
    return open_tasks, done_tasks

def analyze_and_rebalance_with_ai(chat_id: int, max_sand: int = 3) -> Dict[str, Any]:
    """
    Анализирует задачи с помощью AI, учитывая:
    - Глобальные цели и проекты
    - Текущие открытые задачи
    - История выполнения
    - Приоритеты и дедлайны
    
    Возвращает рекомендации по распределению и переносу задач.
    """
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY не задан")
    
    goals, projects = get_goals_and_projects()
    open_tasks, done_tasks = get_tasks_context(chat_id, days=7)
    
    # Формируем контекст для AI
    goals_text = "\n".join([f"- {g.get('Goal_Level', '')}: {g.get('Goal_Objective', '')} (вес: {g.get('Weight', 0)})" for g in goals[:10]])
    projects_text = "\n".join([f"- {p.get('Title', '')} [{p.get('Context', '')}] (дедлайн: {p.get('Deadline', 'N/A')})" for p in projects[:15]])
    
    open_tasks_text = "\n".join([
        f"#{t['id']}: {t['title']} [{t['context']}] — приоритет {int(t['priority'])}, ~{t['est_minutes'] or 0}м, дедлайн: {(t['due_at'][:10] if t['due_at'] else 'нет')}"
        for t in open_tasks[:30]
    ])
    
    done_recent = len(done_tasks)
    
    prompt = f"""Ты — AI-ассистент по планированию и тайм-менеджменту. Проанализируй текущую ситуацию и дай рекомендации по распределению задач.

ГЛОБАЛЬНЫЕ ЦЕЛИ:
{goals_text}

АКТИВНЫЕ ПРОЕКТЫ:
{projects_text}

ТЕКУЩИЕ ОТКРЫТЫЕ ЗАДАЧИ:
{open_tasks_text}

СТАТИСТИКА:
- Выполнено за последнюю неделю: {done_recent} задач
- Открытых задач сейчас: {len(open_tasks)}

ТВОЯ ЗАДАЧА:
1. Проанализируй, какие задачи критичны для достижения целей и проектов
2. Определи, какие задачи можно перенести на более поздний срок
3. Распредели задачи на следующие 7 дней с учётом:
   - Максимум 1 "лягушка" (важная задача) в день в 09:30
   - Максимум 2 "камня" (средние задачи) в день в 14:30
   - До {max_sand} "песка" (мелкие задачи) в день в 20:30
4. Если задача не критична и не успевается — предложи перенести её на следующую неделю или позже

В ответе дай JSON:
{{
    "critical_tasks": [{{"id": 123, "reason": "критично для проекта X"}}],
    "can_postpone": [{{"id": 456, "new_date": "2025-11-10", "reason": "не критично, можно отложить"}}],
    "distribution": [
        {{"date": "2025-11-06", "frog": 123, "stones": [456, 789], "sand": [101, 102]}},
        ...
    ],
    "recommendations": ["рекомендация 1", "рекомендация 2"]
}}

Отвечай только JSON, без дополнительного текста."""

    client = get_client()
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Ты опытный планировщик задач. Анализируешь приоритеты, цели и распределяешь задачи оптимально. Отвечаешь только валидным JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=2000,
        )
        content = response.choices[0].message.content.strip()
        # Убираем markdown code blocks если есть
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        import json
        result = json.loads(content)
        return result
    except Exception as e:
        logger.error(f"AI planning error: {e}", exc_info=True)
        raise

