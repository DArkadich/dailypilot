import os
from notion_client import Client
from datetime import datetime
from ..config import TZINFO

NOTION_TOKEN = os.getenv("NOTION_API_TOKEN","")
DB_WEEK = os.getenv("NOTION_DB_WEEK_TASKS","")
DB_DAYS = os.getenv("NOTION_DB_DAYS","")
DB_MOT = os.getenv("NOTION_DB_MOTIVATION","")

def _cli():
    if not NOTION_TOKEN:
        raise RuntimeError("NOTION_API_TOKEN не задан")
    return Client(auth=NOTION_TOKEN)

def push_week_tasks(tasks_rows):
    """tasks_rows: список dict с полями Direction, Task, Outcome, Deadline, Status, Progress_%."""
    if not DB_WEEK:
        raise RuntimeError("NOTION_DB_WEEK_TASKS не задан")
    n = _cli()
    created = 0
    for r in tasks_rows:
        n.pages.create(parent={"database_id": DB_WEEK}, properties={
            "Direction": {"select": {"name": r["Direction"]}},
            "Task": {"title": [{"text": {"content": r["Task"]}}]},
            "Outcome": {"rich_text": [{"text": {"content": r.get("Outcome","")}}]},
            "Deadline": {"date": {"start": r.get("Deadline")}},
            "Status": {"select": {"name": r.get("Status","planned")}},
            "Progress_%": {"number": float(r.get("Progress_%",0))}
        })
        created += 1
    return created

def push_days(days_rows):
    """days_rows: список dict с полями Date, Day, Frog, Stone1, Stone2 etc."""
    if not DB_DAYS:
        raise RuntimeError("NOTION_DB_DAYS не задан")
    n = _cli()
    created = 0
    for r in days_rows:
        n.pages.create(parent={"database_id": DB_DAYS}, properties={
            "Date": {"date": {"start": r["Date"]}},
            "Day": {"select": {"name": r["Day"]}},
            "Frog": {"title": [{"text": {"content": r.get("Frog","")}}]},
            "Stone1": {"rich_text": [{"text": {"content": r.get("Stone1","")}}]},
            "Stone2": {"rich_text": [{"text": {"content": r.get("Stone2","")}}]},
        })
        created += 1
    return created
