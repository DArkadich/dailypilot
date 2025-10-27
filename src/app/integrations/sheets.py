import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from ..db import db_connect
from ..config import TZINFO

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "")
GCP_CREDS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

SHEET_WEEK_TASKS = "Week_Tasks"
SHEET_DAYS = "Days"
SHEET_MOTIVATION = "Motivation"

def _client():
    if not SPREADSHEET_ID or not GCP_CREDS:
        raise RuntimeError("GOOGLE_SHEETS_SPREADSHEET_ID или GOOGLE_APPLICATION_CREDENTIALS не заданы")
    creds = Credentials.from_service_account_file(GCP_CREDS, scopes=SCOPES)
    return gspread.authorize(creds)

def _open_sheet():
    gc = _client()
    return gc.open_by_key(SPREADSHEET_ID)

def export_week_from_bot_to_sheets():
    """Формирует Week_Tasks + Days из задач бота и пишет в Google Sheets.
       Фикс: wk_rows как список словарей; дедупликация по (Direction, Task)."""
    sh = _open_sheet()

    # --- Берём открытые задачи из БД бота ---
    conn = db_connect()
    c = conn.cursor()
    c.execute("""
      SELECT id,title,description,context,due_at,priority,est_minutes
      FROM tasks WHERE status='open'
    """)
    rows = c.fetchall()
    conn.close()

    # подготовим корзины по контекстам
    ctx_order = ["AI","Horien","Energy","System"]
    tasks = {k: [] for k in ctx_order}
    for r in rows:
        ctx = (r["context"] or "System")
        if ctx in tasks:
            tasks[ctx].append(r)

    # сортировка по приоритету (высший выше) и "коротким" задачам
    for k in ctx_order:
        tasks[k].sort(key=lambda r: (-(r["priority"] or 0), r["est_minutes"] or 999))

    # дедлайн недели
    now_local_dt = datetime.now(TZINFO)
    week_start = now_local_dt - timedelta(days=now_local_dt.weekday())
    week_end = week_start + timedelta(days=6)
    week_end_str = week_end.strftime("%Y-%m-%d")

    # --- Сбор Week_Tasks: ТОР задач по контекстам ---
    wk_rows = []
    picked = {
        "AI": 3, "Horien": 3, "Energy": 2, "System": 2
    }
    seen = set()
    def _norm(s: str) -> str:
        return " ".join((s or "").strip().lower().split())

    for ctx in ctx_order:
        topn = picked.get(ctx, 2)
        for r in tasks[ctx][:topn]:
            rec = {
                "Direction": ctx,
                "Task": r["title"],
                "Outcome": r["description"] or "",
                "Deadline": (r["due_at"] or "")[:10] or week_end_str,
                "Status": "in_progress",
                "Progress_%": 0,
                "Notes": f"task_id={r['id']}"
            }
            key = (rec["Direction"], _norm(rec["Task"]))
            if key in seen:
                continue
            seen.add(key)
            wk_rows.append(rec)

    # если вдруг пусто — каркас
    if not wk_rows:
        wk_rows = [
            {"Direction":"AI","Task":"Определи идею MVP","Outcome":"","Deadline":week_end_str,"Status":"planned","Progress_%":0,"Notes":""},
            {"Direction":"Horien","Task":"Обнови OOS/остатки","Outcome":"","Deadline":week_end_str,"Status":"planned","Progress_%":0,"Notes":""},
            {"Direction":"Energy","Task":"2 ночи 7+ часов","Outcome":"","Deadline":week_end_str,"Status":"planned","Progress_%":0,"Notes":""},
            {"Direction":"System","Task":"Сделай /plan каждый вечер","Outcome":"","Deadline":week_end_str,"Status":"planned","Progress_%":0,"Notes":""},
        ]

    # --- Раскладка по дням (лягушка+камни) ---
    days_names = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]
    start = week_start
    flat = wk_rows[:]  # список словарей
    i = 0
    day_rows = []
    for d in range(7):
        date_str = (start + timedelta(days=d)).strftime("%Y-%m-%d")
        frog = flat[i]["Task"] if i < len(flat) else ""
        i += 1
        stone1 = flat[i]["Task"] if i < len(flat) else ""
        i += 1
        stone2 = flat[i]["Task"] if i < len(flat) else ""
        i += 1
        day_rows.append([date_str, days_names[d], frog, False, stone1, False, stone2, False, "", 0, "", "", "", "", "", 0])

    # --- Пишем в Sheets ---
    df_week = pd.DataFrame(wk_rows, columns=["Direction","Task","Outcome","Deadline","Status","Progress_%","Notes"])
    df_days = pd.DataFrame(day_rows, columns=[
        "Date","Day","Frog","Frog_Done","Stone1","Stone1_Done","Stone2","Stone2_Done",
        "Sand","Energy_0_10","Reflection_Q1","Reflection_Q2","Reflection_Q3","Reflection_Q4","Reflection_Q5",
        "Completed_Today_Count"
    ])

    ws = sh.worksheet(SHEET_WEEK_TASKS)
    ws.clear()
    ws.update([df_week.columns.tolist()] + df_week.values.tolist())

    ws2 = sh.worksheet(SHEET_DAYS)
    ws2.clear()
    ws2.update([df_days.columns.tolist()] + df_days.values.tolist())

    return len(df_week), len(df_days)

def import_week_from_sheets_to_bot():
    """Читает Week_Tasks из Google Sheets и добавляет/обновляет задачи в боте с дедлайнами недели."""
    sh = _open_sheet()
    ws = sh.worksheet(SHEET_WEEK_TASKS)
    data = ws.get_all_records()
    if not data:
        return 0
    # Запишем каждую строку как задачу (если нет task_id в Notes) — добавим новую.
    from ..db import add_task, iso_utc
    from ..handlers import compute_priority, estimate_minutes, parse_human_dt, now_local
    added = 0
    for row in data:
        title = row.get("Task","").strip()
        if not title: continue
        ctx = row.get("Direction","System") or "System"
        due = row.get("Deadline","")
        due_dt = parse_human_dt(due) if due else None
        est = estimate_minutes(title)
        pr = compute_priority(title, due_dt, est)
        add_task(0, title, row.get("Outcome",""), ctx, iso_utc(due_dt), iso_utc(now_local()), pr, est, "sheets")
        added += 1
    return added
