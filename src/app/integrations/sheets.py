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
    """Формирует Week_Tasks + Days из задач бота и пишет в Google Sheets."""
    sh = _open_sheet()
    # --- Week_Tasks: берём открытые задачи и агрегируем по контекстам ---
    conn = db_connect()
    c = conn.cursor()
    c.execute("""
      SELECT id,title,description,context,due_at,priority,est_minutes
      FROM tasks WHERE status='open'
    """)
    rows = c.fetchall()
    conn.close()

    # Соберём «Week_Tasks»: выберем топ-3 по контекстам AI/Horien/Energy/System
    ctx_order = ["AI","Horien","Energy","System"]
    tasks = {k: [] for k in ctx_order}
    for r in rows:
        ctx = (r["context"] or "System")
        if ctx not in tasks: continue
        tasks[ctx].append(r)
    for k in ctx_order:
        tasks[k].sort(key=lambda r: (-(r["priority"] or 0), r["est_minutes"] or 999))

    week_end = (datetime.now(TZINFO) - timedelta(days=datetime.now(TZINFO).weekday())) + timedelta(days=6)
    week_end_str = week_end.strftime("%Y-%m-%d")

    wk_rows = []
    for ctx in ctx_order:
        pick = tasks[ctx][:3] if ctx in ("AI","Horien") else tasks[ctx][:2]
        for r in pick:
            wk_rows.append([
                ctx,
                r["title"],
                r["description"] or "",
                (r["due_at"] or "")[:10] or week_end_str,
                "in_progress",
                0,
                f"task_id={r['id']}"
            ])

    if not wk_rows:
        # хотя бы каркас
        wk_rows = [
            ["AI","Определи идею MVP","",week_end_str,"planned",0,""],
            ["Horien","Обнови OOS/остатки","",week_end_str,"planned",0,""],
            ["Energy","2 ночи 7+ часов","",week_end_str,"planned",0,""],
            ["System","Сделай /plan каждый вечер","",week_end_str,"planned",0,""],
        ]

    df_week = pd.DataFrame(wk_rows, columns=["Direction","Task","Outcome","Deadline","Status","Progress_%","Notes"])

    # --- Days: раскидать «лягушку и камни» на 7 дней ---
    start = datetime.now(TZINFO) - timedelta(days=datetime.now(TZINFO).weekday())
    days = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]
    day_rows = []
    # простой greedy: берем по очереди из wk_rows в приоритетном порядке
    flat = wk_rows.copy()
    i = 0
    for d in range(7):
        date_str = (start + timedelta(days=d)).strftime("%Y-%m-%d")
        frog = flat[i]["Task"] if i < len(flat) else ""
        i += 1
        stone1 = flat[i]["Task"] if i < len(flat) else ""
        i += 1
        stone2 = flat[i]["Task"] if i < len(flat) else ""
        i += 1
        day_rows.append([date_str, days[d], frog, False, stone1, False, stone2, False, "", 0, "", "", "", "", "", 0])

    df_days = pd.DataFrame(day_rows, columns=[
        "Date","Day","Frog","Frog_Done","Stone1","Stone1_Done","Stone2","Stone2_Done",
        "Sand","Energy_0_10","Reflection_Q1","Reflection_Q2","Reflection_Q3","Reflection_Q4","Reflection_Q5",
        "Completed_Today_Count"
    ])

    # Пишем в Google Sheets
    ws = sh.worksheet(SHEET_WEEK_TASKS)  # таблица уже создана в шаблоне
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
