import os
import logging
import pandas as pd
import gspread
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from ..db import db_connect
from ..config import ALLOWED_USER_ID
from ..config import TZINFO

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "")
GCP_CREDS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

SHEET_WEEK_TASKS = "Week_Tasks"
SHEET_DAYS = "Days"
SHEET_MOTIVATION = "Motivation"
SHEET_REFLECTIONS = "Reflections"

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

def _norm_title(s: str) -> str:
    import re
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s\-]+", "", s, flags=re.U)   # убрать знаки
    s = re.sub(r"\s+", " ", s, flags=re.U)         # схлопнуть пробелы
    repl = {"хореи":"хориен", "хориэн":"хориен"}   # частые опечатки под себя
    for k,v in repl.items():
        s = s.replace(k, v)
    return s

def import_week_from_sheets_to_bot(force_new: bool = False):
    """Читает Week_Tasks и добавляет задачи в БД, пишет обратно Bot_ID и статус.
    force_new=True — создавать новые задачи даже при совпадении title+Direction в БД.
    """
    sh = _open_sheet()
    ws = sh.worksheet(SHEET_WEEK_TASKS)

    header = ws.row_values(1)
    def need_col(name): return name not in header
    if need_col("Bot_ID"):
        header.append("Bot_ID")
        ws.update_cell(1, len(header), "Bot_ID")
    if need_col("Notes"):
        header.append("Notes")
        ws.update_cell(1, len(header), "Notes")

    header = ws.row_values(1)
    col = {name: (idx+1) for idx, name in enumerate(header)}

    rows = ws.get_all_values()[1:]
    if not rows: return 0

    from ..db import add_task, iso_utc, db_connect
    from ..handlers import compute_priority, estimate_minutes, parse_human_dt, now_local

    conn = db_connect()
    c = conn.cursor()
    # Учитываем только задачи текущего пользователя
    c.execute("SELECT id,title,context FROM tasks WHERE status='open' AND chat_id=? ORDER BY id DESC LIMIT 200;", (ALLOWED_USER_ID,))
    open_rows = c.fetchall()
    conn.close()

    def _norm_title(s): return (s or "").strip().lower().replace("ё","е")
    cache = {(_norm_title(r["title"]), (r["context"] or "").lower()): r["id"] for r in open_rows}

    added, writeback, nowl = 0, [], now_local()

    for r_idx, row in enumerate(rows, start=2):
        status = (row[col.get("Status",0)-1] or "").strip().lower()
        if status and status not in ("planned", "in_progress"):
            continue
        title = (row[col.get("Task",0)-1] or "").strip()
        if not title: continue
        if "Bot_ID" in col and (row[col["Bot_ID"]-1] or "").strip(): continue

        direction = (row[col.get("Direction",0)-1] or "System").strip()
        outcome = (row[col.get("Outcome",0)-1] or "").strip()
        deadline_val = (row[col.get("Deadline",0)-1] or "").strip()
        key = (_norm_title(title), direction.lower())
        # Если уже есть в БД и не форсируем — записываем Bot_ID/Status/Notes обратно и идём дальше
        if (not force_new) and key in cache:
            existing_id = cache[key]
            if "Bot_ID" in col and not (row[col["Bot_ID"]-1] or "").strip():
                writeback.append({"range": rowcol_to_a1(r_idx, col["Bot_ID"]), "values": [[str(existing_id)]]})
            if "Status" in col:
                writeback.append({"range": rowcol_to_a1(r_idx, col["Status"]), "values": [["in_progress"]]})
            if "Notes" in col:
                try:
                    current_notes = (row[col["Notes"]-1] or "").strip()
                except Exception:
                    current_notes = ""
                if not current_notes or "task_id=" not in current_notes:
                    new_notes = (f"{current_notes}\n" if current_notes else "") + f"task_id={existing_id}"
                    writeback.append({"range": rowcol_to_a1(r_idx, col["Notes"]), "values": [[new_notes]]})
            continue

        due_dt = parse_human_dt(deadline_val) if deadline_val else None
        est = estimate_minutes(title)
        pr = compute_priority(title, due_dt, est)

        new_id = add_task(
            ALLOWED_USER_ID,
            title,
            outcome,
            direction,
            iso_utc(due_dt) if due_dt else None,
            iso_utc(nowl),
            pr,
            est,
            "sheets"
        )
        added += 1
        cache[key] = new_id

        writeback.append({"range": rowcol_to_a1(r_idx, col["Bot_ID"]), "values": [[str(new_id)]]})
        writeback.append({"range": rowcol_to_a1(r_idx, col["Status"]), "values": [["in_progress"]]})
        # Дополнительно пишем task_id в Notes, если пусто или нет task_id=
        if "Notes" in col:
            try:
                current_notes = (row[col["Notes"]-1] or "").strip()
            except Exception:
                current_notes = ""
            if not current_notes or "task_id=" not in current_notes:
                new_notes = (f"{current_notes}\n" if current_notes else "") + f"task_id={new_id}"
                writeback.append({"range": rowcol_to_a1(r_idx, col["Notes"]), "values": [[new_notes]]})

    if writeback:
        # Используем worksheet.batch_update с value_input_option
        ws.batch_update(writeback, value_input_option="USER_ENTERED")

    logger.info(f"Added {added} tasks from Week_Tasks")
    return added

def append_reflection(main_task: str, skip_what: str, focus_trap: str, user_label: str, bot_id: str = ""):
    """Добавляет строку в лист Reflections: Date, Main_Task, Skip_What, Focus_Trap, Bot_ID, User.
       Создаёт лист и заголовок при отсутствии."""
    sh = _open_sheet()
    try:
        ws = sh.worksheet(SHEET_REFLECTIONS)
    except Exception:
        ws = sh.add_worksheet(title=SHEET_REFLECTIONS, rows=100, cols=10)
        ws.update([[
            "Date","Main_Task","Skip_What","Focus_Trap","Bot_ID","User"
        ]])

    header = ws.row_values(1)
    # Гарантируем требуемые колонки
    required = ["Date","Main_Task","Skip_What","Focus_Trap","Bot_ID","User"]
    if header != required:
        # Приводим первый ряд к нужным колонкам
        ws.update_cell(1, 1, "Date")
        ws.update_cell(1, 2, "Main_Task")
        ws.update_cell(1, 3, "Skip_What")
        ws.update_cell(1, 4, "Focus_Trap")
        ws.update_cell(1, 5, "Bot_ID")
        ws.update_cell(1, 6, "User")

    from datetime import datetime
    from ..config import TZINFO
    date_str = datetime.now(TZINFO).strftime("%Y-%m-%d")
    ws.append_row([date_str, main_task or "", skip_what or "", focus_trap or "", bot_id or "", user_label or ""], value_input_option="USER_ENTERED")

def get_week_tasks_done_last_7d():
    """Возвращает задачи из Week_Tasks со статусом 'done' за 7 дней: [{Task, Direction, Outcome, Progress_%}]"""
    sh = _open_sheet()
    try:
        ws = sh.worksheet(SHEET_WEEK_TASKS)
    except Exception:
        return []
    records = ws.get_all_records()
    if not records:
        return []
    from dateutil.parser import isoparse
    from datetime import datetime, timedelta
    now = datetime.now(TZINFO)
    seven_days_ago = (now - timedelta(days=7)).date()
    out = []
    for r in records:
        status = (r.get("Status") or "").strip().lower()
        if status != "done":
            continue
        ddl = (r.get("Deadline") or "").strip()
        try:
            dt = isoparse(ddl).date() if ddl else None
        except Exception:
            dt = None
        if dt is None or dt < seven_days_ago:
            continue
        out.append({
            "Task": r.get("Task",""),
            "Direction": r.get("Direction",""),
            "Outcome": r.get("Outcome",""),
            "Progress_%": r.get("Progress_%", 0)
        })
    return out

def get_reflections_last_7d():
    """Возвращает записи из Reflections за 7 дней: [{Date, Main_Task, Skip_What, Focus_Trap}]"""
    sh = _open_sheet()
    try:
        ws = sh.worksheet(SHEET_REFLECTIONS)
    except Exception:
        return []
    records = ws.get_all_records()
    if not records:
        return []
    from dateutil.parser import isoparse
    from datetime import datetime, timedelta
    now = datetime.now(TZINFO)
    seven_days_ago = (now - timedelta(days=7)).date()
    out = []
    for r in records:
        ds = (r.get("Date") or "").strip()
        try:
            dt = isoparse(ds).date() if ds else None
        except Exception:
            dt = None
        if dt is None or dt < seven_days_ago:
            continue
        out.append({
            "Date": ds,
            "Main_Task": r.get("Main_Task",""),
            "Skip_What": r.get("Skip_What",""),
            "Focus_Trap": r.get("Focus_Trap",""),
        })
    return out

def get_week_tasks_last_14d():
    """Возвращает задачи из Week_Tasks за последние 14 дней с полями:
       Task, Direction, Deadline, Status, Time_Estimate, Done_At
       Включает задачи, у которых Deadline или Done_At попадает в последние 14 дней."""
    sh = _open_sheet()
    try:
        ws = sh.worksheet(SHEET_WEEK_TASKS)
    except Exception:
        return []
    records = ws.get_all_records()
    if not records:
        return []
    
    from dateutil.parser import isoparse
    from datetime import datetime, timedelta
    now = datetime.now(TZINFO)
    fourteen_days_ago = (now - timedelta(days=14)).date()
    
    out = []
    for r in records:
        deadline_str = (r.get("Deadline") or "").strip()
        done_at_str = (r.get("Done_At") or "").strip()
        
        # Проверяем обе даты - если хотя бы одна попадает в период, включаем задачу
        should_include = False
        
        if deadline_str:
            try:
                deadline_dt = isoparse(deadline_str).date()
                if deadline_dt >= fourteen_days_ago:
                    should_include = True
            except Exception:
                pass
        
        if done_at_str:
            try:
                done_dt = isoparse(done_at_str).date()
                if done_dt >= fourteen_days_ago:
                    should_include = True
            except Exception:
                pass
        
        # Если нет дат, но задача создана недавно (например, по статусу), тоже можно включить
        # Но для строгости включаем только если есть хотя бы одна дата в периоде
        if not should_include:
            continue
        
        out.append({
            "Task": r.get("Task", ""),
            "Direction": r.get("Direction", ""),
            "Deadline": deadline_str,
            "Status": r.get("Status", ""),
            "Time_Estimate": r.get("Time_Estimate", ""),
            "Done_At": done_at_str,
        })
    
    return out

def get_active_week_tasks():
    """Возвращает активные задачи из Week_Tasks (статусы: planned, in_progress)"""
    sh = _open_sheet()
    try:
        ws = sh.worksheet(SHEET_WEEK_TASKS)
    except Exception:
        return []
    records = ws.get_all_records()
    if not records:
        return []
    
    active_tasks = []
    for r in records:
        status = (r.get("Status") or "").strip().lower()
        if status in ("planned", "in_progress"):
            active_tasks.append({
                "Task": r.get("Task", ""),
                "Direction": r.get("Direction", ""),
                "Deadline": r.get("Deadline", ""),
                "Time_Estimate": r.get("Time_Estimate", ""),
                "Status": status,
            })
    
    return active_tasks
