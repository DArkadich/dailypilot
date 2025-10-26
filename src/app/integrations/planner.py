import os
from datetime import datetime, timedelta
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from ..config import TZINFO
from ..db import add_task, iso_utc
from ..handlers import compute_priority, estimate_minutes, now_local
from ..integrations.sheets import SCOPES

SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "")
GCP_CREDS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

def _gc():
    creds = Credentials.from_service_account_file(GCP_CREDS, scopes=SCOPES)
    return gspread.authorize(creds)

def _open():
    if not SPREADSHEET_ID or not GCP_CREDS:
        raise RuntimeError("Sheets: нет SPREADSHEET_ID или GOOGLE_APPLICATION_CREDENTIALS")
    return _gc().open_by_key(SPREADSHEET_ID)

def _week_bounds():
    now = datetime.now(TZINFO)
    start = now - timedelta(days=now.weekday())
    end = start + timedelta(days=6)
    return start, end

def _load_tables(sh):
    goals = sh.worksheet("Goals").get_all_records()
    projects = sh.worksheet("Projects").get_all_records()
    return pd.DataFrame(goals), pd.DataFrame(projects)

def _score_project(row, goals_df):
    # Вес от Goal Weight + близость дедлайна
    goal_weight = 1.0
    if "Goal_Level" in row and "Goal_Objective" in row:
        g = goals_df[(goals_df["Level"] == row["Goal_Level"]) & (goals_df["Objective"] == row["Goal_Objective"])]
        if not g.empty:
            try:
                goal_weight = float(g.iloc[0]["Weight"])
            except Exception:
                goal_weight = 1.0
    # дедлайн
    ddl = row.get("Deadline","")
    soon = 0.0
    try:
        if ddl:
            from dateutil.parser import isoparse
            days = (isoparse(ddl).astimezone(TZINFO) - now_local()).days
            if days <= 0: soon = 1.0
            elif days < 14: soon = 0.7
            elif days < 30: soon = 0.4
    except Exception:
        pass
    # контекстная важность
    ctx = (row.get("Context","System") or "System").lower()
    ctx_b = {"ai":1.0,"horien":1.0,"energy":0.7,"system":0.6}.get(ctx,0.6)
    return goal_weight*0.6 + soon*0.3 + ctx_b*0.1

def generate_week_from_goals():
    """
    1) Читаем Goals/Projects в Sheets
    2) Фильтруем active проекты
    3) Ранжируем и распределяем Weekly_Slots по дням недели
    4) Пишем Week_Tasks и Days обратно в Sheets
    5) Создаём задачи в БД бота с дедлайнами этой недели
    """
    sh = _open()
    goals_df, proj_df = _load_tables(sh)
    if proj_df.empty:
        raise RuntimeError("Пустой лист Projects")

    start, end = _week_bounds()
    days_names = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]

    # 1) фильтр активных
    proj_df = proj_df[proj_df["Status"].str.lower() == "active"].copy()

    # 2) скоринг
    proj_df["Score"] = proj_df.apply(lambda r: _score_project(r, goals_df), axis=1)

    # 3) слоты на неделю
    slots = []
    for _, r in proj_df.sort_values("Score", ascending=False).iterrows():
        try:
            n = int(r.get("Weekly_Slots", 1))
        except Exception:
            n = 1
        for i in range(max(0,n)):
            slots.append({
                "Context": r["Context"],
                "Project_ID": r["Project_ID"],
                "Title": r["Title"],
                "Goal": f'{r["Goal_Level"]}:{r["Goal_Objective"]}',
                "Deadline": r["Deadline"]
            })

    # 4) раскладка слотов как лягушка/камни
    # В день 3 слота максимум: 1 Frog + 2 Stones
    days = [{"Date": (start + timedelta(days=i)), "Day": days_names[i], "Frog":"", "Stones":[]} for i in range(7)]
    si = 0
    for d in days:
        for k in range(3):
            if si >= len(slots): break
            if k == 0 and not d["Frog"]:
                d["Frog"] = slots[si]
            else:
                d["Stones"].append(slots[si])
            si += 1

    # 5) Формируем Week_Tasks для Sheets
    wk_rows = []
    for s in slots:
        ctx = s["Context"]
        task = f'{s["Title"]} — шаг недели'
        outcome = f'Прогресс по проекту {s["Project_ID"]} ({s["Goal"]})'
        ddl = s["Deadline"] or end.strftime("%Y-%m-%d")
        wk_rows.append([ctx, task, outcome, ddl, "in_progress", 0, f'project={s["Project_ID"]}'])

    df_week = pd.DataFrame(wk_rows, columns=["Direction","Task","Outcome","Deadline","Status","Progress_%","Notes"])

    # 6) Формируем Days
    day_rows = []
    for d in days:
        frog = d["Frog"]["Title"] if d["Frog"] else ""
        s1 = d["Stones"][0]["Title"] if len(d["Stones"])>0 else ""
        s2 = d["Stones"][1]["Title"] if len(d["Stones"])>1 else ""
        day_rows.append([
            d["Date"].strftime("%Y-%m-%d"), d["Day"],
            frog, False, s1, False, s2, False,
            "", 0, "", "", "", "", "", 0
        ])
    df_days = pd.DataFrame(day_rows, columns=[
        "Date","Day","Frog","Frog_Done","Stone1","Stone1_Done","Stone2","Stone2_Done",
        "Sand","Energy_0_10","Reflection_Q1","Reflection_Q2","Reflection_Q3","Reflection_Q4","Reflection_Q5",
        "Completed_Today_Count"
    ])

    # 7) Пишем в Sheets (затираем листы Week_Tasks/Days)
    ws_w = sh.worksheet("Week_Tasks")
    ws_w.clear()
    ws_w.update([df_week.columns.tolist()] + df_week.values.tolist())
    ws_d = sh.worksheet("Days")
    ws_d.clear()
    ws_d.update([df_days.columns.tolist()] + df_days.values.tolist())

    # 8) Создаём задачи в БД бота на эту неделю (дедлайны по датам дней для frog/stone)
    added = 0
    for d in days:
        due_base = d["Date"].replace(hour=21, minute=0, second=0, microsecond=0)  # вечерний "должно быть сделано"
        # Frog
        if d["Frog"]:
            title = f'Лягушка: {d["Frog"]["Title"]}'
            est = estimate_minutes(title)
            pr = compute_priority(title, due_base, est)
            add_task(0, title, "", d["Frog"]["Context"], iso_utc(due_base), iso_utc(now_local()), pr, est, "planner")
            added += 1
        # Stones
        for st in d["Stones"]:
            title = f'Камень: {st["Title"]}'
            est = estimate_minutes(title)
            pr = compute_priority(title, due_base, est)
            add_task(0, title, "", st["Context"], iso_utc(due_base), iso_utc(now_local()), pr, est, "planner")
            added += 1

    return len(df_week), len(df_days), added

