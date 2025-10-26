FROM python:3.11-slim

# Обновления + ffmpeg для голосовых
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
 && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /app

# Зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Исходники
COPY src ./src

# Создаем директорию для БД ПЕРЕД созданием пользователя
RUN mkdir -p /app/db

# Ненулевой пользователь (безопасность)
RUN useradd -ms /bin/bash botuser && chown -R botuser:botuser /app

USER botuser

# Переменная пути к БД (внутри /app где есть права)
ENV DB_PATH=/app/db/daily_pilot.db

# Запуск
CMD ["python", "-m", "src.app.main"]
