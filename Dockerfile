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

# Ненулевой пользователь (безопасность)
RUN useradd -ms /bin/bash botuser && chown -R botuser:botuser /app

# Создаем директорию для БД и даем права botuser
RUN mkdir -p /data && chown -R botuser:botuser /data

USER botuser

# Переменная пути к БД по умолчанию (можно переопределить в .env)
ENV DB_PATH=/data/daily_pilot.db

# Запуск
CMD ["python", "-m", "src.app.main"]
