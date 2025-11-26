# Базовый образ
FROM python:3.11-slim

# Переменная, чтобы логи Python сразу летели в консоль Docker
ENV PYTHONUNBUFFERED=1

# Установка системных зависимостей (нужны для сборки psycopg2)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /usr/src/app

# Установка Python зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY . .

# Команда запуска
CMD ["python", "load_profiler/main.py"]
