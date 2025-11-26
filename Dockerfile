# Базовый образ
FROM python:3.11-slim

# Переменная, чтобы логи Python сразу летели в консоль Docker
ENV PYTHONUNBUFFERED=1

# --- Установка зависимостей и pgbench ---
# Используем современный метод добавления GPG ключа и установки пакетов.
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    gcc \
    libpq-dev \
    # 1. Очистка и создание папки для ключей
    && rm -f /etc/apt/keyrings/pgdg.gpg \
    && mkdir -p /etc/apt/keyrings \
    # 2. Скачивание и сохранение GPG ключа в новом формате (signed-by)
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/keyrings/pgdg.gpg \
    # 3. Добавление репозитория с указанием, каким ключом он подписан
    && echo "deb [signed-by=/etc/apt/keyrings/pgdg.gpg] http://apt.postgresql.org/pub/repos/apt/ bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    # 4. Повторное обновление apt для получения списка пакетов из нового репозитория
    && apt-get update \
    # 5. Установка финальных пакетов (postgresql-client-16 включает pgbench)
    && apt-get install -y postgresql-client-16 \
    # 6. Очистка кеша
    && rm -rf /var/lib/apt/lists/*

# --- Настройка Python среды ---

# Рабочая директория
WORKDIR /usr/src/app

# Установка Python зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY . .

# Команда запуска
CMD ["python", "load_profiler/main.py"]
