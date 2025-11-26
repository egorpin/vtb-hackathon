import os

# Интервал анализа в секундах
ANALYSIS_INTERVAL = 5

# Настройки подключения (читаем из Docker ENV)
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "mydb"),
    "user": os.getenv("DB_USER", "user"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

# Справочник рекомендаций
RECOMMENDATIONS = {
    "IDLE": {},
    "Classic OLTP": {
        "checkpoint_timeout": "15min",
        "random_page_cost": "1.1",
        "shared_buffers": "25% RAM"
    },
    "Heavy OLAP": {
        "work_mem": "64MB",
        "max_parallel_workers": "4",
        "effective_cache_size": "75% RAM"
    },
    "Web / Read-Only": {
        "autovacuum_naptime": "1min",
        "wal_level": "minimal"
    },
    "IoT / Ingestion": {
        "max_wal_size": "10GB",
        "synchronous_commit": "off",
        "checkpoint_timeout": "30min"
    },
    "Mixed / HTAP": {
        "min_wal_size": "2GB",
        "maintenance_work_mem": "512MB"
    },
    "Bulk Load": {
        "fsync": "off (TEMPORARY!)",
        "autovacuum": "off"
    },
    "Session Heavy": {
        "max_connections": "1000",
        "huge_pages": "off"
    },
    "Geo / Compute": {
        "jit": "on",
        "max_parallel_workers_per_gather": "4"
    }
}