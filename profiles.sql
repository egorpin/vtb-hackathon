-- Создаем таблицу профилей
CREATE TABLE IF NOT EXISTS load_profiles (
    id SERIAL PRIMARY KEY,
    profile_name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    recommendations JSONB
);

-- Создаем таблицу для результатов тестирования
CREATE TABLE IF NOT EXISTS benchmark_results (
    id SERIAL PRIMARY KEY,
    profile_name VARCHAR(50) NOT NULL,
    test_type VARCHAR(20) NOT NULL,
    tpm DECIMAL(10,2),
    nopm DECIMAL(10,2),
    avg_latency DECIMAL(10,4),
    tps DECIMAL(10,2),
    duration_minutes INTEGER,
    clients INTEGER,  -- ДОБАВЛЕН СТОЛБЕЦ
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Наполняем данными (8 профилей для максимального балла)
INSERT INTO load_profiles (profile_name, description, recommendations) VALUES
('IDLE', 'Система простаивает', '{}'),

('Classic OLTP', 'Высокий TPS, короткие транзакции (Банкинг, Биллинг)',
 '{"checkpoint_timeout": "15min", "random_page_cost": "1.1", "shared_buffers": "25% RAM"}'),

('Heavy OLAP', 'Сложные аналитические запросы, высокая нагрузка на CPU/RAM',
 '{"work_mem": "64MB", "max_parallel_workers": "4", "effective_cache_size": "75% RAM"}'),

('Disk-Bound OLAP', 'Аналитика, упирающаяся в диск (IO Wait)',
 '{"work_mem": "128MB", "effective_io_concurrency": "200", "max_worker_processes": "8"}'),

('IoT / Ingestion', 'Интенсивная запись (INSERT), мало чтений',
 '{"max_wal_size": "10GB", "synchronous_commit": "off", "checkpoint_timeout": "30min"}'),

('Mixed / HTAP', 'Смешанная нагрузка (транзакции + отчеты)',
 '{"min_wal_size": "2GB", "maintenance_work_mem": "512MB"}'),

('Web / Read-Only', 'Преобладает чтение данных (Каталоги, CMS)',
 '{"autovacuum_naptime": "1min", "wal_level": "minimal"}'),

('Bulk Load', 'Массовая заливка данных (Миграция)',
 '{"fsync": "off (TEMPORARY!)", "autovacuum": "off", "full_page_writes": "off"}')
ON CONFLICT (profile_name) DO NOTHING;
