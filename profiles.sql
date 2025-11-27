-- Очистка старых данных (опционально, если нужно пересоздать)
-- TRUNCATE TABLE load_profiles;

INSERT INTO load_profiles (profile_name, description, recommendations) VALUES

-- 1. Classic OLTP (Банкинг, биллинг, CRM)
-- Упор на надежность транзакций (ACID), кэширование горячих данных и быстрые коммиты.
('Classic OLTP', 
 'Высокая конкурентность, короткие транзакции, случайный доступ к данным.',
 '{
    "shared_buffers": "25% RAM",
    "random_page_cost": "1.1",
    "effective_io_concurrency": "200",
    "wal_buffers": "16MB",
    "checkpoint_completion_target": "0.9",
    "synchronous_commit": "on"
 }'),

-- 2. Heavy OLAP (Сложная аналитика в памяти)
-- Упор на процессор и память для сортировок и хеширования.
('Heavy OLAP', 
 'Сложные агрегации, JOIN больших таблиц, нагрузка на CPU и RAM.',
 '{
    "work_mem": "64MB",
    "maintenance_work_mem": "512MB",
    "max_parallel_workers_per_gather": "4",
    "effective_cache_size": "75% RAM",
    "jit": "on",
    "random_page_cost": "1.1"
 }'),

-- 3. Disk-Bound OLAP (Аналитика на медленных дисках или огромных объемах)
-- Упор на оптимизацию чтения с диска и параллелизм.
('Disk-Bound OLAP', 
 'Данные не помещаются в RAM, активное чтение с диска.',
 '{
    "work_mem": "128MB",
    "effective_io_concurrency": "300",
    "max_worker_processes": "8",
    "max_parallel_workers": "8",
    "random_page_cost": "1.5",
    "seq_page_cost": "1.0"
 }'),

-- 4. Web / Read-Only (Каталоги, CMS, Блоги)
-- Максимальная скорость чтения, минимальные блокировки, отложенная запись не страшно.
('Web / Read-Only', 
 'Преобладает чтение (95%+), короткие запросы, редкие изменения.',
 '{
    "autovacuum_naptime": "5min",
    "wal_level": "minimal",
    "synchronous_commit": "off",
    "default_transaction_isolation": "read committed",
    "shared_buffers": "30% RAM"
 }'),

-- 5. IoT / Ingestion (Логи, сенсоры, трекинг)
-- Максимальная пропускная способность на запись (INSERT). Риск потери последних секунд данных допустим.
('IoT / Ingestion', 
 'Потоковая вставка данных, минимум обновлений, Time-Series.',
 '{
    "synchronous_commit": "off",
    "commit_delay": "1000",
    "max_wal_size": "10GB",
    "checkpoint_timeout": "30min",
    "wal_writer_delay": "200ms",
    "autovacuum_analyze_scale_factor": "0.05"
 }'),

-- 6. Mixed / HTAP (Гибридная нагрузка)
-- Баланс между OLTP и OLAP. Сложнее всего настраивать.
('Mixed / HTAP', 
 'Транзакции и отчеты одновременно. Требуется баланс.',
 '{
    "shared_buffers": "40% RAM",
    "work_mem": "32MB",
    "min_wal_size": "2GB",
    "max_wal_size": "8GB",
    "random_page_cost": "1.25",
    "effective_cache_size": "60% RAM"
 }'),

-- 7. End of day Batch (Ночные расчеты, закрытие дня)
-- Задача: прожевать огромный объем данных максимально быстро. Вакуум мешает.
('End of day Batch', 
 'Пакетная обработка больших объемов, ETL, массовые UPDATE/INSERT.',
 '{
    "max_wal_size": "40GB",
    "checkpoint_timeout": "60min",
    "autovacuum": "off",
    "full_page_writes": "off",
    "synchronous_commit": "off",
    "wal_buffers": "64MB"
 }'),

-- 8. Data Maintenance (Обслуживание: VACUUM FULL, REINDEX)
-- Выделяем всю доступную память под служебные операции.
('Data Maintenance', 
 'Реиндексация, очистка мусора, восстановление.',
 '{
    "maintenance_work_mem": "2GB",
    "autovacuum_vacuum_cost_limit": "2000",
    "vacuum_cost_delay": "0",
    "max_parallel_maintenance_workers": "4",
    "wal_level": "minimal"
 }')

ON CONFLICT (profile_name) 
DO UPDATE SET 
    description = EXCLUDED.description,
    recommendations = EXCLUDED.recommendations;