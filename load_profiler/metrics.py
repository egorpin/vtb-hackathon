import psycopg2
import time
import sys

class MetricsCollector:
    def __init__(self, config):
        try:
            self.conn = psycopg2.connect(**config)
            self.conn.autocommit = True
            self._init_extensions()
        except Exception as e:
            # Улучшена обработка ошибок подключения
            raise ConnectionError(f"Не удалось подключиться к БД: {e}")

    def _init_extensions(self):
        # Гарантируем, что расширение включено
        with self.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
            # Дополнительно: сброс статистики для чистоты эксперимента
            cur.execute("SELECT pg_stat_statements_reset()")


    def get_snapshot(self):
        with self.conn.cursor() as cur:
            # 1. Транзакции (Commit/Rollback)
            cur.execute("SELECT sum(xact_commit), sum(xact_rollback) FROM pg_stat_database")
            commits, rollbacks = cur.fetchone()

            # 2. Активные сессии (ASH)
            # Исключаем текущий pid, чтобы не считать себя
            cur.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND pid <> pg_backend_pid()")
            active_sessions = cur.fetchone()[0]

            # 3. Общее количество подключений (НОВОЕ!)
            cur.execute("SELECT count(*) FROM pg_stat_activity")
            total_connections = cur.fetchone()[0]

            # 4. Ожидания (IO vs CPU)
            cur.execute("""
                SELECT
                    CASE
                        WHEN wait_event_type IS NULL AND state = 'active' THEN 'CPU'
                        WHEN wait_event_type = 'IO' THEN 'IO'
                        ELSE wait_event_type
                    END,
                    count(*)
                FROM pg_stat_activity
                WHERE pid <> pg_backend_pid()
                GROUP BY 1
            """)
            waits = dict(cur.fetchall())

            # 5. Вставки vs Чтения (Rows)
            cur.execute("SELECT sum(tup_inserted), sum(tup_fetched) FROM pg_stat_database")
            tup_inserted, tup_fetched = cur.fetchone()

            # 6. Latency (Самый долгий текущий запрос)
            cur.execute("SELECT coalesce(max(extract(epoch from (now() - query_start))), 0) FROM pg_stat_activity WHERE state = 'active'")
            max_duration = cur.fetchone()[0]

        return {
            "time": time.time(),
            "commits": float(commits or 0),
            "active_sessions": int(active_sessions or 0),
            "total_connections": int(total_connections or 0), # НОВОЕ!
            "waits": waits,
            "tup_inserted": float(tup_inserted or 0),
            "tup_fetched": float(tup_fetched or 0),
            "max_duration": float(max_duration or 0)
        }
