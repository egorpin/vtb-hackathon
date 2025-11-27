import psycopg2
import time

class MetricsCollector:
    def __init__(self, config):
        try:
            self.conn = psycopg2.connect(**config)
            self.conn.autocommit = True
            self._init_extensions()
        except Exception as e:
            raise ConnectionError(f"Не удалось подключиться к БД: {e}")

    def _init_extensions(self):
        """Пытаемся включить pg_stat_statements для точного учета времени"""
        with self.conn.cursor() as cur:
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
            except psycopg2.Error:
                pass # Игнорируем, если прав нет, будем использовать ASH

    def get_snapshot(self):
        with self.conn.cursor() as cur:
            # 1. Транзакции (Commit/Rollback)
            cur.execute("SELECT sum(xact_commit), sum(xact_rollback) FROM pg_stat_database")
            row = cur.fetchone()
            commits = float(row[0] or 0)
            rollbacks = float(row[1] or 0)

            # 2. DB Time (через pg_stat_statements если есть, иначе 0)
            # total_exec_time - это чистое время выполнения запросов
            db_time_accumulated = 0.0
            try:
                # Для PG < 13 используйте total_time, для PG >= 13 total_exec_time
                cur.execute("""
                    SELECT sum(total_exec_time)
                    FROM pg_stat_statements
                """)
                res = cur.fetchone()
                if res and res[0]:
                    db_time_accumulated = float(res[0]) / 1000.0 # перевод из мс в секунды
            except psycopg2.Error:
                # Если таблицы нет, ставим 0, анализатор переключится на ASH
                self.conn.rollback()
                db_time_accumulated = 0.0

            # 3. Активные сессии (ASH Snapshot) - мгновенный снимок
            # Это fallback метрика для DB Time
            cur.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND pid <> pg_backend_pid()")
            active_sessions = int(cur.fetchone()[0])

            # 4. Вставки vs Чтения (Rows)
            cur.execute("SELECT sum(tup_inserted), sum(tup_fetched) FROM pg_stat_database")
            row = cur.fetchone()
            tup_inserted = float(row[0] or 0)
            tup_fetched = float(row[1] or 0)

            # 5. Ожидания (для диагностики узких мест)
            cur.execute("""
                SELECT wait_event_type, count(*)
                FROM pg_stat_activity
                WHERE state = 'active'
                GROUP BY wait_event_type
            """)
            waits = dict(cur.fetchall())

            # 6. Max Latency текущего момента
            cur.execute("SELECT coalesce(max(extract(epoch from (now() - query_start))), 0) FROM pg_stat_activity WHERE state = 'active'")
            max_duration = cur.fetchone()[0]

        return {
            "time": time.time(),
            "commits": commits,
            "rollbacks": rollbacks,
            "db_time_accumulated": db_time_accumulated, # Накопленное время (счетчик)
            "active_sessions": active_sessions,         # Мгновенное значение
            "waits": waits,
            "tup_inserted": tup_inserted,
            "tup_fetched": tup_fetched,
            "max_duration": float(max_duration or 0)
        }
