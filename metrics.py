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
            db_time_accumulated = 0.0
            try:
                cur.execute("SELECT sum(total_exec_time) FROM pg_stat_statements")
                res = cur.fetchone()
                if res and res[0]:
                    db_time_accumulated = float(res[0]) / 1000.0
            except psycopg2.Error:
                self.conn.rollback()
                db_time_accumulated = 0.0

            # 3. Активные сессии
            cur.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND pid <> pg_backend_pid()")
            active_sessions = int(cur.fetchone()[0])

            # 4. Вставки vs Чтения vs ОБНОВЛЕНИЯ vs УДАЛЕНИЯ (Расширенный набор)
            # Мы добавили tup_updated и tup_deleted
            cur.execute("SELECT sum(tup_inserted), sum(tup_fetched), sum(tup_updated), sum(tup_deleted) FROM pg_stat_database")
            row = cur.fetchone()
            tup_inserted = float(row[0] or 0)
            tup_fetched = float(row[1] or 0)
            tup_updated = float(row[2] or 0) # NEW
            tup_deleted = float(row[3] or 0) # NEW

            # 5. Ожидания
            cur.execute("""
                SELECT wait_event_type, count(*)
                FROM pg_stat_activity
                WHERE state = 'active'
                GROUP BY wait_event_type
            """)
            waits = dict(cur.fetchall())

            # 6. Max Latency
            cur.execute("""
                SELECT coalesce(max(extract(epoch from (now() - query_start))), 0)
                FROM pg_stat_activity
                WHERE state = 'active' AND pid <> pg_backend_pid()
            """)
            max_duration = cur.fetchone()[0]

        return {
            "time": time.time(),
            "commits": commits,
            "rollbacks": rollbacks,
            "db_time_accumulated": db_time_accumulated,
            "active_sessions": active_sessions,
            "waits": waits,
            "tup_inserted": tup_inserted,
            "tup_fetched": tup_fetched,
            "tup_updated": tup_updated, # NEW
            "tup_deleted": tup_deleted, # NEW
            "max_duration": float(max_duration or 0)
        }
