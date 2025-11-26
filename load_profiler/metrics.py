import psycopg2
import time
import sys
from colorama import Fore

class MetricsCollector:
    def __init__(self, config):
        try:
            self.conn = psycopg2.connect(**config)
            self.conn.autocommit = True
            print(f"{Fore.GREEN}[OK] Подключение к БД успешно ({config['host']}).")
            self._init_extensions()
        except Exception as e:
            print(f"{Fore.RED}[ERROR] Не удалось подключиться к БД: {e}")
            sys.exit(1)

    def _init_extensions(self):
        with self.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")

    def get_snapshot(self):
        with self.conn.cursor() as cur:
            # 1. Коммиты и откаты
            cur.execute("SELECT sum(xact_commit), sum(xact_rollback) FROM pg_stat_database")
            commits, rollbacks = cur.fetchone()

            # 2. Активные сессии (ASH)
            cur.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND pid <> pg_backend_pid()")
            active_sessions = cur.fetchone()[0]

            # 3. Ожидания (Wait Events)
            cur.execute("""
                SELECT wait_event_type, count(*) 
                FROM pg_stat_activity 
                WHERE state = 'active' 
                GROUP BY wait_event_type
            """)
            waits = dict(cur.fetchall())
            
            # 4. Вставки и чтения
            cur.execute("SELECT sum(tup_inserted), sum(tup_fetched) FROM pg_stat_database")
            tup_inserted, tup_fetched = cur.fetchone()

            # 5. Максимальная длительность транзакции
            cur.execute("SELECT coalesce(max(extract(epoch from (now() - query_start))), 0) FROM pg_stat_activity WHERE state = 'active'")
            max_duration = cur.fetchone()[0]

        return {
            "time": time.time(),
            "commits": float(commits or 0),
            "rollbacks": float(rollbacks or 0),
            "active_sessions": int(active_sessions or 0),
            "waits": waits,
            "tup_inserted": float(tup_inserted or 0),
            "tup_fetched": float(tup_fetched or 0),
            "max_duration": float(max_duration or 0)
        }