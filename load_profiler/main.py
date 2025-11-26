import psycopg2
import time
import sys
import os  # <--- Добавили импорт os
from datetime import datetime
from colorama import init, Fore, Style
from tabulate import tabulate

init(autoreset=True)

# --- КОНФИГУРАЦИЯ (Читаем из Docker ENV) ---
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "mydb"),
    "user": os.getenv("DB_USER", "user"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "host": os.getenv("DB_HOST", "localhost"), # По умолчанию localhost, в докере будет 'db'
    "port": os.getenv("DB_PORT", "5432")
}

ANALYSIS_INTERVAL = 5 

# --- СПРАВОЧНИК РЕКОМЕНДАЦИЙ (ДЛЯ 8 ПРОФИЛЕЙ) ---
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
            cur.execute("SELECT sum(xact_commit), sum(xact_rollback) FROM pg_stat_database")
            commits, rollbacks = cur.fetchone()

            cur.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND pid <> pg_backend_pid()")
            active_sessions = cur.fetchone()[0]

            cur.execute("""
                SELECT wait_event_type, count(*) 
                FROM pg_stat_activity 
                WHERE state = 'active' 
                GROUP BY wait_event_type
            """)
            waits = dict(cur.fetchall())
            
            cur.execute("SELECT sum(tup_inserted), sum(tup_fetched) FROM pg_stat_database")
            tup_inserted, tup_fetched = cur.fetchone()

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

class ProfileAnalyzer:
    def analyze(self, prev, curr, duration):
        d_commits = curr["commits"] - prev["commits"]
        d_inserted = curr["tup_inserted"] - prev["tup_inserted"]
        d_fetched = curr["tup_fetched"] - prev["tup_fetched"]
        
        tps = d_commits / duration
        avg_active = (prev["active_sessions"] + curr["active_sessions"]) / 2
        
        tx_cost = (avg_active * duration) / d_commits if d_commits > 0 else 999.0
        read_write_ratio = d_fetched / d_inserted if d_inserted > 0 else 999.0
        io_waits = curr["waits"].get("IO", 0)

        metrics = {
            "TPS": round(tps, 2),
            "Active Sessions (ASH)": round(avg_active, 2),
            "Tx Cost (s)": round(tx_cost, 4),
            "Max Latency (s)": round(curr["max_duration"], 2),
            "IO Waits": io_waits,
            "Rows Inserted/s": round(d_inserted/duration, 0)
        }

        profile = "Unknown"
        confidence = "Low"

        if avg_active < 1 and tps < 5:
            profile = "IDLE"
            confidence = "High"
        
        elif d_inserted > 5000 and d_fetched < 100:
            profile = "Bulk Load"
            confidence = "High"

        elif tps > 50 and tx_cost < 0.05:
            if d_inserted > tps * 0.8: 
                profile = "IoT / Ingestion"
            else:
                profile = "Classic OLTP"
            confidence = "High"

        elif tx_cost > 0.5 or curr["max_duration"] > 2.0:
            if io_waits > 0:
                profile = "Heavy OLAP (Disk Bound)"
            else:
                profile = "Heavy OLAP (CPU/Mem Bound)"
            confidence = "Medium"
            
            if tps > 10:
                profile = "Mixed / HTAP"
                confidence = "Medium"

        elif read_write_ratio > 100 and tps > 5:
            profile = "Web / Read-Only"
            confidence = "High"
            
        else:
            profile = "Mixed / HTAP"
            confidence = "Low"

        return profile, confidence, metrics

def print_dashboard(profile, confidence, metrics):
    print("\033c", end="")
    print(f"{Style.BRIGHT}{Fore.CYAN}=== VTB HACKATHON: Load Profile Detector (Docker Edition) ==={Style.RESET_ALL}")
    print(f"Статус на: {datetime.now().strftime('%H:%M:%S')}")
    print("-" * 50)
    
    p_color = Fore.GREEN if "OLTP" in profile else Fore.YELLOW if "Mixed" in profile else Fore.MAGENTA
    print(f"DETECTED PROFILE: {Style.BRIGHT}{p_color}{profile}{Style.RESET_ALL} (Confidence: {confidence})")
    
    table_data = [[k, v] for k, v in metrics.items()]
    print(tabulate(table_data, headers=["Metric", "Value"], tablefmt="fancy_grid"))
    
    base_profile = profile.split(" (")[0]
    if base_profile in RECOMMENDATIONS:
        print(f"\n{Fore.WHITE}{Style.BRIGHT}Рекомендации (postgresql.conf):{Style.RESET_ALL}")
        recs = RECOMMENDATIONS[base_profile]
        for param, val in recs.items():
            print(f"  {Fore.YELLOW}• {param} = {val}{Style.RESET_ALL}")
    else:
        print(f"\n{Fore.RED}Нет специфичных рекомендаций.{Style.RESET_ALL}")

def main():
    config = DB_CONFIG
    print(f"Попытка подключения к {config['host']}...")
    
    # Небольшая задержка, чтобы БД успела проснуться
    time.sleep(2) 
    
    collector = MetricsCollector(config)
    analyzer = ProfileAnalyzer()
    
    print("Сбор данных...")
    prev_snapshot = collector.get_snapshot()
    time.sleep(1)

    try:
        while True:
            time.sleep(ANALYSIS_INTERVAL)
            curr_snapshot = collector.get_snapshot()
            profile, conf, metrics = analyzer.analyze(prev_snapshot, curr_snapshot, ANALYSIS_INTERVAL)
            print_dashboard(profile, conf, metrics)
            prev_snapshot = curr_snapshot

    except KeyboardInterrupt:
        print("\nОстановка.")

if __name__ == "__main__":
    main()