#!/usr/bin/env python3
import json
import psycopg2
import time
from datetime import datetime

class WorkloadClassifier:
    def __init__(self, host="postgres", user="postgres", password="password", database="tpc_tests"):
        # Используем пароль, если он есть, для более надежного соединения
        self.conn_string = f"host={host} user={user} password={password} dbname={database}"

        # Обновленные пороги для более точной классификации (учитывая Tx Cost)
        self.thresholds = {
            'oltp': {'tps_min': 20, 'tx_cost_max': 0.05, 'active_sessions_min': 1},
            'olap': {'tps_max': 5, 'tx_cost_min': 0.1, 'io_wait_min': 1},
            'hybrid': {'tps_min': 5, 'tx_cost_min': 0.05}, # Смесь быстрого и дорогого
            'ingestion': {'insert_ratio_min': 50}, # Соотношение inserted/fetched > 50
            'session_heavy': {'total_connections_min': 50, 'active_sessions_max': 2}
        }

    def collect_snapshot(self):
        """Собирает полный моментальный снимок метрик для расчета дельт"""
        try:
            conn = psycopg2.connect(self.conn_string)
            cur = conn.cursor()

            # 1. Сбор транзакций и операций с кортежами (Rows)
            cur.execute("""
                SELECT
                    sum(xact_commit), sum(xact_rollback),
                    sum(tup_inserted), sum(tup_fetched),
                    sum(blks_read), sum(blks_hit)
                FROM pg_stat_database
            """)
            commits, rollbacks, inserted, fetched, blks_read, blks_hit = cur.fetchone()

            # 2. Сбор активных сессий и ожиданий (ASH/Waits)
            cur.execute("""
                SELECT
                    count(*) as total_conn,
                    sum(CASE WHEN state = 'active' THEN 1 ELSE 0 END) as active_sessions,
                    sum(CASE WHEN wait_event_type = 'IO' THEN 1 ELSE 0 END) as io_waits,
                    coalesce(max(extract(epoch from (now() - query_start))), 0) as max_duration
                FROM pg_stat_activity
                WHERE pid <> pg_backend_pid()
            """)
            total_conn, active_sessions, io_waits, max_duration = cur.fetchone()

            conn.close()

            return {
                "time": time.time(),
                "commits": float(commits or 0),
                "active_sessions": int(active_sessions or 0),
                "total_connections": int(total_conn or 0),
                "tup_inserted": float(inserted or 0),
                "tup_fetched": float(fetched or 0),
                "blks_read": float(blks_read or 0),
                "blks_hit": float(blks_hit or 0),
                "io_waits": int(io_waits or 0),
                "max_duration": float(max_duration or 0)
            }
        except Exception as e:
            print(f"Error collecting metrics: {e}")
            return None

    def calculate_workload_indicators(self, prev, curr, duration):
        """Рассчитывает ключевые индикаторы нагрузки на основе двух снимков."""

        d_commits = curr["commits"] - prev["commits"]
        d_inserted = curr["tup_inserted"] - prev["tup_inserted"]
        d_fetched = curr["tup_fetched"] - prev["tup_fetched"]

        tps = d_commits / duration
        avg_active = (prev["active_sessions"] + curr["active_sessions"]) / 2

        # --- ГЛАВНАЯ МЕТРИКА ---
        # Tx Cost (ASH / Commit) = (Avg Active Sessions * Duration) / Commits
        tx_cost = (avg_active * duration) / d_commits if d_commits > 0 else 999.0

        # Соотношение Чтение/Запись: (d_fetched + d_blks_hit) / d_inserted
        read_write_ratio = d_fetched / (d_inserted + 1)

        # Коэффициент попадания в кэш
        total_blks = (curr["blks_read"] - prev["blks_read"]) + (curr["blks_hit"] - prev["blks_hit"])
        cache_hit_ratio = (curr["blks_hit"] - prev["blks_hit"]) / (total_blks + 1)

        indicators = {
            "TPS": round(tps, 2),
            "Tx Cost (s)": round(tx_cost, 4),
            "Active Sessions (ASH)": round(avg_active, 2),
            "Max Latency (s)": round(curr["max_duration"], 2),
            "IO Waits": curr["io_waits"],
            "Total Connections": curr["total_connections"],
            "R/W Ratio": round(read_write_ratio, 2),
            "Cache Hit Ratio": round(cache_hit_ratio, 4)
        }

        return indicators

    def get_detailed_classification(self, indicators):
        """Определяет профиль нагрузки на основе рассчитанных индикаторов."""

        tps = indicators['TPS']
        tx_cost = indicators['Tx Cost (s)']
        avg_active = indicators['Active Sessions (ASH)']
        max_latency = indicators['Max Latency (s)']
        io_waits = indicators['IO Waits']
        total_conn = indicators['Total Connections']
        rw_ratio = indicators['R/W Ratio']

        profile = "Unknown"
        confidence = "Low"

        # 1. IDLE (Простой)
        if avg_active < 1 and tps < 1:
            profile = "IDLE"
            confidence = "High"

        # 2. WRITE-HEAVY / INGESTION (IoT)
        elif rw_ratio < 0.05 and tps > 5: # Очень мало чтений на много записей
            profile = "IoT / Ingestion"
            confidence = "High"

        # 3. CLASSIC OLTP
        elif tps > self.thresholds['oltp']['tps_min'] and tx_cost < self.thresholds['oltp']['tx_cost_max']:
            profile = "Classic OLTP"
            confidence = "High"

        # 4. SESSION-HEAVY
        elif total_conn > self.thresholds['session_heavy']['total_connections_min'] and avg_active < self.thresholds['session_heavy']['active_sessions_max']:
            profile = "Session-Heavy"
            confidence = "Medium"

        # 5. OLAP / HTAP (по высокой стоимости транзакции)
        elif tx_cost > self.thresholds['olap']['tx_cost_min'] or max_latency > 1.0:

            # Mixed / HTAP
            if tps > self.thresholds['hybrid']['tps_min'] and tx_cost > self.thresholds['hybrid']['tx_cost_min']:
                profile = "Mixed / HTAP"
                confidence = "High"

            # Disk-Bound OLAP
            elif io_waits >= self.thresholds['olap']['io_wait_min']:
                profile = "Disk-Bound OLAP"
                confidence = "Medium"

            # Heavy OLAP (CPU-Bound)
            else:
                profile = "Heavy OLAP"
                confidence = "Medium"

        return profile, confidence

    def collect_metrics(self):
        """Collect database metrics for classification"""
        try:
            conn = psycopg2.connect(self.conn_string)
            cur = conn.cursor()

            metrics = {}

            # Database statistics
            cur.execute("""
                SELECT
                    xact_commit,
                    xact_rollback,
                    tup_returned,
                    tup_fetched,
                    tup_inserted,
                    tup_updated,
                    tup_deleted,
                    blks_read,
                    blks_hit
                FROM pg_stat_database
                WHERE datname = 'tpc_tests'
            """)
            db_stats = cur.fetchone()

            if db_stats:
                metrics['db_stats'] = {
                    'xact_commit': db_stats[0],
                    'xact_rollback': db_stats[1],
                    'read_operations': db_stats[2] + db_stats[3],
                    'write_operations': db_stats[4] + db_stats[5] + db_stats[6],
                    'cache_hit_ratio': db_stats[8] / (db_stats[7] + db_stats[8]) if (db_stats[7] + db_stats[8]) > 0 else 0
                }
            else:
                metrics['db_stats'] = {
                    'xact_commit': 0,
                    'xact_rollback': 0,
                    'read_operations': 0,
                    'write_operations': 0,
                    'cache_hit_ratio': 0
                }

            # Active sessions
            cur.execute("""
                SELECT
                    COUNT(*) as total_sessions,
                    COUNT(CASE WHEN state = 'active' THEN 1 END) as active_sessions,
                    COUNT(CASE WHEN wait_event IS NOT NULL THEN 1 END) as waiting_sessions
                FROM pg_stat_activity
                WHERE datname = 'tpc_tests'
            """)
            session_stats = cur.fetchone()
            if session_stats:
                metrics['sessions'] = {
                    'total': session_stats[0],
                    'active': session_stats[1],
                    'waiting': session_stats[2]
                }
            else:
                metrics['sessions'] = {
                    'total': 0,
                    'active': 0,
                    'waiting': 0
                }

            # Query statistics - using correct column names for PostgreSQL 15
            try:
                cur.execute("""
                    SELECT
                        COUNT(*) as total_queries,
                        AVG(total_exec_time) as avg_query_time,
                        MAX(total_exec_time) as max_query_time,
                        SUM(total_exec_time) as total_query_time
                    FROM pg_stat_statements
                """)
                query_stats = cur.fetchone()
                if query_stats and query_stats[0] > 0:
                    metrics['queries'] = {
                        'total_queries': query_stats[0],
                        'avg_query_time': query_stats[1] or 0,
                        'max_query_time': query_stats[2] or 0,
                        'total_query_time': query_stats[3] or 0
                    }
                else:
                    metrics['queries'] = {
                        'total_queries': 0,
                        'avg_query_time': 0,
                        'max_query_time': 0,
                        'total_query_time': 0
                    }
            except Exception as e:
                print(f"Warning: Could not collect query stats: {e}")
                metrics['queries'] = {
                    'total_queries': 0,
                    'avg_query_time': 0,
                    'max_query_time': 0,
                    'total_query_time': 0
                }

            conn.close()
            return metrics

        except Exception as e:
            print(f"Error collecting metrics: {e}")
            # Return default metrics structure
            return {
                'db_stats': {
                    'xact_commit': 0,
                    'xact_rollback': 0,
                    'read_operations': 0,
                    'write_operations': 0,
                    'cache_hit_ratio': 0
                },
                'sessions': {
                    'total': 0,
                    'active': 0,
                    'waiting': 0
                },
                'queries': {
                    'total_queries': 0,
                    'avg_query_time': 0,
                    'max_query_time': 0,
                    'total_query_time': 0
                }
            }

    def classify_workload(self, metrics):
        """Classify workload based on collected metrics"""
        if not metrics:
            return "UNKNOWN"

        # Calculate TPS (approximate)
        total_transactions = metrics['db_stats']['xact_commit'] + metrics['db_stats']['xact_rollback']
        tps = total_transactions / 60  # Assuming 1-minute window for test environment

        avg_latency = metrics['queries']['avg_query_time']
        active_sessions = metrics['sessions']['active']

        print(f"Classification metrics: TPS={tps:.2f}, Latency={avg_latency:.2f}ms, ActiveSessions={active_sessions}")

        # Classification logic
        if (tps >= self.thresholds['oltp']['tps_min'] and
            avg_latency <= self.thresholds['oltp']['avg_latency_max']):
            return "OLTP"

        elif (tps <= self.thresholds['olap']['tps_max'] and
              avg_latency >= self.thresholds['olap']['avg_latency_min']):
            return "OLAP"

        elif (self.thresholds['hybrid']['tps_range'][0] <= tps <= self.thresholds['hybrid']['tps_range'][1]):
            return "HYBRID"

        else:
            return "MIXED"

    def generate_report(self):
        """Generate complete classification report"""
        metrics = self.collect_metrics()
        profile = self.classify_workload(metrics)

        total_transactions = metrics['db_stats']['xact_commit'] + metrics['db_stats']['xact_rollback']
        tps = total_transactions / 60

        report = {
            'timestamp': datetime.now().isoformat(),
            'profile': profile,
            'metrics': metrics,
            'performance_indicators': {
                'tps': round(tps, 2),
                'avg_latency_ms': round(metrics['queries']['avg_query_time'], 2),
                'active_sessions': metrics['sessions']['active'],
                'cache_hit_ratio': round(metrics['db_stats']['cache_hit_ratio'], 4),
                'read_write_ratio': round(metrics['db_stats']['read_operations'] / (metrics['db_stats']['write_operations'] + 1), 2)
            },
            'classification_rules_applied': {
                'oltp_tps_min': self.thresholds['oltp']['tps_min'],
                'olap_tps_max': self.thresholds['olap']['tps_max'],
                'oltp_latency_max': self.thresholds['oltp']['avg_latency_max'],
                'olap_latency_min': self.thresholds['olap']['avg_latency_min']
            }
        }

        return report

def main():
    classifier = WorkloadClassifier()
    report = classifier.generate_report()

    print("\n" + "="*60)
    print("WORKLOAD CLASSIFICATION REPORT")
    print("="*60)
    print(f"Profile: {report['profile']}")
    print(f"TPS: {report['performance_indicators']['tps']}")
    print(f"Average Latency: {report['performance_indicators']['avg_latency_ms']} ms")
    print(f"Active Sessions: {report['performance_indicators']['active_sessions']}")
    print(f"Cache Hit Ratio: {report['performance_indicators']['cache_hit_ratio']:.2%}")
    print(f"Read/Write Ratio: {report['performance_indicators']['read_write_ratio']}")
    print("="*60)

    # Save report
    with open('/results/classification_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    print("\nReport saved to /results/classification_report.json")

if __name__ == "__main__":
    main()
