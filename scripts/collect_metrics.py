#!/usr/bin/env python3

import json
import subprocess
import psycopg2
import time
from datetime import datetime

class MetricsCollector:
    def __init__(self, host="postgres", user="postgres", password="password", database="tpc_tests"):
        self.conn_string = f"host={host} user={user} password={password} dbname={database}"
        self.metrics = []

    def collect_postgres_metrics(self):
        """Collect comprehensive PostgreSQL metrics"""
        try:
            conn = psycopg2.connect(self.conn_string)
            cur = conn.cursor()

            metrics = {
                'timestamp': datetime.now().isoformat(),
                'database': {},
                'statements': [],
                'activity': [],
                'table_stats': []
            }

            # Database-wide statistics
            cur.execute("""
                SELECT datname, numbackends, xact_commit, xact_rollback,
                       blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted,
                       tup_updated, tup_deleted
                FROM pg_stat_database
                WHERE datname = 'tpc_tests'
            """)
            metrics['database'] = dict(zip(
                [desc[0] for desc in cur.description],
                cur.fetchone()
            ))

            # Query statistics
            cur.execute("""
                SELECT query, calls, total_time, mean_time, rows,
                       100.0 * total_time / SUM(total_time) OVER() as percentage
                FROM pg_stat_statements
                ORDER BY total_time DESC
                LIMIT 20
            """)
            metrics['statements'] = [
                dict(zip([desc[0] for desc in cur.description], row))
                for row in cur.fetchall()
            ]

            # Current activity
            cur.execute("""
                SELECT datname, usename, state, query, wait_event_type, wait_event
                FROM pg_stat_activity
                WHERE datname = 'tpc_tests' AND state IS NOT NULL
            """)
            metrics['activity'] = [
                dict(zip([desc[0] for desc in cur.description], row))
                for row in cur.fetchall()
            ]

            conn.close()
            return metrics

        except Exception as e:
            print(f"Error collecting metrics: {e}")
            return {}

    def calculate_performance_metrics(self, scenario_name, duration):
        """Calculate TPS, latency, and throughput metrics"""
        conn = psycopg2.connect(self.conn_string)
        cur = conn.cursor()

        # Get transaction statistics
        cur.execute("""
            SELECT xact_commit, xact_rollback, blks_read, blks_hit
            FROM pg_stat_database
            WHERE datname = 'tpc_tests'
        """)
        stats = cur.fetchone()

        tps = (stats[0] + stats[1]) / duration if duration > 0 else 0
        hit_ratio = stats[3] / (stats[2] + stats[3]) if (stats[2] + stats[3]) > 0 else 1

        # Get average query latency
        cur.execute("""
            SELECT AVG(mean_time), PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY mean_time)
            FROM pg_stat_statements
            WHERE calls > 10
        """)
        latency_stats = cur.fetchone()

        performance_metrics = {
            'scenario': scenario_name,
            'timestamp': datetime.now().isoformat(),
            'tps': round(tps, 2),
            'transaction_throughput': stats[0] + stats[1],
            'avg_latency_ms': round(latency_stats[0] or 0, 2),
            'p95_latency_ms': round(latency_stats[1] or 0, 2),
            'cache_hit_ratio': round(hit_ratio, 4),
            'duration_seconds': duration
        }

        conn.close()
        return performance_metrics

    def generate_report(self, output_file="/results/reports/final_report.json"):
        """Generate comprehensive test report"""
        report = {
            'test_timestamp': datetime.now().isoformat(),
            'scenarios': [],
            'summary': {},
            'recommendations': []
        }

        # Aggregate all collected metrics
        for metrics in self.metrics:
            report['scenarios'].append(metrics)

        # Calculate summary statistics
        if self.metrics:
            report['summary'] = {
                'total_scenarios': len(self.metrics),
                'max_tps': max(m.get('tps', 0) for m in self.metrics),
                'min_latency': min(m.get('avg_latency_ms', 1000) for m in self.metrics),
                'avg_cache_hit_ratio': sum(m.get('cache_hit_ratio', 0) for m in self.metrics) / len(self.metrics)
            }

        # Generate recommendations based on metrics
        self._generate_recommendations(report)

        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"Report generated: {output_file}")
        return report

    def _generate_recommendations(self, report):
        """Generate tuning recommendations based on collected metrics"""
        recommendations = []

        for scenario in report['scenarios']:
            if scenario.get('avg_latency_ms', 0) > 100:
                recommendations.append({
                    'scenario': scenario.get('scenario'),
                    'issue': 'High query latency',
                    'recommendation': 'Increase work_mem and optimize queries with indexes'
                })

            if scenario.get('cache_hit_ratio', 1) < 0.9:
                recommendations.append({
                    'scenario': scenario.get('scenario'),
                    'issue': 'Low cache hit ratio',
                    'recommendation': 'Increase shared_buffers and consider more RAM'
                })

            if scenario.get('tps', 0) < 100 and 'OLTP' in scenario.get('scenario', ''):
                recommendations.append({
                    'scenario': scenario.get('scenario'),
                    'issue': 'Low TPS for OLTP workload',
                    'recommendation': 'Check for lock contention, optimize transactions'
                })

        report['recommendations'] = recommendations

def main():
    collector = MetricsCollector()

    # Collect baseline metrics
    baseline = collector.collect_postgres_metrics()

    # Simulate different scenarios and collect metrics
    scenarios = [
        ("OLTP_Workload", 300),
        ("OLAP_Workload", 180),
        ("Hybrid_Workload", 600),
        ("TPC-C_Benchmark", 600)
    ]

    for scenario_name, duration in scenarios:
        print(f"Collecting metrics for {scenario_name}...")

        # Simulate collection after workload execution
        time.sleep(2)  # In real implementation, this would be actual workload

        metrics = collector.calculate_performance_metrics(scenario_name, duration)
        collector.metrics.append(metrics)

    # Generate final report
    report = collector.generate_report()

    # Print summary
    print("\n=== TESTING SUMMARY ===")
    print(f"Scenarios executed: {report['summary']['total_scenarios']}")
    print(f"Maximum TPS: {report['summary']['max_tps']}")
    print(f"Minimum latency: {report['summary']['min_latency']}ms")
    print(f"Average cache hit ratio: {report['summary']['avg_cache_hit_ratio']:.2%}")

    print("\n=== RECOMMENDATIONS ===")
    for rec in report['recommendations']:
        print(f"{rec['scenario']}: {rec['issue']} -> {rec['recommendation']}")

if __name__ == "__main__":
    main()
