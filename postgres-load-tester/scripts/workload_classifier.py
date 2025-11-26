#!/usr/bin/env python3

import json
import psycopg2
from datetime import datetime

class WorkloadClassifier:
    def __init__(self, host="postgres", user="postgres", password=None, database="tpc_tests"):
        # Connect without password since we're using trust authentication
        self.conn_string = f"host={host} user={user} dbname={database}"
        
        # Thresholds for classification
        self.thresholds = {
            'oltp': {
                'tps_min': 1,
                'avg_latency_max': 100,
                'active_sessions_min': 1
            },
            'olap': {
                'tps_max': 0.5,
                'avg_latency_min': 100,
                'active_sessions_max': 2
            },
            'hybrid': {
                'tps_range': (0.5, 2),
                'active_sessions_range': (1, 3)
            }
        }

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
