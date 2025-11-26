import subprocess
import time
import re
from datetime import datetime
import psycopg2
from config import DB_CONFIG

class BenchmarkRunner:
    def __init__(self, db_config):
        self.db_config = db_config

    def run_oltp_test(self, profile_name, clients=8, duration=30):
        """Тестировщик для OLTP нагрузки"""
        try:
            print(f"🚀 Starting OLTP test for {profile_name}...")

            # Принудительная инициализация pgbench
            self._initialize_pgbench(scale=10)

            # Запуск OLTP теста
            run_cmd = [
                "docker", "exec", "-i", "vtb_postgres",
                "pgbench", "-c", str(clients), "-j", "2", "-T", str(duration),
                "-U", "user", "mydb", "-r", "-P", "2"
            ]

            print(f"🔧 Running: {' '.join(run_cmd)}")
            result = subprocess.run(run_cmd, capture_output=True, text=True)

            # Парсим результаты
            tps, avg_latency = self._parse_pgbench_output(result.stdout)
            tpm = tps * 60

            results = {
                'profile': profile_name,
                'test_type': 'OLTP',
                'tps': round(tps, 2),
                'tpm': round(tpm, 2),
                'avg_latency': round(avg_latency, 2),
                'duration_minutes': round(duration / 60, 2),
                'clients': clients,
                'timestamp': datetime.now().isoformat()
            }

            self._save_results(results)
            print(f"✅ OLTP test completed: {tps:.1f} TPS, {avg_latency:.2f}ms latency")
            return results

        except Exception as e:
            error_msg = f"OLTP test failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {'error': error_msg, 'profile': profile_name}

    def run_olap_test(self, profile_name, duration=30):
        """Тестировщик для OLAP нагрузки - аналитические запросы"""
        try:
            print(f"🚀 Starting OLAP test for {profile_name}...")

            # Создаем тестовые данные для аналитики
            self._create_olap_test_data()

            container = "vtb_postgres"
            heavy_queries = [
                # Тяжелый аналитический запрос 1
                """
                SELECT bid, count(*) as account_count, avg(abalance) as avg_balance,
                       sum(abalance) as total_balance
                FROM pgbench_accounts
                GROUP BY bid
                ORDER BY total_balance DESC;
                """,
                # Тяжелый аналитический запрос 2
                """
                SELECT a.aid, b.bbalance, t.tbalance, a.abalance,
                       (a.abalance + b.bbalance + t.tbalance) as total
                FROM pgbench_accounts a
                JOIN pgbench_branches b ON a.bid = b.bid
                JOIN pgbench_tellers t ON a.bid = t.bid
                WHERE a.abalance > 0
                ORDER BY total DESC
                LIMIT 1000;
                """,
                # Тяжелый аналитический запрос 3
                """
                WITH account_stats AS (
                    SELECT bid,
                           count(*) as cnt,
                           avg(abalance) as avg_bal,
                           stddev(abalance) as std_bal
                    FROM pgbench_accounts
                    GROUP BY bid
                )
                SELECT b.bid, b.bbalance, a.cnt, a.avg_bal, a.std_bal
                FROM pgbench_branches b
                JOIN account_stats a ON b.bid = a.bid
                ORDER BY a.avg_bal DESC;
                """
            ]

            start_time = time.time()
            completed_queries = 0
            total_latency = 0.0

            while time.time() - start_time < duration:
                for i, query in enumerate(heavy_queries):
                    if time.time() - start_time >= duration:
                        break

                    cmd = ["docker", "exec", "-i", container, "psql", "-U", "user", "-d", "mydb", "-c", query]

                    query_start = time.time()
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    query_latency = (time.time() - query_start) * 1000  # в ms

                    if result.returncode == 0:
                        completed_queries += 1
                        total_latency += query_latency
                    else:
                        print(f"Query {i+1} failed: {result.stderr}")

                    # Пауза между запросами
                    time.sleep(0.5)

            actual_duration = time.time() - start_time
            qps = completed_queries / actual_duration if actual_duration > 0 else 0
            avg_latency = total_latency / completed_queries if completed_queries > 0 else 0

            results = {
                'profile': profile_name,
                'test_type': 'OLAP',
                'tps': round(qps, 2),  # Queries per second
                'tpm': round(qps * 60, 2),
                'avg_latency': round(avg_latency, 2),
                'duration_minutes': round(actual_duration / 60, 2),
                'clients': 1,  # OLAP обычно single-threaded
                'timestamp': datetime.now().isoformat()
            }

            self._save_results(results)
            print(f"✅ OLAP test completed: {qps:.1f} QPS, {avg_latency:.2f}ms latency")
            return results

        except Exception as e:
            error_msg = f"OLAP test failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {'error': error_msg, 'profile': profile_name}

    def run_iot_test(self, profile_name, duration=30):
        """Тестировщик для IoT нагрузки - интенсивная запись"""
        try:
            print(f"🚀 Starting IoT test for {profile_name}...")

            # Создаем таблицу для IoT тестов если нужно
            self._create_iot_test_table()

            container = "vtb_postgres"

            # Оптимизированные запросы для IoT
            insert_queries = [
                # Быстрая вставка 1
                """
                INSERT INTO iot_sensor_data
                (sensor_id, value, timestamp)
                VALUES (
                    floor(random() * 1000)::int,
                    random() * 100,
                    NOW() - (random() * interval '1 day')
                );
                """,
                # Быстрая вставка 2
                """
                INSERT INTO iot_metrics
                (device_id, metric_type, value, recorded_at)
                VALUES (
                    floor(random() * 100)::int,
                    floor(random() * 10)::int,
                    random() * 1000,
                    NOW()
                );
                """
            ]

            start_time = time.time()
            completed_inserts = 0
            total_latency = 0.0

            while time.time() - start_time < duration:
                for i, query in enumerate(insert_queries):
                    if time.time() - start_time >= duration:
                        break

                    cmd = ["docker", "exec", "-i", container, "psql", "-U", "user", "-d", "mydb", "-c", query]

                    insert_start = time.time()
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    insert_latency = (time.time() - insert_start) * 1000  # в ms

                    if result.returncode == 0:
                        completed_inserts += 1
                        total_latency += insert_latency
                    else:
                        print(f"Insert {i+1} failed: {result.stderr}")

                    # Минимальная пауза для максимальной производительности
                    time.sleep(0.01)

            actual_duration = time.time() - start_time
            ips = completed_inserts / actual_duration if actual_duration > 0 else 0
            avg_latency = total_latency / completed_inserts if completed_inserts > 0 else 0

            results = {
                'profile': profile_name,
                'test_type': 'IoT',
                'tps': round(ips, 2),  # Inserts per second
                'tpm': round(ips * 60, 2),
                'avg_latency': round(avg_latency, 2),
                'duration_minutes': round(actual_duration / 60, 2),
                'clients': 1,
                'timestamp': datetime.now().isoformat()
            }

            self._save_results(results)
            print(f"✅ IoT test completed: {ips:.1f} IPS, {avg_latency:.2f}ms latency")
            return results

        except Exception as e:
            error_msg = f"IoT test failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {'error': error_msg, 'profile': profile_name}

    def run_mixed_test(self, profile_name, duration=30):
        """Тестировщик для смешанной нагрузки"""
        try:
            print(f"🚀 Starting Mixed test for {profile_name}...")

            # Инициализация для mixed теста
            self._initialize_pgbench(scale=5)

            container = "vtb_postgres"

            # Смешанные запросы: чтение + запись + аналитика
            mixed_queries = [
                # OLTP-like: короткие транзакции
                "UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = 1;",
                # OLAP-like: аналитические запросы
                "SELECT count(*), avg(abalance) FROM pgbench_accounts WHERE bid = 1;",
                # IoT-like: вставки
                "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (1, 1, 1, 1, NOW());",
                # Чтение
                "SELECT abalance FROM pgbench_accounts WHERE aid = 1;"
            ]

            start_time = time.time()
            completed_operations = 0
            total_latency = 0.0

            while time.time() - start_time < duration:
                for i, query in enumerate(mixed_queries):
                    if time.time() - start_time >= duration:
                        break

                    cmd = ["docker", "exec", "-i", container, "psql", "-U", "user", "-d", "mydb", "-c", query]

                    op_start = time.time()
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    op_latency = (time.time() - op_start) * 1000  # в ms

                    if result.returncode == 0:
                        completed_operations += 1
                        total_latency += op_latency
                    else:
                        print(f"Operation {i+1} failed: {result.stderr}")

                    time.sleep(0.1)

            actual_duration = time.time() - start_time
            ops = completed_operations / actual_duration if actual_duration > 0 else 0
            avg_latency = total_latency / completed_operations if completed_operations > 0 else 0

            results = {
                'profile': profile_name,
                'test_type': 'Mixed',
                'tps': round(ops, 2),  # Operations per second
                'tpm': round(ops * 60, 2),
                'avg_latency': round(avg_latency, 2),
                'duration_minutes': round(actual_duration / 60, 2),
                'clients': 1,
                'timestamp': datetime.now().isoformat()
            }

            self._save_results(results)
            print(f"✅ Mixed test completed: {ops:.1f} OPS, {avg_latency:.2f}ms latency")
            return results

        except Exception as e:
            error_msg = f"Mixed test failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {'error': error_msg, 'profile': profile_name}

    def _initialize_pgbench(self, scale=5):
        """Надежная инициализация pgbench"""
        try:
            print("🔄 Initializing pgbench...")

            # Сначала проверяем, существует ли база
            check_cmd = [
                "docker", "exec", "-i", "vtb_postgres",
                "psql", "-U", "user", "-d", "mydb", "-c", "SELECT 1;"
            ]
            subprocess.run(check_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            # Принудительно переинициализируем pgbench
            init_cmd = [
                "docker", "exec", "-i", "vtb_postgres",
                "pgbench", "-i", "-s", str(scale), "-U", "user", "mydb"
            ]

            result = subprocess.run(init_cmd, capture_output=True, text=True)

            if result.returncode != 0:
                if "already exists" not in result.stderr:
                    print(f"⚠️  Init warning: {result.stderr}")
                # Все равно продолжаем, т.к. таблицы могут существовать
            else:
                print("✅ Pgbench initialized successfully")

        except Exception as e:
            print(f"❌ Pgbench initialization failed: {e}")

    def _create_olap_test_data(self):
        """Создает дополнительные данные для OLAP тестов"""
        try:
            container = "vtb_postgres"

            # Добавляем индексы для ускорения аналитических запросов
            index_queries = [
                "CREATE INDEX IF NOT EXISTS idx_accounts_bid ON pgbench_accounts(bid);",
                "CREATE INDEX IF NOT EXISTS idx_accounts_balance ON pgbench_accounts(abalance);",
                "CREATE INDEX IF NOT EXISTS idx_history_mtime ON pgbench_history(mtime);"
            ]

            for query in index_queries:
                cmd = ["docker", "exec", "-i", container, "psql", "-U", "user", "-d", "mydb", "-c", query]
                subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            print("✅ OLAP test data prepared")

        except Exception as e:
            print(f"❌ OLAP data preparation failed: {e}")

    def _create_iot_test_table(self):
        """Создает таблицы для IoT тестов"""
        try:
            container = "vtb_postgres"

            create_tables = [
                """
                CREATE TABLE IF NOT EXISTS iot_sensor_data (
                    id SERIAL PRIMARY KEY,
                    sensor_id INTEGER,
                    value DECIMAL(10,2),
                    timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS iot_metrics (
                    id SERIAL PRIMARY KEY,
                    device_id INTEGER,
                    metric_type INTEGER,
                    value DECIMAL(10,2),
                    recorded_at TIMESTAMP DEFAULT NOW()
                );
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_sensor_timestamp ON iot_sensor_data(timestamp);
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_metrics_recorded ON iot_metrics(recorded_at);
                """
            ]

            for query in create_tables:
                cmd = ["docker", "exec", "-i", container, "psql", "-U", "user", "-d", "mydb", "-c", query]
                subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            print("✅ IoT test tables created")

        except Exception as e:
            print(f"❌ IoT table creation failed: {e}")

    def _parse_pgbench_output(self, output):
        """Парсит вывод pgbench"""
        tps = 0.0
        avg_latency = 0.0

        # Основные паттерны для TPS
        tps_patterns = [
            r'tps = (\d+\.\d+) \(without initial connection time\)',
            r'tps = (\d+\.\d+) \(including connections establishing\)',
            r'tps = (\d+\.\d+)',
        ]

        for pattern in tps_patterns:
            match = re.search(pattern, output)
            if match:
                tps = float(match.group(1))
                break

        # Паттерны для latency
        latency_patterns = [
            r'latency average = (\d+\.\d+) ms',
            r'avg latency\s*=\s*(\d+\.\d+) ms',
        ]

        for pattern in latency_patterns:
            match = re.search(pattern, output)
            if match:
                avg_latency = float(match.group(1))
                break

        return tps, avg_latency

    def _save_results(self, results):
        """Сохраняет результаты в БД"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO benchmark_results
                (profile_name, test_type, tpm, nopm, avg_latency, tps, duration_minutes, clients)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                results.get('profile'),
                results.get('test_type'),
                results.get('tpm', 0),
                results.get('nopm', 0),
                results.get('avg_latency', 0),
                results.get('tps', 0),
                results.get('duration_minutes', 0),
                results.get('clients', 0)
            ))

            conn.commit()
            conn.close()
            print(f"💾 Results saved for {results.get('profile')}")

        except Exception as e:
            print(f"❌ Error saving results: {e}")

    def get_comparison_report(self):
        """Генерирует отчет сравнения профилей"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()

            cur.execute("""
                SELECT
                    profile_name,
                    test_type,
                    ROUND(AVG(COALESCE(tps, 0)), 2) as avg_tps,
                    ROUND(AVG(COALESCE(tpm, 0)), 2) as avg_tpm,
                    ROUND(AVG(COALESCE(avg_latency, 0)), 4) as avg_latency,
                    COUNT(*) as test_count
                FROM benchmark_results
                WHERE tps > 0
                GROUP BY profile_name, test_type
                ORDER BY avg_tps DESC
            """)

            results = cur.fetchall()
            conn.close()

            return results

        except Exception as e:
            print(f"❌ Error generating report: {e}")
            return []

    def cleanup_failed_tests(self):
        """Удаляет записи тестов с ошибками"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()

            cur.execute("""
                DELETE FROM benchmark_results
                WHERE tps IS NULL OR tps <= 0
            """)

            deleted_count = cur.rowcount
            conn.commit()
            conn.close()

            print(f"🧹 Cleaned up {deleted_count} failed test records")
            return deleted_count

        except Exception as e:
            print(f"❌ Error cleaning up failed tests: {e}")
            return 0
