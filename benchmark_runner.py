import subprocess
import time
import re
import os
import tempfile
from datetime import datetime
import psycopg2
from config import DB_CONFIG

class BenchmarkRunner:
    def __init__(self, db_config):
        self.db_config = db_config
        self.container_name = "vtb_postgres"
        self.hammerdb_container = "vtb_hammerdb"

    def _copy_script_to_container(self, script_content, script_name="test.sql"):
        """
        Создает временный файл со скриптом и копирует его в контейнер.
        Это позволяет pgbench исполнять скрипт локально без сетевых задержек.
        """
        try:
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql') as tmp:
                tmp.write(script_content)
                tmp_path = tmp.name

            docker_dest = f"{self.container_name}:/tmp/{script_name}"
            subprocess.run(["docker", "cp", tmp_path, docker_dest], check=True)

            os.remove(tmp_path)

            return f"/tmp/{script_name}"
        except Exception as e:
            print(f" Error copying script to docker: {e}")
            return None

    def _run_pgbench_custom(self, script_path, duration, clients, threads, test_name):
        """
        Запускает pgbench внутри контейнера с указанным скриптом.
        """
        cmd = [
            "docker", "exec", "-i", self.container_name,
            "pgbench",
            "-U", "user",
            "-d", "mydb",
            "-T", str(duration),
            "-c", str(clients),
            "-j", str(threads),
            "-P", "5",
            "-f", script_path,
            "-r"
        ]

        print(f"🔧 Running {test_name}: pgbench -c {clients} -j {threads} -T {duration} ...")

        result = subprocess.run(cmd, capture_output=True, text=True)
        return result

    def run_oltp_test(self, profile_name, duration=30, clients=20):
        """
        Стандартный TPC-B подобный тест (чтение + запись в транзакции).
        Использует встроенный сценарий pgbench.
        """
        try:
            print(f" Starting OLTP test for {profile_name}...")

            self._initialize_pgbench(scale=10)

            cmd = [
                "docker", "exec", "-i", self.container_name,
                "pgbench",
                "-c", str(clients),
                "-j", "4",
                "-T", str(duration),
                "-U", "user", "mydb",
                "-r", "-P", "5"
            ]

            result = subprocess.run(cmd, capture_output=True, text=True)
            return self._process_results(result.stdout, profile_name, "OLTP", duration, clients)

        except Exception as e:
            return self._handle_error(e, profile_name)

    def run_olap_test(self, profile_name, duration=30):
        """
        Аналитическая нагрузка: сложные агрегации и JOIN'ы.
        """
        try:
            print(f" Starting OLAP test for {profile_name}...")
            self._create_olap_indexes()

            sql_script = """
            \set r random(1, 3)
            \if :r = 1
                -- Агрегация по всем счетам
                SELECT bid, count(*), avg(abalance) FROM pgbench_accounts GROUP BY bid;
            \elif :r = 2
                -- JOIN трех таблиц
                SELECT a.aid, b.bbalance, t.tbalance
                FROM pgbench_accounts a
                JOIN pgbench_branches b ON a.bid = b.bid
                JOIN pgbench_tellers t ON a.bid = t.bid
                WHERE a.abalance > 0 LIMIT 100;
            \else
                -- Оконные функции (если версия PG позволяет, иначе простой count)
                SELECT bid, sum(abalance) FROM pgbench_accounts GROUP BY bid ORDER BY sum(abalance) DESC LIMIT 5;
            \endif
            """

            script_path = self._copy_script_to_container(sql_script, "olap.sql")

            result = self._run_pgbench_custom(script_path, duration, clients=4, threads=2, test_name="OLAP")

            return self._process_results(result.stdout, profile_name, "OLAP", duration, 4)

        except Exception as e:
            return self._handle_error(e, profile_name)

    def run_disk_bound_olap_test(self, profile_name, duration=30):
        """
        Аналитическая нагрузка, упирающаяся в I/O: большие последовательные сканы.
        """
        try:
            print(f" Starting Disk-Bound OLAP test for {profile_name}...")
            self._create_disk_bound_table()

            # Сложный запрос с функцией, заставляющий делать последовательный скан
            sql_script = """
            -- Выполняем агрегацию по большому полю без индекса, чтобы убедиться в сканировании диска.
            SELECT count(*), max(random_data)
            FROM disk_bound_data
            WHERE random_data LIKE 'A%';
            """

            script_path = self._copy_script_to_container(sql_script, "disk_olap.sql")

            clients = 2 # Низкое число клиентов, чтобы тест был долгим
            threads = 1
            result = self._run_pgbench_custom(script_path, duration, clients, threads, test_name="DISK_OLAP")

            return self._process_results(result.stdout, profile_name, "DISK_OLAP", duration, clients)

        except Exception as e:
            return self._handle_error(e, profile_name)

    def run_iot_test(self, profile_name, duration=30):
        """
        IoT нагрузка: Максимально быстрая вставка мелких данных.
        """
        try:
            print(f" Starting IoT test for {profile_name}...")
            self._create_iot_tables()

            sql_script = """
            INSERT INTO iot_sensor_data (sensor_id, value, timestamp)
            VALUES (floor(random() * 1000)::int, random() * 100, NOW());

            INSERT INTO iot_metrics (device_id, metric_type, value, recorded_at)
            VALUES (floor(random() * 100)::int, 1, random() * 500, NOW());
            """

            script_path = self._copy_script_to_container(sql_script, "iot.sql")

            clients = 30
            threads = 4
            result = self._run_pgbench_custom(script_path, duration, clients, threads, test_name="IoT")

            return self._process_results(result.stdout, profile_name, "IoT", duration, clients)

        except Exception as e:
            return self._handle_error(e, profile_name)

    def run_mixed_test(self, profile_name, duration=30):
        """
        Смешанная нагрузка: Чтение (50%), Обновление (30%), Вставка (20%).
        """
        try:
            print(f" Starting Mixed test for {profile_name}...")
            self._initialize_pgbench(scale=5)

            sql_script = """
            \set r random(1, 100)
            \if :r <= 50
                -- 50% Read
                SELECT abalance FROM pgbench_accounts WHERE aid = :r;
            \elif :r <= 80
                -- 30% Update
                UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = :r;
            \else
                -- 20% Insert
                INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (1, 1, 1, 1, NOW());
            \endif
            """

            script_path = self._copy_script_to_container(sql_script, "mixed.sql")
            result = self._run_pgbench_custom(script_path, duration, clients=16, threads=4, test_name="Mixed")

            return self._process_results(result.stdout, profile_name, "Mixed", duration, 16)

        except Exception as e:
            return self._handle_error(e, profile_name)

    def run_read_only_test(self, profile_name, duration=30):
        """
        Тест только для чтения (Web / Read-Only): высокая скорость извлечения данных.
        """
        try:
            print(f" Starting Read-Only test for {profile_name}...")
            self._initialize_pgbench(scale=10)

            sql_script = """
            -- Выбираем случайную запись, имитируя чтение страницы/объекта
            SELECT abalance, filler FROM pgbench_accounts WHERE aid = :aid;
            """

            # Используем встроенный скрипт, но с высокой конкуренцией для проверки кеша
            script_path = self._copy_script_to_container(sql_script, "readonly.sql")

            clients = 50
            threads = 8
            result = self._run_pgbench_custom(script_path, duration, clients, threads, test_name="READ_ONLY")

            return self._process_results(result.stdout, profile_name, "READ_ONLY", duration, clients)

        except Exception as e:
            return self._handle_error(e, profile_name)

    def run_bulk_load_test(self, profile_name, duration=30):
        """
        Массовая заливка данных (Bulk Load): интенсивные INSERT'ы, нагрузка на WAL/Checkpoints.
        """
        try:
            print(f" Starting Bulk Load test for {profile_name}...")
            self._create_bulk_table()

            sql_script = """
            INSERT INTO bulk_data (col1, col2, col3)
            SELECT i, md5(random()::text), now() FROM generate_series(1, 100) AS s(i);
            """

            # Много клиентов, чтобы обеспечить высокую скорость записи
            script_path = self._copy_script_to_container(sql_script, "bulk_load.sql")

            clients = 40
            threads = 8
            result = self._run_pgbench_custom(script_path, duration, clients, threads, test_name="BULK_LOAD")

            # Очистка таблицы
            self._exec_sql("TRUNCATE TABLE bulk_data;")

            return self._process_results(result.stdout, profile_name, "BULK_LOAD", duration, clients)

        except Exception as e:
            return self._handle_error(e, profile_name)

    def run_tpcc_test(self, profile_name, duration=60):
        """
        Пытается запустить HammerDB. Если нет - мощная эмуляция через pgbench.
        """
        try:
            print(f" Starting TPC-C test for {profile_name}...")

            check = subprocess.run(["docker", "ps", "-q", "-f", f"name={self.hammerdb_container}"], capture_output=True, text=True)

            if check.stdout.strip():
                print(" Using HammerDB container...")
                cmd = ["docker", "exec", self.hammerdb_container, "hammerdbcli", "auto", "/hammerdb/run_tpcc.tcl"]
                result = subprocess.run(cmd, capture_output=True, text=True)
                tps, latency = self._parse_hammerdb_output(result.stdout)

                results = {
                    'profile': profile_name, 'test_type': 'TPC-C',
                    'tps': tps, 'tpm': tps * 60, 'avg_latency': latency,
                    'duration_minutes': round(duration/60, 2), 'clients': 4
                }
            else:
                print(" HammerDB not found. Running TPC-C simulation via pgbench...")

                sql_script = """
                BEGIN;
                -- Payment Transaction Logic
                UPDATE pgbench_branches SET bbalance = bbalance + 10 WHERE bid = :scale;
                UPDATE pgbench_tellers SET tbalance = tbalance + 10 WHERE tid = :scale;
                UPDATE pgbench_accounts SET abalance = abalance + 10 WHERE aid = :scale;
                INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (1, 1, 1, 10, NOW());
                -- New Order Check
                SELECT abalance FROM pgbench_accounts WHERE aid = :scale;
                COMMIT;
                """
                script_path = self._copy_script_to_container(sql_script, "tpcc_sim.sql")
                res = self._run_pgbench_custom(script_path, duration, clients=10, threads=2, test_name="TPC-C (Sim)")
                return self._process_results(res.stdout, profile_name, "TPC-C", duration, 10)

            self._save_results(results)
            print(f" TPC-C test completed: {results['tps']:.1f} TPS")
            return results

        except Exception as e:
            return self._handle_error(e, profile_name)


    def _process_results(self, stdout, profile_name, test_type, duration, clients):
        """Парсинг вывода pgbench и сохранение в БД"""
        tps, avg_latency = self._parse_pgbench_output(stdout)

        results = {
            'profile': profile_name,
            'test_type': test_type,
            'tps': round(tps, 2),
            'tpm': round(tps * 60, 2),
            'avg_latency': round(avg_latency, 2),
            'duration_minutes': round(duration / 60, 2),
            'clients': clients,
            'timestamp': datetime.now().isoformat()
        }

        self._save_results(results)
        print(f" {test_type} completed: {tps:.1f} TPS, {avg_latency:.2f}ms")
        return results

    def _initialize_pgbench(self, scale=5):
        """Инициализация pgbench таблиц, если они пусты"""
        try:
            check_cmd = ["docker", "exec", "-i", self.container_name, "psql", "-U", "user", "-d", "mydb", "-tAc", "SELECT count(*) FROM pgbench_accounts"]
            res = subprocess.run(check_cmd, capture_output=True, text=True)

            if res.returncode == 0 and res.stdout.strip().isdigit() and int(res.stdout.strip()) > 0:
                return

            print(f" Initializing pgbench (Scale {scale})...")
            init_cmd = ["docker", "exec", "-i", self.container_name, "pgbench", "-i", "-s", str(scale), "--foreign-keys", "-U", "user", "mydb"]
            subprocess.run(init_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass

    def _create_iot_tables(self):
        """Создает таблицы для IoT теста"""
        sql = """
        CREATE TABLE IF NOT EXISTS iot_sensor_data (id SERIAL, sensor_id INT, value DECIMAL, timestamp TIMESTAMP);
        CREATE TABLE IF NOT EXISTS iot_metrics (id SERIAL, device_id INT, metric_type INT, value DECIMAL, recorded_at TIMESTAMP);
        TRUNCATE TABLE iot_sensor_data; -- Очистка для чистоты теста
        """
        self._exec_sql(sql)

    def _create_olap_indexes(self):
        """Создает индексы для JOIN'ов"""
        sql = "CREATE INDEX IF NOT EXISTS idx_pgbench_accounts_bid ON pgbench_accounts(bid);"
        self._exec_sql(sql)

    def _create_bulk_table(self):
        """Создает таблицу для Bulk Load теста"""
        sql = """
        CREATE TABLE IF NOT EXISTS bulk_data (
            id BIGSERIAL PRIMARY KEY,
            col1 INT,
            col2 VARCHAR(32),
            col3 TIMESTAMP
        );
        """
        self._exec_sql(sql)

    def _create_disk_bound_table(self):
        """Создает большую таблицу без индексов для Disk-Bound OLAP теста"""
        sql = """
        CREATE TABLE IF NOT EXISTS disk_bound_data (
            id BIGSERIAL PRIMARY KEY,
            random_data VARCHAR(100) DEFAULT md5(random()::text),
            payload TEXT
        );

        -- Проверяем, если таблица пуста, заполняем ее (1М строк ~ 100MB)
        DO $$
        BEGIN
            IF (SELECT count(*) FROM disk_bound_data) < 1000000 THEN
                TRUNCATE TABLE disk_bound_data;
                INSERT INTO disk_bound_data (payload)
                SELECT repeat('X', 500)
                FROM generate_series(1, 1000000);
            END IF;
        END
        $$;

        DROP INDEX IF EXISTS disk_bound_data_random_data_idx; -- Убираем возможный индекс
        """
        self._exec_sql(sql)


    def _exec_sql(self, sql):
        cmd = ["docker", "exec", "-i", self.container_name, "psql", "-U", "user", "-d", "mydb", "-c", sql]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def _parse_pgbench_output(self, output):
        tps = 0.0
        latency = 0.0
        for line in output.splitlines():
            # tps = 1234.567890 (without initial connection time)
            if "tps =" in line:
                try:
                    parts = line.split("=")
                    tps = float(parts[1].split()[0])
                except: pass
            # latency average = 1.234 ms
            if "latency average =" in line:
                try:
                    parts = line.split("=")
                    latency = float(parts[1].split()[0])
                except: pass
        return tps, latency

    def _parse_hammerdb_output(self, output):
        tps = 0.0
        latency = 0.0
        if "tpmC" in output:
            try:
                tpm = float(re.search(r'tpmC\s*[:=]\s*(\d+\.?\d*)', output).group(1))
                tps = tpm / 60
            except: pass
        return tps, latency

    def _save_results(self, results):
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO benchmark_results
                (profile_name, test_type, tpm, nopm, avg_latency, tps, duration_minutes, clients)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                results.get('profile'), results.get('test_type'), results.get('tpm', 0),
                0, results.get('avg_latency', 0), results.get('tps', 0),
                results.get('duration_minutes'), results.get('clients')
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f" DB Save Error: {e}")

    def cleanup_failed_tests(self):
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            cur.execute("DELETE FROM benchmark_results WHERE tps IS NULL OR tps <= 0")
            count = cur.rowcount
            conn.commit()
            conn.close()
            return count
        except: return 0

    def get_comparison_report(self):
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            cur.execute("""
                SELECT profile_name, test_type, ROUND(AVG(tps), 2), ROUND(AVG(tpm), 2),
                       ROUND(AVG(avg_latency), 4), COUNT(*)
                FROM benchmark_results WHERE tps > 0
                GROUP BY profile_name, test_type ORDER BY AVG(tps) DESC
            """)
            return cur.fetchall()
        except: return []

    def _handle_error(self, e, profile):
        msg = f"Test failed: {str(e)}"
        print(f" {msg}")
        return {'error': msg, 'profile': profile}
