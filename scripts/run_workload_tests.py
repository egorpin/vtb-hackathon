#!/usr/bin/env python3

import subprocess
import time
import json
import psycopg2
from workload_classifier import WorkloadClassifier
from datetime import datetime

class WorkloadTestSuite:
    def __init__(self):
        self.classifier = WorkloadClassifier()
        self.conn_string = "host=postgres user=postgres password=password dbname=tpc_tests"
        self.test_results = []

    def run_test(self, test_name, workload_func, expected_profile, description):
        """Запуск отдельного теста с валидацией результатов"""
        print(f"\n{'='*60}")
        print(f"ТЕСТ: {test_name}")
        print(f"Ожидаемый профиль: {expected_profile}")
        print(f"Описание: {description}")
        print(f"{'='*60}")

        # Сбор baseline метрик
        baseline_metrics = self.classifier.collect_ash_metrics()

        # Запуск нагрузки
        start_time = time.time()
        workload_func()
        duration = time.time() - start_time

        # Сбор метрик после нагрузки
        post_metrics = self.classifier.collect_ash_metrics()
        indicators = self.classifier.calculate_workload_indicators(post_metrics)
        classification = self.classifier.get_detailed_classification(indicators)

        # Анализ перехода
        transition = self.classifier.analyze_workload_transition(baseline_metrics, post_metrics)

        # Валидация результата
        is_success = classification['profile'] == expected_profile
        confidence = classification['confidence']

        test_result = {
            'test_name': test_name,
            'timestamp': datetime.now().isoformat(),
            'expected_profile': expected_profile,
            'actual_profile': classification['profile'],
            'success': is_success,
            'confidence': confidence,
            'duration_seconds': duration,
            'indicators': indicators,
            'classification_details': classification,
            'transition': transition
        }

        self.test_results.append(test_result)

        # Вывод результатов
        status = "✅ УСПЕХ" if is_success else "❌ ОШИБКА"
        print(f"Результат: {status}")
        print(f"Уверенность: {confidence}%")
        print(f"Показатели: ASH={indicators['ash_ratio']:.3f}, "
              f"Committed={indicators['committed_ratio']:.3f}, "
              f"TPS={indicators['tps']:.1f}")

        return test_result

    def generate_test_report(self):
        """Генерация итогового отчета по всем тестам"""
        report = {
            'test_suite_timestamp': datetime.now().isoformat(),
            'total_tests': len(self.test_results),
            'passed_tests': sum(1 for r in self.test_results if r['success']),
            'failed_tests': sum(1 for r in self.test_results if not r['success']),
            'success_rate': len([r for r in self.test_results if r['success']]) / len(self.test_results) * 100,
            'tests': self.test_results,
            'summary_by_profile': {}
        }

        # Статистика по профилям
        profiles = {}
        for test in self.test_results:
            profile = test['actual_profile']
            if profile not in profiles:
                profiles[profile] = []
            profiles[profile].append(test)

        for profile, tests in profiles.items():
            report['summary_by_profile'][profile] = {
                'count': len(tests),
                'avg_confidence': sum(t['confidence'] for t in tests) / len(tests),
                'avg_tps': sum(t['indicators']['tps'] for t in tests) / len(tests),
                'avg_ash_ratio': sum(t['indicators']['ash_ratio'] for t in tests) / len(tests)
            }

        # Сохранение отчета
        with open('/results/test_suite_report.json', 'w') as f:
            json.dump(report, f, indent=2)

        return report

    def print_detailed_report(self):
        """Печать детального отчета"""
        report = self.generate_test_report()

        print(f"\n{'='*80}")
        print("ИТОГОВЫЙ ОТЧЕТ ТЕСТИРОВАНИЯ")
        print(f"{'='*80}")
        print(f"Всего тестов: {report['total_tests']}")
        print(f"Успешных: {report['passed_tests']}")
        print(f"Проваленных: {report['failed_tests']}")
        print(f"Успешность: {report['success_rate']:.1f}%")

        print(f"\nСтатистика по профилям:")
        for profile, stats in report['summary_by_profile'].items():
            print(f"  {profile}: {stats['count']} тестов, "
                  f"средняя уверенность: {stats['avg_confidence']:.1f}%, "
                  f"средний TPS: {stats['avg_tps']:.1f}")

# Тестовые нагрузки
class WorkloadGenerators:
    def __init__(self):
        self.conn_string = "host=postgres user=postgres password=password dbname=tpc_tests"

    def setup_test_data(self):
        """Настройка тестовых данных"""
        conn = psycopg2.connect(self.conn_string)
        cur = conn.cursor()

        # Создание тестовых таблиц
        cur.execute("""
            CREATE TABLE IF NOT EXISTS test_orders (
                order_id SERIAL PRIMARY KEY,
                customer_id INTEGER,
                order_total DECIMAL(10,2),
                order_date TIMESTAMP,
                status VARCHAR(20)
            );

            CREATE TABLE IF NOT EXISTS test_order_items (
                item_id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES test_orders(order_id),
                product_id INTEGER,
                quantity INTEGER,
                price DECIMAL(10,2)
            );

            CREATE TABLE IF NOT EXISTS test_customers (
                customer_id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_date TIMESTAMP
            );
        """)

        # Генерация тестовых данных
        print("Генерация тестовых данных...")
        for i in range(1000):
            cur.execute("""
                INSERT INTO test_customers (name, email, created_date)
                VALUES (%s, %s, NOW())
            """, (f"Customer_{i}", f"customer_{i}@test.com"))

        conn.commit()
        conn.close()

    def oltp_workload(self):
        """Генерация OLTP нагрузки - короткие транзакции"""
        conn = psycopg2.connect(self.conn_string)
        cur = conn.cursor()

        # Имитация банковских транзакций
        for i in range(500):
            try:
                cur.execute("BEGIN")

                # Вставка нового заказа
                cur.execute("""
                    INSERT INTO test_orders (customer_id, order_total, order_date, status)
                    VALUES (%s, %s, NOW(), 'pending')
                    RETURNING order_id
                """, (i % 100 + 1, 100.00 + (i % 50)))

                order_id = cur.fetchone()[0]

                # Вставка элементов заказа
                for j in range(3):
                    cur.execute("""
                        INSERT INTO test_order_items (order_id, product_id, quantity, price)
                        VALUES (%s, %s, %s, %s)
                    """, (order_id, j + 1, (i + j) % 5 + 1, 25.00 + j * 10))

                # Обновление статуса
                cur.execute("""
                    UPDATE test_orders
                    SET status = 'completed', order_total = %s
                    WHERE order_id = %s
                """, (150.00 + (i % 30), order_id))

                cur.execute("COMMIT")

            except Exception as e:
                cur.execute("ROLLBACK")
                print(f"OLTP transaction error: {e}")

        conn.close()

    def olap_workload(self):
        """Генерация OLAP нагрузки - тяжелые аналитические запросы"""
        conn = psycopg2.connect(self.conn_string)
        cur = conn.cursor()

        # Сложные аналитические запросы
        queries = [
            # Анализ клиентской активности с оконными функциями
            """
            WITH customer_analysis AS (
                SELECT
                    c.customer_id,
                    c.name,
                    COUNT(o.order_id) as order_count,
                    SUM(o.order_total) as total_spent,
                    AVG(o.order_total) as avg_order,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.order_total) as median_order,
                    MAX(o.order_date) as last_order_date,
                    NTILE(4) OVER (ORDER BY SUM(o.order_total) DESC) as spending_quartile
                FROM test_customers c
                LEFT JOIN test_orders o ON c.customer_id = o.customer_id
                GROUP BY c.customer_id, c.name
            )
            SELECT
                spending_quartile,
                COUNT(*) as customers,
                AVG(total_spent) as avg_total_spent,
                AVG(order_count) as avg_orders
            FROM customer_analysis
            GROUP BY spending_quartile
            ORDER BY spending_quartile
            """,

            # Временной анализ с агрегациями
            """
            SELECT
                DATE_TRUNC('hour', order_date) as hour_bucket,
                COUNT(*) as orders_per_hour,
                SUM(order_total) as revenue_per_hour,
                AVG(order_total) as avg_order_size,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM test_orders
            WHERE order_date > NOW() - INTERVAL '1 week'
            GROUP BY hour_bucket
            ORDER BY hour_bucket
            """,

            # Анализ продуктовой корзины
            """
            SELECT
                c.customer_id,
                c.name,
                STRING_AGG(DISTINCT oi.product_id::TEXT, ', ') as purchased_products,
                COUNT(DISTINCT oi.product_id) as unique_products,
                SUM(oi.quantity * oi.price) as total_product_spend
            FROM test_customers c
            JOIN test_orders o ON c.customer_id = o.customer_id
            JOIN test_order_items oi ON o.order_id = oi.order_id
            GROUP BY c.customer_id, c.name
            HAVING COUNT(DISTINCT oi.product_id) > 2
            ORDER BY total_product_spend DESC
            LIMIT 100
            """
        ]

        for query in queries:
            try:
                start_time = time.time()
                cur.execute(query)
                results = cur.fetchall()
                execution_time = time.time() - start_time
                print(f"OLAP query executed in {execution_time:.2f}s, returned {len(results)} rows")
            except Exception as e:
                print(f"OLAP query error: {e}")

        conn.close()

    def batch_workload(self):
        """Генерация BATCH нагрузки - массовые операции"""
        conn = psycopg2.connect(self.conn_string)
        cur = conn.cursor()

        # Массовое обновление данных
        print("Запуск BATCH операций...")

        # Операция 1: Массовое обновление цен
        cur.execute("""
            UPDATE test_order_items
            SET price = price * 1.1
            WHERE product_id IN (1, 2, 3)
        """)
        print(f"Обновлено {cur.rowcount} записей цен")

        # Операция 2: Архивация старых заказов
        cur.execute("""
            DELETE FROM test_orders
            WHERE order_date < NOW() - INTERVAL '1 year'
        """)
        print(f"Архивировано {cur.rowcount} старых заказов")

        # Операция 3: Генерация агрегированных данных
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customer_summary AS
            SELECT
                customer_id,
                COUNT(*) as total_orders,
                SUM(order_total) as lifetime_value,
                AVG(order_total) as avg_order_value,
                MAX(order_date) as last_order_date
            FROM test_orders
            GROUP BY customer_id
        """)

        conn.commit()
        conn.close()

    def mixed_workload(self):
        """Генерация смешанной нагрузки"""
        import threading

        def run_oltp_segment():
            """OLTP сегмент смешанной нагрузки"""
            self.oltp_workload()

        def run_olap_segment():
            """OLAP сегмент смешанной нагрузки"""
            self.olap_workload()

        # Запуск в параллельных потоках
        oltp_thread = threading.Thread(target=run_oltp_segment)
        olap_thread = threading.Thread(target=run_olap_segment)

        oltp_thread.start()
        time.sleep(2)  # Задержка для имитации реальной нагрузки
        olap_thread.start()

        oltp_thread.join()
        olap_thread.join()

    def tpcc_like_workload(self):
        """Имитация TPC-C подобной нагрузки"""
        conn = psycopg2.connect(self.conn_string)
        cur = conn.cursor()

        # Смесь транзакций разных типов как в TPC-C
        transaction_types = [
            # New Order (45%)
            lambda: self._tpcc_new_order(cur),
            # Payment (43%)
            lambda: self._tpcc_payment(cur),
            # Order Status (4%)
            lambda: self._tpcc_order_status(cur),
            # Delivery (4%)
            lambda: self._tpcc_delivery(cur),
            # Stock Level (4%)
            lambda: self._tpcc_stock_level(cur)
        ]

        weights = [45, 43, 4, 4, 4]  # Веса в процентах

        for i in range(200):  # 200 транзакций
            # Выбор типа транзакции по весу
            import random
            trans_type = random.choices(transaction_types, weights=weights)[0]
            try:
                trans_type()
            except Exception as e:
                print(f"TPC-C transaction error: {e}")
                conn.rollback()

        conn.close()

    def _tpcc_new_order(self, cur):
        """TPC-C New Order транзакция"""
        cur.execute("BEGIN")

        customer_id = random.randint(1, 100)

        # Создание заказа
        cur.execute("""
            INSERT INTO test_orders (customer_id, order_total, order_date, status)
            VALUES (%s, %s, NOW(), 'new')
            RETURNING order_id
        """, (customer_id, 100.00))

        order_id = cur.fetchone()[0]

        # Добавление товаров
        for i in range(random.randint(1, 5)):
            cur.execute("""
                INSERT INTO test_order_items (order_id, product_id, quantity, price)
                VALUES (%s, %s, %s, %s)
            """, (order_id, random.randint(1, 50), random.randint(1, 10), 25.00))

        cur.execute("COMMIT")

    def _tpcc_payment(self, cur):
        """TPC-C Payment транзакция"""
        cur.execute("BEGIN")

        customer_id = random.randint(1, 100)
        payment_amount = random.uniform(10, 500)

        # Обновление баланса (упрощенно)
        cur.execute("""
            UPDATE test_orders
            SET order_total = order_total - %s
            WHERE customer_id = %s AND status = 'completed'
        """, (payment_amount, customer_id))

        cur.execute("COMMIT")

    def _tpcc_order_status(self, cur):
        """TPC-C Order Status транзакция"""
        cur.execute("""
            SELECT o.order_id, o.status, o.order_date, COUNT(oi.item_id) as item_count
            FROM test_orders o
            LEFT JOIN test_order_items oi ON o.order_id = oi.order_id
            WHERE o.customer_id = %s
            GROUP BY o.order_id, o.status, o.order_date
            ORDER BY o.order_date DESC
            LIMIT 5
        """, (random.randint(1, 100),))

        cur.fetchall()

    def _tpcc_delivery(self, cur):
        """TPC-C Delivery транзакция"""
        cur.execute("BEGIN")

        # Обновление статуса доставки
        cur.execute("""
            UPDATE test_orders
            SET status = 'delivered'
            WHERE status = 'completed'
            AND order_date < NOW() - INTERVAL '1 day'
            LIMIT 10
        """)

        cur.execute("COMMIT")

    def _tpcc_stock_level(self, cur):
        """TPC-C Stock Level транзакция"""
        cur.execute("""
            SELECT product_id, COUNT(*) as order_count
            FROM test_order_items
            WHERE order_id IN (
                SELECT order_id FROM test_orders
                WHERE order_date > NOW() - INTERVAL '1 week'
            )
            GROUP BY product_id
            ORDER BY order_count DESC
            LIMIT 10
        """)

        cur.fetchall()

# Запуск тестов
def main():
    # Инициализация
    test_suite = WorkloadTestSuite()
    workload_gen = WorkloadGenerators()

    print("Подготовка тестовых данных...")
    workload_gen.setup_test_data()

    # Запуск отдельных тестов
    tests = [
        {
            'name': 'OLTP Профиль',
            'workload': workload_gen.oltp_workload,
            'expected': 'OLTP',
            'description': 'Короткие транзакции, высокая частота, низкая задержка'
        },
        {
            'name': 'OLAP Профиль',
            'workload': workload_gen.olap_workload,
            'expected': 'OLAP',
            'description': 'Тяжелые аналитические запросы, сканирования, агрегации'
        },
        {
            'name': 'BATCH Профиль',
            'workload': workload_gen.batch_workload,
            'expected': 'BATCH',
            'description': 'Массовые операции, преобладание записи, длительные транзакции'
        },
        {
            'name': 'HYBRID Профиль',
            'workload': workload_gen.mixed_workload,
            'expected': 'HYBRID',
            'description': 'Смесь OLTP и OLAP нагрузки, сбалансированные показатели'
        },
        {
            'name': 'TPC-C Профиль',
            'workload': workload_gen.tpcc_like_workload,
            'expected': 'TPC-C',
            'description': 'Стандартизированная смешанная нагрузка по спецификации TPC-C'
        }
    ]

    # Выполнение всех тестов
    for test_config in tests:
        test_suite.run_test(
            test_config['name'],
            test_config['workload'],
            test_config['expected'],
            test_config['description']
        )

    # Генерация финального отчета
    test_suite.print_detailed_report()

if __name__ == "__main__":
    import random
    main()
