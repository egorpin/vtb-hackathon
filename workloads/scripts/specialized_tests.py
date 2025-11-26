#!/usr/bin/env python3

import time
import psycopg2
from workload_classifier import WorkloadClassifier

class SpecializedTests:
    def __init__(self):
        self.classifier = WorkloadClassifier()
        self.conn_string = "host=postgres user=postgres password=password dbname=tpc_tests"

    def test_workload_transition(self):
        """Тест перехода между профилями нагрузки"""
        print("\n🔀 ТЕСТ ПЕРЕХОДА МЕЖДУ ПРОФИЛЯМИ")

        # Начальное состояние - без нагрузки
        baseline = self.classifier.collect_ash_metrics()
        print("Начальное состояние собрано")

        # Добавляем OLTP нагрузку
        self._generate_oltp_transition()
        oltp_metrics = self.classifier.collect_ash_metrics()
        oltp_transition = self.classifier.analyze_workload_transition(baseline, oltp_metrics)
        print(f"После OLTP: {oltp_transition['from']} -> {oltp_transition['to']}")

        # Добавляем OLAP поверх OLTP
        self._generate_olap_transition()
        hybrid_metrics = self.classifier.collect_ash_metrics()
        hybrid_transition = self.classifier.analyze_workload_transition(oltp_metrics, hybrid_metrics)
        print(f"После добавления OLAP: {hybrid_transition['from']} -> {hybrid_transition['to']}")

        # Убираем OLTP, оставляем OLAP
        time.sleep(5)  # Даем успокоиться
        olap_metrics = self.classifier.collect_ash_metrics()
        olap_transition = self.classifier.analyze_workload_transition(hybrid_metrics, olap_metrics)
        print(f"После снятия OLTP: {olap_transition['from']} -> {olap_transition['to']}")

        return {
            'oltp_transition': oltp_transition,
            'hybrid_transition': hybrid_transition,
            'olap_transition': olap_transition
        }

    def test_threshold_boundaries(self):
        """Тест граничных условий классификации"""
        print("\n🎯 ТЕСТ ГРАНИЧНЫХ УСЛОВИЙ")

        test_cases = [
            {'name': 'Нижняя граница OLTP', 'tps': 500, 'latency': 45, 'expected': 'OLTP'},
            {'name': 'Верхняя граница OLTP', 'tps': 50, 'latency': 55, 'expected': 'HYBRID'},
            {'name': 'Нижняя граница OLAP', 'tps': 40, 'latency': 1100, 'expected': 'OLAP'},
            {'name': 'Типичный HYBRID', 'tps': 800, 'latency': 150, 'expected': 'HYBRID'},
        ]

        results = []
        for test_case in test_cases:
            # Создаем синтетические метрики для теста
            synthetic_metrics = self._create_synthetic_metrics(
                test_case['tps'],
                test_case['latency']
            )

            indicators = self.classifier.calculate_workload_indicators(synthetic_metrics)
            classification = self.classifier.classify_workload(indicators)

            is_correct = classification == test_case['expected']
            results.append({
                'test_case': test_case['name'],
                'expected': test_case['expected'],
                'actual': classification,
                'success': is_correct,
                'tps': test_case['tps'],
                'latency': test_case['latency']
            })

            status = "✅" if is_correct else "❌"
            print(f"{status} {test_case['name']}: {classification} (ожидалось: {test_case['expected']})")

        return results

    def test_parameter_sensitivity(self):
        """Тест чувствительности к параметрам конфигурации"""
        print("\n⚙️ ТЕСТ ЧУВСТВИТЕЛЬНОСТИ К ПАРАМЕТРАМ")

        # Тестируем разные конфигурации PostgreSQL
        configs = [
            {'name': 'OLTP конфиг', 'work_mem': '4MB', 'shared_buffers': '256MB', 'expected_profile': 'OLTP'},
            {'name': 'OLAP конфиг', 'work_mem': '64MB', 'shared_buffers': '1GB', 'expected_profile': 'OLAP'},
            {'name': 'HYBRID конфиг', 'work_mem': '16MB', 'shared_buffers': '512MB', 'expected_profile': 'HYBRID'},
        ]

        results = []
        for config in configs:
            print(f"Тестирование конфигурации: {config['name']}")

            # Здесь должна быть логика применения конфигурации
            # Для демонстрации используем фиктивные данные

            # Запускаем стандартную OLTP нагрузку
            self._generate_oltp_transition()
            metrics = self.classifier.collect_ash_metrics()
            indicators = self.classifier.calculate_workload_indicators(metrics)
            profile = self.classifier.classify_workload(indicators)

            results.append({
                'config': config['name'],
                'expected': config['expected_profile'],
                'actual': profile,
                'work_mem': config['work_mem'],
                'shared_buffers': config['shared_buffers'],
                'success': profile == config['expected_profile']
            })

        return results

    def _generate_oltp_transition(self):
        """Генерация OLTP нагрузки для теста переходов"""
        conn = psycopg2.connect(self.conn_string)
        cur = conn.cursor()

        for i in range(100):
            cur.execute("""
                INSERT INTO test_orders (customer_id, order_total, order_date, status)
                VALUES (%s, %s, NOW(), 'pending')
            """, (i % 100 + 1, 100.00))

            if i % 10 == 0:
                conn.commit()

        conn.commit()
        conn.close()

    def _generate_olap_transition(self):
        """Генерация OLAP нагрузки для теста переходов"""
        conn = psycopg2.connect(self.conn_string)
        cur = conn.cursor()

        cur.execute("""
            SELECT c.customer_id, COUNT(o.order_id), SUM(o.order_total)
            FROM test_customers c
            LEFT JOIN test_orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id
            HAVING COUNT(o.order_id) > 0
        """)

        cur.fetchall()
        conn.close()

    def _create_synthetic_metrics(self, target_tps, target_latency):
        """Создание синтетических метрик для тестирования граничных условий"""
        return {
            'ash': {
                'active': {
                    'session_count': 10 if target_tps > 100 else 2,
                    'avg_query_time_seconds': target_latency / 1000,
                    'waiting_sessions': 1
                }
            },
            'db_time': {
                'total_db_time': target_tps * target_latency / 1000 * 100,
                'total_calls': target_tps * 100,
                'total_rows': target_tps * 1000,
                'cache_hit_ratio': 0.95
            },
            'db_committed': {
                'xact_commit': target_tps * 300,
                'xact_rollback': target_tps * 10,
                'read_write_ratio': 0.6,
                'write_operations': target_tps * 120,
                'read_operations': target_tps * 180
            }
        }

def run_specialized_tests():
    """Запуск всех специализированных тестов"""
    specialized = SpecializedTests()

    print("🚀 ЗАПУСК СПЕЦИАЛИЗИРОВАННЫХ ТЕСТОВ")

    # Тест переходов
    transition_results = specialized.test_workload_transition()

    # Тест граничных условий
    boundary_results = specialized.test_threshold_boundaries()

    # Тест чувствительности
    sensitivity_results = specialized.test_parameter_sensitivity()

    # Сводка
    print(f"\n📊 СВОДКА СПЕЦИАЛИЗИРОВАННЫХ ТЕСТОВ:")
    print(f"Тестов переходов: выполнено")
    print(f"Тестов граничных условий: {len(boundary_results)}")
    print(f"Тестов чувствительности: {len(sensitivity_results)}")

    successful_boundary = sum(1 for r in boundary_results if r['success'])
    successful_sensitivity = sum(1 for r in sensitivity_results if r['success'])

    print(f"Успешных граничных тестов: {successful_boundary}/{len(boundary_results)}")
    print(f"Успешных тестов чувствительности: {successful_sensitivity}/{len(sensitivity_results)}")

if __name__ == "__main__":
    run_specialized_tests()
