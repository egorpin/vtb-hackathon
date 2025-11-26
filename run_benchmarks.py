#!/usr/bin/env python3
"""
Автоматический запуск всех бенчмарков и генерация отчета
"""

import time
import sys
import os

# Добавляем текущую директорию в путь
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from benchmark_runner import BenchmarkRunner
from config import DB_CONFIG  # Импорт напрямую из config

def main():
    runner = BenchmarkRunner(DB_CONFIG)

    profiles_to_test = [
        "Classic OLTP",
        "Heavy OLAP",
        "Mixed / HTAP",
        "Web / Read-Only",
        "IoT / Ingestion"
    ]

    print("Starting automated benchmark suite...")
    print("=" * 60)

    all_results = []

    for profile in profiles_to_test:
        print(f"\n=== Testing {profile} ===")

        # Запускаем TPC-C тест
        results = runner.run_tpcc_benchmark(profile, duration_minutes=2)

        if 'error' in results:
            print(f"ERROR: {results['error']}")
        else:
            tpm = results.get('tpm', 0)
            latency = results.get('avg_latency', 0)
            print(f"Results: {tpm:.0f} TPM, Latency: {latency:.2f}ms")
            all_results.append((profile, tpm, latency))

        time.sleep(10)  # Пауза между тестами

    # Запускаем pgbench тесты
    print("\n=== Running pgbench tests ===")
    for profile in ["Classic OLTP", "Mixed / HTAP"]:
        results = runner.run_pgbench_test(profile, duration=30)
        if 'error' not in results:
            print(f"pgbench {profile}: {results.get('tps', 0):.1f} TPS")

    # Генерация финального отчета
    print("\n" + "=" * 60)
    print("FINAL BENCHMARK COMPARISON")
    print("=" * 60)

    report = runner.get_comparison_report()
    print(f"{'Profile':<20} | {'Test Type':<10} | {'TPS':<8} | {'TPM':<8} | {'Latency':<10} | {'Tests':<6}")
    print("-" * 80)

    for profile, test_type, avg_tps, avg_tpm, avg_latency, tests in report:
        print(f"{profile:<20} | {test_type:<10} | {avg_tps:<8.1f} | {avg_tpm:<8.0f} | {avg_latency:<10.2f} | {tests:<6}")

    # Сохраняем отчет в файл
    with open("benchmark_report.txt", "w") as f:
        f.write("VTB Load Profiler - Benchmark Report\n")
        f.write("=" * 50 + "\n\n")
        for profile, test_type, avg_tps, avg_tpm, avg_latency, tests in report:
            f.write(f"{profile} ({test_type}): {avg_tps:.1f} TPS, {avg_tpm:.0f} TPM, {avg_latency:.2f}ms latency\n")

    print("\nReport saved to benchmark_report.txt")

if __name__ == "__main__":
    main()
