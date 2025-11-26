#!/usr/bin/env python3
"""
Тестовый скрипт для проверки всех компонентов системы
"""

import sys
import os
import psycopg2
import subprocess

# Добавляем текущую директорию в путь
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

def test_database_connection():
    """Тест подключения к БД"""
    try:
        from config import DB_CONFIG
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Проверяем основные таблицы
        cur.execute("SELECT to_regclass('public.load_profiles');")
        profiles_table = cur.fetchone()[0]

        cur.execute("SELECT to_regclass('public.benchmark_results');")
        results_table = cur.fetchone()[0]

        print("✅ База данных: подключение успешно")
        print(f"   - Таблица load_profiles: {'✅ найдена' if profiles_table else '❌ отсутствует'}")
        print(f"   - Таблица benchmark_results: {'✅ найдена' if results_table else '❌ отсутствует'}")

        if profiles_table:
            cur.execute("SELECT COUNT(*) FROM load_profiles")
            count = cur.fetchone()[0]
            print(f"   - Записей в load_profiles: {count}")

        conn.close()
        return True

    except Exception as e:
        print(f"❌ База данных: ошибка подключения - {e}")
        return False

def test_docker_containers():
    """Тест Docker контейнеров"""
    try:
        # Проверяем запущенные контейнеры
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True
        )

        containers = result.stdout.strip().split('\n')
        expected_containers = ['vtb_postgres', 'vtb_hammerdb']

        print("✅ Docker контейнеры:")
        for container in expected_containers:
            if container in containers:
                print(f"   - {container}: ✅ запущен")
            else:
                print(f"   - {container}: ❌ не запущен")

        return all(container in containers for container in expected_containers)

    except Exception as e:
        print(f"❌ Docker: ошибка - {e}")
        return False

def test_imports():
    """Тест импортов модулей"""
    modules_to_test = [
        'config', 'metrics', 'analyzer', 'db_loader', 'benchmark_runner'
    ]

    print("✅ Импорты модулей:")
    for module in modules_to_test:
        try:
            __import__(module)
            print(f"   - {module}: ✅ успешно")
        except ImportError as e:
            print(f"   - {module}: ❌ ошибка - {e}")
            return False

    return True

def test_hammerdb():
    """Тест HammerDB"""
    try:
        # Проверяем наличие hammerdbcli в контейнере
        result = subprocess.run([
            "docker", "exec", "vtb_hammerdb", "ls", "/hammerdb"
        ], capture_output=True, text=True)

        if result.returncode == 0:
            print("✅ HammerDB: контейнер доступен")

            # Проверяем наличие скрипта
            if "run_tpcc.tcl" in result.stdout:
                print("   - run_tpcc.tcl: ✅ найден")
            else:
                print("   - run_tpcc.tcl: ❌ отсутствует")

            return True
        else:
            print("❌ HammerDB: контейнер недоступен")
            return False

    except Exception as e:
        print(f"❌ HammerDB: ошибка - {e}")
        return False

def main():
    print("=== ТЕСТИРОВАНИЕ СИСТЕМЫ VTB PROFILER ===\n")

    tests = [
        test_imports,
        test_docker_containers,
        test_database_connection,
        test_hammerdb
    ]

    results = []
    for test in tests:
        results.append(test())
        print()

    if all(results):
        print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Система готова к работе.")
        print("\nЗапустите: python simple_gui.py")
    else:
        print("⚠️  НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОЙДЕНЫ!")
        print("Проверьте настройки и перезапустите контейнеры:")
        print("docker-compose down && docker-compose up --build")

if __name__ == "__main__":
    main()
