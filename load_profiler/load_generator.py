import subprocess
import time
import sys
from colorama import Fore, Style
from config import DB_CONFIG

# Параметры подключения для pgbench (используем ENV)
PGBENCH_CMD_BASE = [
    "pgbench",
    "-h", DB_CONFIG['host'],
    "-p", DB_CONFIG['port'],
    "-U", DB_CONFIG['user'],
    "-d", DB_CONFIG['dbname']
]

# --- РЕАЛЬНЫЕ ШАБЛОНЫ НАГРУЗКИ ---
LOAD_SCENARIOS = {
    "1": {
        "name": "Classic OLTP (Банкинг)",
        "profile": "Classic OLTP",
        "desc": "Высокий TPS, низкая задержка. Имитация TPC-B.",
        "options": ["-c", "30", "-j", "4", "-T", "60", "-P", "5"] # 30 клиентов, 60 секунд
    },
    "2": {
        "name": "IoT / Ingestion",
        "profile": "IoT / Ingestion",
        "desc": "Преимущественно INSERT-транзакции (имитация записи логов).",
        "options": ["-c", "10", "-j", "2", "-T", "40", "-P", "5", "-b", "insert_simple"] # 10 клиентов, 40с, встроенный скрипт
    },
    "3": {
        "name": "Heavy OLAP (CPU/Mem Bound)",
        "profile": "Heavy OLAP (CPU/Mem Bound)",
        "desc": "Один или несколько тяжелых запросов с JOIN и GROUP BY.",
        "options": ["-c", "2", "-j", "1", "-T", "30", "-f", "load_profiler/olap_heavy.sql"] # 2 клиента, 30с, использует olap_heavy.sql
    },
    "4": {
        "name": "Web / Read-Only (Session Heavy)",
        "profile": "Web / Read-Only",
        "desc": "Очень высокая конкуренция (SELECTы). Много активных сессий.",
        "options": ["-c", "50", "-j", "4", "-T", "60", "-P", "10", "-f", "load_profiler/oltp_read_heavy.sql"] # 50 клиентов, 60с
    },
    "5": {
        "name": "Mixed / HTAP",
        "profile": "Mixed / HTAP",
        "desc": "Смешанная нагрузка: 70% OLTP (дефолт) и 30% OLAP (olap_heavy).",
        "options": ["-c", "20", "-j", "4", "-T", "60", "-P", "10", "--rate", "70", "-f", "load_profiler/olap_heavy.sql", "-f", "load_profiler/oltp_read_heavy.sql", "-p", "7:3"] # Соотношение 7:3
    },
    "6": {
        "name": "Bulk Load (Синглтон)",
        "profile": "Bulk Load",
        "desc": "Одиночный поток, максимально быстрая запись (имитация COPY FROM).",
        "options": ["-c", "1", "-j", "1", "-T", "20", "-b", "insert_simple"]
    }
}

def init_pgbench_db():
    """Инициализирует базу данных pgbench, если она еще не инициализирована."""
    print(f"\n{Fore.GREEN}--- Подготовка: Инициализация БД (scale 10) ---{Style.RESET_ALL}")
    # Параметры: -i (инициализация), -s 10 (масштаб 10)
    init_cmd = PGBENCH_CMD_BASE + ["-i", "-s", "10"]

    try:
        # Добавляем небольшой таймаут перед запуском, чтобы дать БД время
        time.sleep(1)

        # Запускаем команду и проверяем код возврата
        result = subprocess.run(
            init_cmd,
            check=True,
            text=True,
            capture_output=True,
            timeout=10 # Добавляем таймаут на всякий случай
        )
        print(f"{Fore.GREEN}Инициализация завершена успешно. Создано {len(result.stdout.splitlines())} строк лога.{Style.RESET_ALL}")

    except subprocess.CalledProcessError as e:
        print(f"{Fore.RED}{Style.BRIGHT}[КРИТИЧЕСКАЯ ОШИБКА ИНИЦИАЛИЗАЦИИ pgbench]{Style.RESET_ALL}")
        print(f"Команда: {' '.join(e.cmd)}")
        print(f"Код ошибки: {e.returncode}")
        print(f"{Fore.YELLOW}STDOUT (Вывод pgbench):{Style.RESET_ALL}\n{e.stdout}")
        print(f"{Fore.YELLOW}STDERR (Ошибка pgbench):{Style.RESET_ALL}\n{e.stderr}")
        print(f"{Fore.RED}Проверьте, что БД доступна, и пользователь 'user' может создавать таблицы.{Style.RESET_ALL}")
        # Возвращаем False, чтобы не пытаться запускать нагрузку
        return False
    except subprocess.TimeoutExpired:
        print(f"{Fore.RED}Инициализация pgbench превысила таймаут в 10 секунд. Проверьте БД.{Style.RESET_ALL}")
        return False
    except Exception as e:
        print(f"{Fore.RED}Неизвестная ошибка при инициализации: {e}{Style.RESET_ALL}")
        return False

    return True


def generate_load_menu():
    """Показывает меню выбора нагрузки и запускает ее."""

    if not init_pgbench_db():
        input(f"\n{Fore.YELLOW}Исправьте ошибку инициализации pgbench. Нажмите Enter, чтобы вернуться в меню...{Style.RESET_ALL}")
        return

    while True:
        print("\033c", end="")
        print(f"{Style.BRIGHT}{Fore.YELLOW}=== ГЕНЕРАТОР НАГРУЗКИ (pgbench) ==={Style.RESET_ALL}")
        print("Выберите тип нагрузки для генерации:")

        for key, scenario in LOAD_SCENARIOS.items():
            print(f"[{key}] {scenario['name']} ({scenario['profile']}): {scenario['desc']}")

        print(f"[0] {Fore.RED}Назад в Главное Меню{Style.RESET_ALL}")

        choice = input(f"\n{Fore.WHITE}Введите номер нагрузки (0-6): {Style.RESET_ALL}")

        if choice == '0':
            return

        if choice in LOAD_SCENARIOS:
            scenario = LOAD_SCENARIOS[choice]

            # Запуск нагрузки
            print(f"\n{Fore.GREEN}--- ЗАПУСК НАГРУЗКИ: {scenario['name']} ({scenario['options'][5]}с) ---{Style.RESET_ALL}")

            full_cmd = PGBENCH_CMD_BASE + scenario['options']

            # Запускаем в новом процессе
            print(f"{Fore.CYAN}Нагрузка запущена. (Длительность: {scenario['options'][5]} секунд).{Style.RESET_ALL}")
            print(f"{Fore.CYAN}Переключитесь на Опцию 1 (Анализ в Реальном Времени) в другом окне.{Style.RESET_ALL}")

            process = subprocess.Popen(full_cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            # Ждем завершения
            process.wait()

            print(f"\n{Fore.GREEN}--- НАГРУЗКА ЗАВЕРШЕНА ({scenario['name']}). Результат pgbench: ---{Style.RESET_ALL}")
            print(process.stdout.read())

            input(f"{Fore.YELLOW}Нажмите Enter, чтобы вернуться в меню...{Style.RESET_ALL}")
            break # Выходим из цикла выбора нагрузки

        else:
            print(f"{Fore.RED}Неверный выбор. Попробуйте снова.{Style.RESET_ALL}")
            time.sleep(1)
