import subprocess
import time
from colorama import Fore, Style
from config import DB_CONFIG

# Параметры подключения для pgbench (используем ENV, как настроено в Docker)
PGBENCH_CMD_BASE = [
    "pgbench",
    "-h", DB_CONFIG['host'],
    "-p", DB_CONFIG['port'],
    "-U", DB_CONFIG['user'],
    "-d", DB_CONFIG['dbname']
]

# --- ШАБЛОНЫ НАГРУЗКИ ---
LOAD_SCENARIOS = {
    "1": {
        "name": "Classic OLTP (Банкинг)",
        "desc": "Быстрые, короткие транзакции (SELECT/UPDATE).",
        "options": ["-c", "10", "-j", "2", "-T", "60", "-P", "5"] # 10 клиентов, 60 секунд, отчет каждые 5с
    },
    "2": {
        "name": "Write-Heavy / IoT (Вставка данных)",
        "desc": "Преимущественно INSERT-транзакции (имитация IoT).",
        "options": ["-c", "5", "-j", "1", "-T", "30", "-P", "5", "-M", "prepared", "-b", "insert_simple"] # 5 клиентов, 30с, использует встроенный скрипт insert_simple
    },
    "3": {
        "name": "Heavy Read / OLAP Query (Аналитика)",
        "desc": "Один длинный, тяжелый SELECT-запрос.",
        "options": ["-c", "1", "-j", "1", "-T", "20", "-f", "/usr/src/app/load_profiler/olap_query.sql"] # 1 клиент, 20с, использует внешний скрипт
    }
}

def generate_load_menu():
    """Показывает меню выбора нагрузки и запускает ее."""
    print("\033c", end="")
    print(f"{Style.BRIGHT}{Fore.YELLOW}=== ГЕНЕРАТОР НАГРУЗКИ (pgbench) ==={Style.RESET_ALL}")
    print("Выберите тип нагрузки:")

    for key, scenario in LOAD_SCENARIOS.items():
        print(f"[{key}] {scenario['name']}: {scenario['desc']}")

    print(f"[0] {Fore.RED}Назад в Главное Меню{Style.RESET_ALL}")

    choice = input(f"\n{Fore.WHITE}Введите номер нагрузки (0-3): {Style.RESET_ALL}")

    if choice == '0':
        return

    if choice in LOAD_SCENARIOS:
        scenario = LOAD_SCENARIOS[choice]
        print(f"{Fore.GREEN}--- Подготовка: Инициализация БД для {scenario['name']}... ---{Style.RESET_ALL}")

        # 1. Инициализация (требуется перед запуском pgbench)
        # Масштаб 10 достаточно для демонстрации
        init_cmd = PGBENCH_CMD_BASE + ["-i", "-s", "10"]
        subprocess.run(init_cmd, check=True, text=True, capture_output=True)
        print(f"{Fore.GREEN}Инициализация завершена.{Style.RESET_ALL}")

        # 2. Запуск нагрузки
        print(f"\n{Fore.GREEN}--- ЗАПУСК НАГРУЗКИ: {scenario['name']} ({scenario['options'][3]}с) ---{Style.RESET_ALL}")

        full_cmd = PGBENCH_CMD_BASE + scenario['options']

        # Запускаем в новом процессе и ждем завершения
        # Не выводим сразу, чтобы не мешать выводу анализатора
        process = subprocess.Popen(full_cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        print(f"{Fore.CYAN}Нагрузка запущена. Во время работы откройте Анализ в Реальном Времени (Опция 1) в другом окне.{Style.RESET_ALL}")
        print(f"{Fore.CYAN}Или дождитесь завершения, чтобы увидеть результат...{Style.RESET_ALL}")

        # Ждем завершения процесса (Таймаут нагрузки задан в options)
        process.wait()

        print(f"\n{Fore.GREEN}--- НАГРУЗКА ЗАВЕРШЕНА. Результат pgbench: ---{Style.RESET_ALL}")
        print(process.stdout.read())

        input(f"{Fore.YELLOW}Нажмите Enter, чтобы вернуться в меню...{Style.RESET_ALL}")

    else:
        print(f"{Fore.RED}Неверный выбор. Попробуйте снова.{Style.RESET_ALL}")
        time.sleep(1)

# Дополнительный скрипт для OLAP, который нужно поместить в load_profiler/
OLAP_QUERY_SCRIPT = """
\\set aid random(1, 100000)
\\set bid random(1, 100000)
SELECT count(*) FROM pgbench_accounts a, pgbench_branches b, pgbench_tellers t
WHERE a.aid = :aid AND b.bid = :bid AND a.bid = b.bid AND a.tid = t.tid
GROUP BY a.aid;
"""
