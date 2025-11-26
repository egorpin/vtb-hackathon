import time
import sys
from datetime import datetime
from colorama import init, Fore, Style
from tabulate import tabulate
from load_generator import generate_load_menu

# Импорты наших модулей
from config import DB_CONFIG, ANALYSIS_INTERVAL, RECOMMENDATIONS
from metrics import MetricsCollector
from analyzer import ProfileAnalyzer, edit_thresholds # ИМПОРТИРУЕМ НОВУЮ ФУНКЦИЮ
from ui import print_dashboard

init(autoreset=True)

# Глобальная переменная для интервала, которую можно изменять
current_analysis_interval = ANALYSIS_INTERVAL

def run_analysis_loop(collector, analyzer, single_shot=False):
    """Запускает анализ в цикле или единичным снимком."""
    global current_analysis_interval
    interval = current_analysis_interval if not single_shot else 3 # Короткий интервал для снимка

    print(f"{Fore.CYAN}--- {'Запуск анализа' if not single_shot else 'Сбор снимка'} (Интервал: {interval}s) ---")

    # Сбор первого снимка
    prev_snapshot = collector.get_snapshot()
    time.sleep(1)

    try:
        while True:
            time.sleep(interval)
            curr_snapshot = collector.get_snapshot()

            profile, conf, metrics = analyzer.analyze(prev_snapshot, curr_snapshot, interval)
            # Передаем интервал в функцию вывода
            print_dashboard(profile, conf, metrics, interval)

            prev_snapshot = curr_snapshot

            if single_shot:
                input(f"{Fore.YELLOW}Нажмите Enter, чтобы вернуться в меню...{Style.RESET_ALL}")
                break

    except KeyboardInterrupt:
        if not single_shot:
            print(f"\n{Fore.YELLOW}--- Анализ остановлен. Возврат в меню. ---{Style.RESET_ALL}")

def display_recommendations():
    """Выводит на экран полный справочник рекомендаций."""
    print("\033c", end="")
    print(f"{Style.BRIGHT}{Fore.MAGENTA}=== СПРАВОЧНИК РЕКОМЕНДАЦИЙ (postgresql.conf) ==={Style.RESET_ALL}")

    table_data = []
    all_params = set()
    for recs in RECOMMENDATIONS.values():
        all_params.update(recs.keys())

    sorted_params = sorted(list(all_params))
    headers = ["Профиль Нагрузки"] + sorted_params

    for profile, recs in RECOMMENDATIONS.items():
        row = [profile]
        for param in sorted_params:
            row.append(recs.get(param, "-"))
        table_data.append(row)

    print(tabulate(table_data, headers=headers, tablefmt="fancy_grid"))
    input(f"\n{Fore.YELLOW}Нажмите Enter, чтобы вернуться в меню...{Style.RESET_ALL}")

def set_interval():
    """Позволяет пользователю изменить интервал анализа."""
    global current_analysis_interval
    print(f"{Fore.CYAN}Текущий интервал анализа: {current_analysis_interval} секунд.")
    try:
        new_interval = int(input("Введите новый интервал (в секундах, например, 10): "))
        if new_interval > 0:
            current_analysis_interval = new_interval
            print(f"{Fore.GREEN}Интервал успешно изменен на {current_analysis_interval} секунд.{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Интервал должен быть положительным числом.{Style.RESET_ALL}")
    except ValueError:
        print(f"{Fore.RED}Ошибка ввода. Пожалуйста, введите целое число.{Style.RESET_ALL}")
    time.sleep(1)

def show_menu():
    """Отображает главное меню."""
    print("\n" + "="*50)
    print(f"{Style.BRIGHT}{Fore.CYAN}      МЕНЮ АНАЛИЗАТОРА ПРОФИЛЯ НАГРУЗКИ{Style.RESET_ALL}")
    print("="*50)
    print("1: Запустить Анализ в Реальном Времени (Ctrl+C для остановки)")
    print("2: Запустить Единичный Снимок")
    print("3: Показать Полный Справочник Рекомендаций")
    print("4: Настроить Пороги Классификации (Тюнинг)")
    print("5: Установить Интервал Анализа (Текущий: {}s)".format(current_analysis_interval))
    print(f"6: {Style.BRIGHT}{Fore.YELLOW}Запустить Генератор Нагрузки (pgbench){Style.RESET_ALL}") # <-- ДОБАВЛЕНО
    print("0: Выход")
    print("-" * 50)
    return input(f"{Fore.WHITE}Выберите опцию (0-6): {Style.RESET_ALL}")

def main():
    # Инициализация подключения
    try:
        collector = MetricsCollector(DB_CONFIG)
    except SystemExit:
        return

    analyzer = ProfileAnalyzer()

    while True:
        try:
            choice = show_menu()

            if choice == '1':
                run_analysis_loop(collector, analyzer)
            elif choice == '2':
                run_analysis_loop(collector, analyzer, single_shot=True)
            elif choice == '3':
                display_recommendations()
            elif choice == '4':
                edit_thresholds(analyzer)
            elif choice == '5':
                set_interval()
            elif choice == '6':
                generate_load_menu()
            elif choice == '0':
                print(f"{Fore.MAGENTA}Программа остановлена.{Style.RESET_ALL}")
                sys.exit(0)
            else:
                print(f"{Fore.RED}Неизвестная опция '{choice}'. Пожалуйста, выберите число от 0 до 6.{Style.RESET_ALL}")
                time.sleep(1)
        except Exception as e:
            print(f"{Fore.RED}Критическая ошибка в меню: {e}{Style.RESET_ALL}")
            time.sleep(2)

if __name__ == "__main__":
    main()
