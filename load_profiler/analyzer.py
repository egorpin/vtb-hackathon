from colorama import Fore, Style
from tabulate import tabulate
import time

class ProfileAnalyzer:
    def __init__(self):
        # 1. Пороги для классификации. Вынесены для возможности тюнинга.
        self.thresholds = {
            "IDLE_TPS": 5.0,            # Макс. TPS для IDLE
            "OLTP_TPS": 50.0,           # Мин. TPS для Classic OLTP
            "OLTP_TX_COST": 0.05,       # Макс. Tx Cost (s/tx) для Classic OLTP (50ms)
            "BULK_INSERT_RATE": 5000.0, # Мин. Rows Inserted/s для Bulk Load
            "OLAP_MAX_LATENCY": 2.0,    # Мин. Max Latency (s) для OLAP
            "MIXED_TPS": 10.0,          # Мин. TPS для Mixed / HTAP
            "READ_HEAVY_RATIO": 100.0   # Мин. Read/Write Ratio для Web / Read-Only
        }

    def analyze(self, prev, curr, duration):
        # Вычисляем дельты
        d_commits = curr["commits"] - prev["commits"]
        d_inserted = curr["tup_inserted"] - prev["tup_inserted"]
        d_fetched = curr["tup_fetched"] - prev["tup_fetched"]

        # Базовые метрики
        tps = d_commits / duration
        avg_active = (prev["active_sessions"] + curr["active_sessions"]) / 2

        # Основная формула: Стоимость одной транзакции в секундах работы БД (DB Time ASH / Committed)
        tx_cost = (avg_active * duration) / d_commits if d_commits > 0 else 999.0

        read_write_ratio = d_fetched / d_inserted if d_inserted > 0 else 999.0
        io_waits = curr["waits"].get("IO", 0)

        metrics = {
            "TPS": round(tps, 2),
            "Active Sessions (ASH)": round(avg_active, 2),
            "Tx Cost (s)": round(tx_cost, 4),
            "Max Latency (s)": round(curr["max_duration"], 2),
            "IO Waits": io_waits,
            "Rows Inserted/s": round(d_inserted/duration, 0)
        }

        # --- ДЕРЕВО РЕШЕНИЙ (используем self.thresholds) ---
        profile = "Unknown"
        confidence = "Low"

        # 1. IDLE (Простой)
        if avg_active < 1 and tps < self.thresholds["IDLE_TPS"]:
            profile = "IDLE"
            confidence = "High"

        # 2. Bulk Load (Пакетная загрузка)
        elif d_inserted / duration > self.thresholds["BULK_INSERT_RATE"] and tps < self.thresholds["MIXED_TPS"]:
            profile = "Bulk Load"
            confidence = "High"

        # 3. Read-Heavy (Чтение)
        elif read_write_ratio > self.thresholds["READ_HEAVY_RATIO"] and tps > self.thresholds["IDLE_TPS"]:
            profile = "Web / Read-Only"
            confidence = "High"

        # 4. OLTP (Транзакции)
        elif tps > self.thresholds["OLTP_TPS"] and tx_cost < self.thresholds["OLTP_TX_COST"]:
            profile = "Classic OLTP"
            confidence = "High"

        # 5. OLAP / Mixed (Тяжелые запросы)
        elif tx_cost > self.thresholds["OLTP_TX_COST"] * 10 or curr["max_duration"] > self.thresholds["OLAP_MAX_LATENCY"]:
            if tps > self.thresholds["MIXED_TPS"]:
                profile = "Mixed / HTAP"
                confidence = "Medium"
            elif io_waits > 0:
                profile = "Heavy OLAP (Disk Bound)"
            else:
                profile = "Heavy OLAP (CPU/Mem Bound)"
            confidence = "Medium"

        else:
            profile = "Mixed / HTAP" # Дефолт, если не подходит ни под что
            confidence = "Low"

        return profile, confidence, metrics


def edit_thresholds(analyzer):
    """Интерактивное редактирование пороговых значений классификации."""
    print("\033c", end="")
    print(f"{Style.BRIGHT}{Fore.MAGENTA}=== НАСТРОЙКА ПОРОГОВ КЛАССИФИКАЦИИ (ТЮНИНГ) ==={Style.RESET_ALL}")

    thresholds = analyzer.thresholds
    threshold_keys = list(thresholds.keys())

    print("\nТекущие пороги:")
    for i, key in enumerate(threshold_keys):
        print(f"[{i+1}] {key} ({ProfileAnalyzer.get_description(key)}): {thresholds[key]}")

    print(f"[0] {Fore.YELLOW}Вернуться в главное меню{Style.RESET_ALL}")

    try:
        choice = input("Выберите номер порога для изменения или 0 для выхода: ")
        choice_index = int(choice)

        if choice_index == 0:
            return

        if 1 <= choice_index <= len(threshold_keys):
            key_to_change = threshold_keys[choice_index - 1]
            current_value = thresholds[key_to_change]

            new_value_str = input(f"Введите новое значение для {key_to_change} (Текущее: {current_value}): ")
            new_value = float(new_value_str)

            if new_value >= 0:
                thresholds[key_to_change] = new_value
                print(f"{Fore.GREEN}Порог {key_to_change} успешно изменен на {new_value}{Style.RESET_ALL}")
            else:
                print(f"{Fore.RED}Значение должно быть неотрицательным.{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Неверный номер.{Style.RESET_ALL}")

    except ValueError:
        print(f"{Fore.RED}Ошибка ввода. Пожалуйста, введите число.{Style.RESET_ALL}")

    time.sleep(1)


# Добавляем описание для удобства пользователя в меню тюнинга
ProfileAnalyzer.get_description = lambda k: {
    "IDLE_TPS": "Транзакций в секунду для простоя",
    "OLTP_TPS": "Мин. транзакций в секунду для OLTP",
    "OLTP_TX_COST": "Макс. стоимость транзакции (s) для OLTP",
    "BULK_INSERT_RATE": "Мин. вставленных строк в секунду для Bulk Load",
    "OLAP_MAX_LATENCY": "Мин. задержка (s) для OLAP",
    "MIXED_TPS": "Мин. TPS для смешанной нагрузки",
    "READ_HEAVY_RATIO": "Мин. соотношение Чтение/Запись для Read-Only"
}.get(k, "Нет описания")
