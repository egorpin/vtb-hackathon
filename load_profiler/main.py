import time
from colorama import init

# Импорты наших модулей
from config import DB_CONFIG, ANALYSIS_INTERVAL
from metrics import MetricsCollector
from analyzer import ProfileAnalyzer
from ui import print_dashboard

# Инициализация цвета
init(autoreset=True)

def main():
    print(f"Запуск VTB Profiler...")
    print(f"Попытка подключения к {DB_CONFIG['host']}...")
    
    # Инициализация классов
    collector = MetricsCollector(DB_CONFIG)
    analyzer = ProfileAnalyzer()
    
    # Небольшая пауза для прогрева
    time.sleep(2)
    
    print("Сбор первоначальных данных...")
    prev_snapshot = collector.get_snapshot()
    time.sleep(1)

    try:
        while True:
            # Ждем интервал
            time.sleep(ANALYSIS_INTERVAL)
            
            # Собираем новые данные
            curr_snapshot = collector.get_snapshot()
            
            # Анализируем
            profile, conf, metrics = analyzer.analyze(prev_snapshot, curr_snapshot, ANALYSIS_INTERVAL)
            
            # Рисуем
            print_dashboard(profile, conf, metrics)
            
            # Обновляем состояние
            prev_snapshot = curr_snapshot

    except KeyboardInterrupt:
        print("\nОстановка мониторинга.")

if __name__ == "__main__":
    main()