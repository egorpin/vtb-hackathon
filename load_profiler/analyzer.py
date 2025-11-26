class ProfileAnalyzer:
    """
    Разработанный алгоритм определения профиля нагрузки на основе метрик.
    Центральная метрика: Tx Cost (ASH / Committed)
    """
    def analyze(self, prev, curr, duration):
        # Дельты
        d_commits = curr["commits"] - prev["commits"]
        d_inserted = curr["tup_inserted"] - prev["tup_inserted"]
        d_fetched = curr["tup_fetched"] - prev["tup_fetched"]

        # Основные метрики
        tps = d_commits / duration
        # Среднее количество активных сессий за интервал
        avg_active = (prev["active_sessions"] + curr["active_sessions"]) / 2

        # --- ФОРМУЛА: Tx Cost (ASH / Commit) ---
        # Показывает, сколько в среднем *секунд* работы активной сессии тратится на один коммит.
        # Низкое значение (0.01-0.1) -> OLTP (быстрые транзакции)
        # Высокое значение (> 0.5) -> OLAP (тяжелые транзакции)
        tx_cost = (avg_active * duration) / d_commits if d_commits > 0 else 999.0

        # Соотношение Чтение/Запись
        read_write_ratio = d_fetched / d_inserted if d_inserted > 0 else 999.0

        # Количество активных ожиданий ввода/вывода (IO)
        io_waits = curr["waits"].get("IO", 0)

        # Количество ожиданий на блокировках
        lock_waits = curr["waits"].get("Lock", 0)

        # Метрики для отображения в GUI
        metrics = {
            "TPS": round(tps, 2),
            "Active Sessions (ASH)": round(avg_active, 2),
            "Total Connections": curr["total_connections"], # НОВАЯ МЕТРИКА
            "Tx Cost (s)": round(tx_cost, 4),
            "Max Latency (s)": round(curr["max_duration"], 2),
            "IO Waits": io_waits,
            "Lock Waits": lock_waits,
            "R/W Ratio": round(read_write_ratio, 2)
        }

        # --- ДЕТЕКЦИЯ (Иерархические правила) ---
        profile = "Unknown"
        confidence = "Low"

        # 1. IDLE (Простой) - Самое простое
        if avg_active < 1 and tps < 1:
            profile = "IDLE"
            confidence = "High"

        # 2. WRITE-HEAVY / INGESTION (IoT) - Приоритет высокой записи
        elif d_inserted > 500 and d_fetched < 100: # Много вставок, мало чтений
            profile = "IoT / Ingestion"
            confidence = "High"

        # 3. CLASSIC OLTP - Много быстрых транзакций
        elif tps > 20 and tx_cost < 0.05:
            profile = "Classic OLTP"
            confidence = "High"

        # 4. READ-ONLY / WEB - Низкий Tx Cost, но очень много чтений
        elif tps < 5 and read_write_ratio > 500: # Низкий TPS, но высокое соотношение R/W
            profile = "Read-Only / Web"
            confidence = "Medium"

        # 5. SESSION-HEAVY - Много соединений, но невысокая активность
        elif curr["total_connections"] > 50 and avg_active < 2 and tx_cost < 0.1:
            profile = "Session-Heavy"
            confidence = "Medium"

        # 6. OLAP / HTAP - Высокая стоимость транзакции (медленные запросы)
        elif tx_cost > 0.1 or curr["max_duration"] > 0.5:

            # a) Mixed / HTAP - Если есть OLTP-признаки вместе с OLAP-признаками
            if tps > 10 and tx_cost > 0.1:
                profile = "Mixed / HTAP" # Высокий TPS (OLTP) + Высокий Cost (OLAP)
                confidence = "High"

            # b) Disk-Bound OLAP - Упирается в диск
            elif io_waits >= 1:
                profile = "Disk-Bound OLAP"
                confidence = "Medium"

            # c) Heavy OLAP - Упирается в CPU/RAM
            else:
                profile = "Heavy OLAP"
                confidence = "Medium"

        # Добавляем профиль, если все остальное не подошло, но есть активность
        elif tps > 1 and tx_cost < 0.1:
            profile = "Balanced (Unclassified OLTP-like)"
            confidence = "Low"

        return profile, confidence, metrics
