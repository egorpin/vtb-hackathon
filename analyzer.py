class ProfileAnalyzer:
    def analyze(self, prev, curr, duration):
        if duration <= 0:
            duration = 1

        # 1. Вычисляем Дельты
        d_commits = max(curr["commits"] - prev["commits"], 0)
        d_inserted = max(curr["tup_inserted"] - prev["tup_inserted"], 0)
        d_fetched = max(curr["tup_fetched"] - prev["tup_fetched"], 0)

        # 2. Расчет DB Time (Load)
        d_db_time_stats = max(curr.get("db_time_accumulated", 0) - prev.get("db_time_accumulated", 0), 0)

        # Fallback: ASH
        avg_active_sessions = (prev["active_sessions"] + curr["active_sessions"]) / 2

        if d_db_time_stats > 0:
            db_time_rate = d_db_time_stats / duration
        else:
            db_time_rate = avg_active_sessions

        # 3. Расчет TPS
        tps = d_commits / duration

        # 4. Tx Cost
        if tps > 0.1:
            tx_cost = db_time_rate / tps
        else:
            tx_cost = 0.0

        # 5. Read/Write Ratio
        # Защита от деления на ноль: если вставок 0, ratio = бесконечность (чистое чтение)
        read_write_ratio = d_fetched / d_inserted if d_inserted > 0 else 9999.0

        # Waits
        io_waits = curr["waits"].get("IO", 0)
        cpu_waits = curr["waits"].get("CPU", 0)

        metrics = {
            "TPS": round(tps, 2),
            "Active Sessions (ASH)": round(db_time_rate, 2),
            "Tx Cost (s)": round(tx_cost, 4),
            "Max Latency (s)": round(curr["max_duration"], 2),
            "IO Waits": io_waits,
            "CPU Waits": cpu_waits,
            "Read/Write Ratio": round(read_write_ratio, 2)
        }

        # === ЛОГИКА ОПРЕДЕЛЕНИЯ ПРОФИЛЯ (ИСПРАВЛЕННАЯ) ===

        # 1. IDLE (Простой)
        if db_time_rate < 0.1 and tps < 1:
            return "IDLE", "High", metrics

        # 2. IoT / Ingestion (ПРОВЕРЯЕМ ПЕРВЫМ!)
        # Характеристики: Интенсивная вставка, почти нет чтений (ratio < 0.1)
        # Даже если TPS высокий, это IoT, а не OLTP, если ratio экстремально низкий.
        if d_inserted > 100 and read_write_ratio < 0.2:
            if tx_cost < 0.05:
                return "IoT / Ingestion", "High", metrics
            else:
                return "Bulk Load", "Medium", metrics

        # 3. Heavy OLAP / Disk-Bound OLAP
        # Характеристики: Длинные транзакции ИЛИ Доминирование чтений при низком TPS
        # Если TPS низкий (< 50), но база работает (db_time > 1), это скорее всего сложные запросы
        is_high_latency = (tx_cost > 0.5) or (metrics["Max Latency (s)"] > 1.0)
        is_low_tps_high_load = (tps < 50 and db_time_rate > 1.0)

        if is_high_latency or is_low_tps_high_load:
            if io_waits > avg_active_sessions * 0.4:
                return "Disk-Bound OLAP", "High", metrics
            else:
                return "Heavy OLAP", "Medium", metrics

        # 4. Web / Read-Only
        # Характеристики: Огромное количество чтений по сравнению с записью
        if read_write_ratio > 100 and tps > 5:
            return "Web / Read-Only", "High", metrics

        # 5. Classic OLTP
        # Характеристики: Высокий TPS, короткие транзакции, сбалансированное чтение/запись
        if tps > 10 and tx_cost < 0.5:
            return "Classic OLTP", "High", metrics

        # 6. Mixed / HTAP (Все остальное)
        return "Mixed / HTAP", "Low", metrics
