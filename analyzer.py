class ProfileAnalyzer:
    def analyze(self, prev, curr, duration):
        if duration <= 0:
            duration = 1

        # 1. Вычисляем Дельты
        d_commits = max(curr["commits"] - prev["commits"], 0)
        d_inserted = max(curr["tup_inserted"] - prev["tup_inserted"], 0)
        d_fetched = max(curr["tup_fetched"] - prev["tup_fetched"], 0)

        # 2. Расчет DB Time (Load)
        # Если есть данные из pg_stat_statements (db_time_accumulated), используем их
        d_db_time_stats = max(curr.get("db_time_accumulated", 0) - prev.get("db_time_accumulated", 0), 0)

        # Fallback: ASH (Average Active Sessions)
        avg_active_sessions = (prev["active_sessions"] + curr["active_sessions"]) / 2

        # Основная метрика нагрузки: DB Time Rate (AAS)
        # Если удалось получить точное время (d_db_time_stats > 0), используем его
        if d_db_time_stats > 0:
            db_time_rate = d_db_time_stats / duration
        else:
            # Иначе используем приближение через активные сессии
            db_time_rate = avg_active_sessions

        # 3. Расчет TPS
        tps = d_commits / duration

        # 4. Cost per Transaction (Tx Cost) - сколько DB Time тратится на 1 транзакцию
        if tps > 0.1:
            tx_cost = db_time_rate / tps
        else:
            # Если транзакций нет, ставим условный 0 или max, в зависимости от контекста
            tx_cost = 0.0

        # Вспомогательные метрики
        read_write_ratio = d_fetched / d_inserted if d_inserted > 0 else 999.0

        # Waits (для совместимости оставляем, даже если 0)
        io_waits = curr["waits"].get("IO", 0)
        cpu_waits = curr["waits"].get("CPU", 0)

        # === СОБИРАЕМ СЛОВАРЬ (Сохраняем старые ключи для совместимости с UI) ===
        metrics = {
            "TPS": round(tps, 2),
            "Active Sessions (ASH)": round(db_time_rate, 2), # Теперь это AAS (DB Time Rate)
            "Tx Cost (s)": round(tx_cost, 4),                # Рассчитано через DB Time / TPS
            "Max Latency (s)": round(curr["max_duration"], 2),
            "IO Waits": io_waits,
            "CPU Waits": cpu_waits,
            "Read/Write Ratio": round(read_write_ratio, 2)
        }

        # === ЛОГИКА ОПРЕДЕЛЕНИЯ ПРОФИЛЯ ===
        profile = "IDLE"
        confidence = "Low"

        # 1. IDLE
        if db_time_rate < 0.1 and tps < 1:
            return "IDLE", "High", metrics

        # 2. Classic OLTP
        # Дешевые транзакции (cost < 0.05s) и высокий TPS
        if tps > 10 and tx_cost < 0.1:
            if read_write_ratio > 100:
                profile = "Web / Read-Only"
            else:
                profile = "Classic OLTP"
            confidence = "High"
            return profile, confidence, metrics

        # 3. Heavy OLAP
        # Дорогие транзакции (cost > 1.0s) или высокая latency
        if tx_cost > 1.0 or metrics["Max Latency (s)"] > 2.0:
            if io_waits > avg_active_sessions * 0.5:
                profile = "Disk-Bound OLAP"
            else:
                profile = "Heavy OLAP"
            confidence = "Medium"
            return profile, confidence, metrics

        # 4. Bulk Load
        if d_inserted > 1000 and read_write_ratio < 0.5:
            profile = "Bulk Load"
            confidence = "High"
            return profile, confidence, metrics

        # 5. Mixed / HTAP
        if 0.1 <= tx_cost <= 1.0 and tps > 2:
            profile = "Mixed / HTAP"
            confidence = "Medium"
            return profile, confidence, metrics

        # Fallback
        if db_time_rate > 1:
            profile = "Mixed / HTAP"

        return profile, confidence, metrics
