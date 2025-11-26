class ProfileAnalyzer:
    def analyze(self, prev, curr, duration):
        if duration <= 0:
            duration = 1

        # Дельты с защитой от отрицательных значений
        d_commits = max(curr["commits"] - prev["commits"], 0)
        d_inserted = max(curr["tup_inserted"] - prev["tup_inserted"], 0)
        d_fetched = max(curr["tup_fetched"] - prev["tup_fetched"], 0)

        # Основные метрики
        tps = d_commits / duration
        avg_active = (prev["active_sessions"] + curr["active_sessions"]) / 2

        # Tx Cost с защитой от деления на ноль
        tx_cost = (avg_active * duration) / d_commits if d_commits > 0 else 999.0

        read_write_ratio = d_fetched / d_inserted if d_inserted > 0 else 999.0
        io_waits = curr["waits"].get("IO", 0)
        cpu_waits = curr["waits"].get("CPU", 0)

        metrics = {
            "TPS": round(tps, 2),
            "Active Sessions (ASH)": round(avg_active, 2),
            "Tx Cost (s)": round(tx_cost, 4),
            "Max Latency (s)": round(curr["max_duration"], 2),
            "IO Waits": io_waits,
            "CPU Waits": cpu_waits,
            "Read/Write Ratio": round(read_write_ratio, 2)
        }

        # === УЛУЧШЕННАЯ ЛОГИКА ДЕТЕКЦИИ ПРОФИЛЕЙ ===

        profile = "IDLE"
        confidence = "Low"

        # 1. IDLE - система практически неактивна
        if tps < 0.5 and avg_active < 0.5 and d_inserted < 5:
            profile = "IDLE"
            confidence = "High"
            return profile, confidence, metrics

        # 2. Bulk Load - очень интенсивная запись, почти нет чтений
        if d_inserted > 1000 and read_write_ratio < 0.1:
            profile = "Bulk Load"
            confidence = "High"
            return profile, confidence, metrics

        # 3. Classic OLTP - высокая транзакционная нагрузка, короткие транзакции
        if tps > 10 and tx_cost < 0.1 and avg_active > 1:
            profile = "Classic OLTP"
            confidence = "High"
            return profile, confidence, metrics

        # 4. Web / Read-Only - преобладание операций чтения
        if read_write_ratio > 20 and tps > 2:
            profile = "Web / Read-Only"
            confidence = "High"
            return profile, confidence, metrics

        # 5. IoT / Ingestion - умеренная/высокая интенсивность записи
        if d_inserted > 100 and read_write_ratio < 1 and tps < 20:
            profile = "IoT / Ingestion"
            confidence = "Medium"
            return profile, confidence, metrics

        # 6. OLAP варианты - аналитические запросы, высокая latency
        if tx_cost > 0.3 or curr["max_duration"] > 2.0:
            if io_waits > cpu_waits and io_waits > 0:
                profile = "Disk-Bound OLAP"
                confidence = "Medium"
            else:
                profile = "Heavy OLAP"
                confidence = "Medium"

            # Mixed/HTAP если есть приличный TPS при аналитической нагрузке
            if tps > 5:
                profile = "Mixed / HTAP"
                confidence = "Medium"
            return profile, confidence, metrics

        # 7. Mixed / HTAP - смешанная нагрузка по умолчанию
        if tps > 2 or avg_active > 1 or d_inserted > 50:
            profile = "Mixed / HTAP"
            confidence = "Medium"
        else:
            profile = "IDLE"
            confidence = "Low"

        return profile, confidence, metrics
