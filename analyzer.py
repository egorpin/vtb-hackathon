class ProfileAnalyzer:
    def analyze(self, prev, curr, duration):
        if duration <= 0:
            duration = 1

        # 1. Вычисляем Дельты
        d_commits = max(curr["commits"] - prev["commits"], 0)
        d_inserted = max(curr["tup_inserted"] - prev["tup_inserted"], 0)
        d_fetched = max(curr["tup_fetched"] - prev["tup_fetched"], 0)

        # 2. Расчет DB Time
        d_db_time_stats = max(curr.get("db_time_accumulated", 0) - prev.get("db_time_accumulated", 0), 0)

        # Fallback: ASH
        avg_active_sessions = (prev["active_sessions"] + curr["active_sessions"]) / 2

        if d_db_time_stats > 0:
            db_time_rate = d_db_time_stats / duration
        else:
            db_time_rate = avg_active_sessions

        # 3. TPS
        tps = d_commits / duration

        # 4. Cost per Transaction
        if tps > 0.1:
            tx_cost = db_time_rate / tps
        else:
            tx_cost = 0.0

        # === ИСПРАВЛЕНИЕ 1: Расчет Ratio с защитой от шума ===
        # Вычисляем скорость чтения и записи
        inserts_per_sec = d_inserted / duration
        fetches_per_sec = d_fetched / duration

        if d_inserted > 0:
            read_write_ratio = d_fetched / d_inserted
        else:
            # Если вставок нет, ставим 9999 ТОЛЬКО если есть реальная нагрузка на чтение.
            # Если мы читаем 5 строк в секунду (фоновый шум), это не OLAP.
            read_write_ratio = 9999.0 if fetches_per_sec > 100 else 0.0

        io_waits = curr["waits"].get("IO", 0)

        metrics = {
            "TPS": round(tps, 2),
            "Active Sessions (ASH)": round(db_time_rate, 2),
            "Tx Cost (s)": round(tx_cost, 4),
            "Max Latency (s)": round(curr["max_duration"], 2),
            "IO Waits": io_waits,
            "Read/Write Ratio": round(read_write_ratio, 2),
            "Inserted/sec": round(inserts_per_sec, 2),
            "Fetched/sec": round(fetches_per_sec, 2)
        }

        # === ЛОГИКА ОПРЕДЕЛЕНИЯ ПРОФИЛЯ ===
        profile = "IDLE"
        confidence = "Low"

        # 1. IDLE
        # Немного подняли порог TPS, чтобы мелкий шум не мешал
        if db_time_rate < 0.1 and tps < 2:
            return "IDLE", "High", metrics

        # 2. IoT / Ingestion / Bulk Load
        # Критерий: Доминируют вставки, чтений мало
        if inserts_per_sec > 5 and read_write_ratio < 2.0:
            if read_write_ratio < 0.5 or inserts_per_sec > 50:
                profile = "IoT / Ingestion"
                if inserts_per_sec > 500:
                    profile = "Bulk Load"
                confidence = "High"
                return profile, confidence, metrics

        # 3. Heavy OLAP / Disk-Bound
        # === ИСПРАВЛЕНИЕ 2: Улучшенные критерии OLAP ===

        # Условие Latency: Запросы долгие И система реально загружена (ASH > 0.2)
        # Это предотвращает ложное срабатывание на одном "зависшем" коннекте
        is_heavy_latency = (metrics["Max Latency (s)"] > 0.5 or tx_cost > 0.5) and db_time_rate > 0.2

        # Условие Read Intensive:
        # 1. Ratio высокий (> 1000)
        # 2. И ГЛАВНОЕ: Мы реально читаем много строк (> 2000 в сек)
        is_read_intensive = read_write_ratio > 1000 and fetches_per_sec > 2000

        if is_heavy_latency or is_read_intensive:
            if io_waits > avg_active_sessions * 0.3:
                profile = "Disk-Bound OLAP"
            else:
                profile = "Heavy OLAP"
            confidence = "Medium"
            return profile, confidence, metrics

        # 4. Web / Read-Only
        # Высокий TPS, преобладает чтение, но не в промышленных масштабах OLAP
        if tps > 5 and read_write_ratio > 10:
             profile = "Web / Read-Only"
             confidence = "High"
             return profile, confidence, metrics

        # 5. Classic OLTP
        if tps > 5:
            profile = "Classic OLTP"
            confidence = "High"
            return profile, confidence, metrics

        # 6. Mixed / HTAP (Fallback)
        profile = "Mixed / HTAP"
        confidence = "Low"

        return profile, confidence, metrics
