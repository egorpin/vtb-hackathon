class ProfileAnalyzer:
    def analyze(self, prev, curr, duration):
        if duration <= 0:
            duration = 1

        d_commits = max(curr["commits"] - prev["commits"], 0)

        d_inserted = max(curr["tup_inserted"] - prev["tup_inserted"], 0)
        d_fetched = max(curr["tup_fetched"] - prev["tup_fetched"], 0)
        d_updated = max(curr.get("tup_updated", 0) - prev.get("tup_updated", 0), 0)
        d_deleted = max(curr.get("tup_deleted", 0) - prev.get("tup_deleted", 0), 0)

        d_writes = d_inserted + d_updated + d_deleted

        d_db_time_stats = max(curr.get("db_time_accumulated", 0) - prev.get("db_time_accumulated", 0), 0)

        avg_active_sessions = (prev["active_sessions"] + curr["active_sessions"]) / 2

        if d_db_time_stats > 0:
            db_time_rate = d_db_time_stats / duration
        else:
            db_time_rate = avg_active_sessions

        tps = d_commits / duration

        if tps > 0.5:
            tx_cost = db_time_rate / tps
        else:
            tx_cost = 0.0

        rw_ratio = d_fetched / d_writes if d_writes > 0 else 0.0
        if d_writes == 0 and d_fetched > 0:
            rw_ratio = 9999.0

        insert_ratio = d_inserted / d_writes if d_writes > 0 else 0.0

        io_waits = curr["waits"].get("IO", 0)

        metrics = {
            "TPS": round(tps, 2),
            "Active Sessions (ASH)": round(db_time_rate, 2),
            "Tx Cost (s)": round(tx_cost, 4),
            "Max Latency (s)": round(curr["max_duration"], 2),
            "IO Waits": io_waits,
            "Read/Write Ratio": round(rw_ratio, 2),
            "Insert/Write Ratio": round(insert_ratio, 2)
        }

        # ----------------------------------------------------
        # 1. IDLE/LOW ACTIVITY
        # ----------------------------------------------------
        if tps < 1.0 and db_time_rate < 0.5:
            return "IDLE", "High", metrics

        if db_time_rate < 0.1 and tps < 2:
            return "IDLE", "High", metrics

        # ----------------------------------------------------
        # 2. НОВЫЙ ПРОФИЛЬ: Data Maintenance (VACUUM, ANALYZE, REINDEX)
        # ----------------------------------------------------
        # Характеризуется:
        # - Высокой активностью (db_time_rate > 0.5)
        # - Нулевым TPS (tps < 0.5), т.к. VACUUM не является 'xact_commit'
        # - Нулевыми DML-операциями (d_writes < 10)
        if tps < 0.5 and db_time_rate > 0.5 and d_writes < 10:
            return "Data Maintenance", "High", metrics

        # ----------------------------------------------------
        # 3. WRITE-INTENSIVE LOADS (IoT)
        # ----------------------------------------------------

        # IoT / Ingestion: Высокий insert_ratio (80%+) с меньшим объемом записи, либо высокая TPS
        if d_writes > 50 and insert_ratio > 0.8:
            return "IoT / Ingestion", "High", metrics

        # ----------------------------------------------------
        # 4. READ-INTENSIVE & HEAVY LOADS (Web, OLAP, Batch)
        # ----------------------------------------------------

        is_heavy_query = (tx_cost > 0.05) or (metrics["Max Latency (s)"] > 1.0)

        # Web / Read-Only: Очень высокий коэффициент чтения (>100) И низкая стоимость транзакции (быстрые, кешированные запросы)
        if rw_ratio > 100 and tps > 10 and tx_cost < 0.015:
            return "Web / Read-Only", "High", metrics

        # ----------------------------------------------------
        # 4.1 НОВЫЙ ПРОФИЛЬ: End of day Batch (Тяжелые UPDATE/DELETE)
        # ----------------------------------------------------
        # Должен идти ПЕРЕД общим OLAP.
        # Характеризуется:
        # - Тяжелый запрос (is_heavy_query)
        # - Низкий TPS (tps < 10)
        # - Низкий Read/Write (rw_ratio < 5) - это НЕ чтение
        # - Низкий Insert/Write (insert_ratio < 0.2) - это НЕ IoT
        # (Это оставляет тяжелые UPDATE / DELETE)
        if is_heavy_query and rw_ratio < 5.0 and insert_ratio < 0.2 and tps < 10:
            return "End of day Batch", "High", metrics

        # ----------------------------------------------------
        # 4.2 Общая категория OLAP
        # ----------------------------------------------------
        if (rw_ratio > 50) or (is_heavy_query and tps < 100):
            if tps < 5.0 and db_time_rate < 1.0:
                 return "IDLE", "Low", metrics

            # Disk-Bound OLAP: Высокие задержки ввода/вывода (IO Waits) относительно ASH
            if io_waits > avg_active_sessions * 0.3:
                return "Disk-Bound OLAP", "High", metrics
            # Heavy OLAP: CPU/RAM-зависимый OLAP
            else:
                return "Heavy OLAP", "High", metrics

        # ----------------------------------------------------
        # 5. MIXED / OLTP LOADS
        # ----------------------------------------------------
        if 0.30 <= insert_ratio <= 0.65:
            return "Mixed / HTAP", "Medium", metrics

        if insert_ratio < 0.30 and tps > 10.0:
            # Classic OLTP: Быстрые или сложные, но с низким insert_ratio
            return "Classic OLTP", "High", metrics

        if tps > 5:
            return "Mixed / HTAP", "Low", metrics

        return "IDLE", "Low", metrics
