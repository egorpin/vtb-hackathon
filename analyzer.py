class ProfileAnalyzer:
    def analyze(self, prev, curr, duration):
        if duration <= 0:
            duration = 1

        # --- 1. ВЫЧИСЛЕНИЕ ДЕЛЬТ ---
        d_commits = max(curr["commits"] - prev["commits"], 0)

        # Операции с данными
        d_inserted = max(curr["tup_inserted"] - prev["tup_inserted"], 0)
        d_fetched = max(curr["tup_fetched"] - prev["tup_fetched"], 0)
        d_updated = max(curr.get("tup_updated", 0) - prev.get("tup_updated", 0), 0)
        d_deleted = max(curr.get("tup_deleted", 0) - prev.get("tup_deleted", 0), 0)

        # Всего операций записи (DML)
        d_writes = d_inserted + d_updated + d_deleted

        # --- 2. РАСЧЕТ НАГРУЗКИ ---
        d_db_time_stats = max(curr.get("db_time_accumulated", 0) - prev.get("db_time_accumulated", 0), 0)
        avg_active_sessions = (prev["active_sessions"] + curr["active_sessions"]) / 2

        if d_db_time_stats > 0:
            db_time_rate = d_db_time_stats / duration
        else:
            db_time_rate = avg_active_sessions

        # TPS
        tps = d_commits / duration

        # Tx Cost (Стоимость транзакции)
        if tps > 0.1:
            tx_cost = db_time_rate / tps
        else:
            tx_cost = 0.0

        # --- 3. РАСЧЕТ КОЭФФИЦИЕНТОВ (RATIOS) ---

        # Read/Write Ratio: Отношение чтений к записи
        # Если d_writes 0, но есть чтения - это бесконечность (чистое чтение)
        rw_ratio = d_fetched / d_writes if d_writes > 0 else 9999.0

        # Insert Ratio: Доля вставок в общем потоке записи
        # IoT = 1.0 (только вставки)
        # Mixed = 0.4 (20 insert / 50 total writes)
        # OLTP/TPC-C = 0.25 (1 insert / 4 total writes)
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

        # --- ОТЛАДКА В КОНСОЛЬ (Чтобы понимать, почему выбран профиль) ---
        # print(f"DEBUG: TPS={tps:.1f}, InsRatio={insert_ratio:.2f}, R/W={rw_ratio:.1f}, Cost={tx_cost:.4f}")

        # --- 4. ДЕРЕВО РЕШЕНИЙ ---

        # 0. IDLE
        if db_time_rate < 0.1 and tps < 2:
            return "IDLE", "High", metrics

        # 1. IoT / Ingestion (Доминируют вставки)
        # Математически insert_ratio ≈ 1.0
        if d_writes > 10 and insert_ratio > 0.75:
            return "IoT / Ingestion", "High", metrics

        # 2. Heavy OLAP (Тяжелые запросы или чистое чтение)
        # Либо очень высокий rw_ratio (только селекты), либо высокая стоимость транзакции
        is_heavy_query = (tx_cost > 0.05) or (metrics["Max Latency (s)"] > 0.5)
        if (rw_ratio > 50) or (is_heavy_query and tps < 50):
            if io_waits > avg_active_sessions * 0.3:
                return "Disk-Bound OLAP", "High", metrics
            else:
                return "Heavy OLAP", "High", metrics

        # 3. Mixed / HTAP (Смешанная нагрузка)
        # Математически insert_ratio должен быть около 0.4
        # Ставим диапазон от 0.30 до 0.60
        if 0.30 <= insert_ratio <= 0.60:
            return "Mixed / HTAP", "Medium", metrics

        # 4. OLTP и TPC-C (Интенсивное обновление)
        # Математически insert_ratio около 0.25 (много апдейтов, мало инсертов)
        if insert_ratio < 0.30:
            # Как отличить TPC-C от простого OLTP?
            # TPC-C (симуляция) чуть тяжелее и часто имеет чуть больший Latency/Cost
            if tx_cost > 0.002: # Чуть тяжелее
                return "TPC-C OLTP", "Medium", metrics
            else:
                return "Classic OLTP", "Medium", metrics

        # Fallback (Если нагрузка есть, но странная)
        if tps > 5:
            return "Mixed / HTAP", "Low", metrics

        return "IDLE", "Low", metrics
