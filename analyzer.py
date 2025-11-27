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

        # Среднее кол-во активных сессий (из pg_stat_activity)
        avg_active_sessions = (prev["active_sessions"] + curr["active_sessions"]) / 2

        # DB Time Rate (AAS - Average Active Sessions)
        # Если есть статистика по времени выполнения, используем её (точнее), иначе snapshot сессий
        if d_db_time_stats > 0:
            db_time_rate = d_db_time_stats / duration
        else:
            db_time_rate = avg_active_sessions

        # TPS
        tps = d_commits / duration

        # Tx Cost (Стоимость одной транзакции в секундах CPU)
        if tps > 0.5: # Считаем стоимость только если есть хоть какой-то поток транзакций
            tx_cost = db_time_rate / tps
        else:
            tx_cost = 0.0

        # --- 3. РАСЧЕТ КОЭФФИЦИЕНТОВ (RATIOS) ---

        # Read/Write Ratio: Отношение чтений к записи
        rw_ratio = d_fetched / d_writes if d_writes > 0 else 0.0
        if d_writes == 0 and d_fetched > 0:
            rw_ratio = 9999.0 # Pure Read

        # Insert Ratio: Доля вставок в общем потоке записи
        # IoT ≈ 1.0 (только вставки)
        # Mixed ≈ 0.3-0.6
        # OLTP ≈ 0.25
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

        # --- 4. ДЕРЕВО РЕШЕНИЙ (DECISION TREE) ---

        # [FIX] 0. IDLE (Система простаивает или шум)
        # Если TPS очень низкий (< 1.0) и CPU почти не занят (< 0.5 ядра),
        # то считаем это простоем, даже если есть случайные долгие запросы (мониторинг/вакуум).
        if tps < 1.0 and db_time_rate < 0.5:
            return "IDLE", "High", metrics

        # Также классический IDLE
        if db_time_rate < 0.1 and tps < 2:
            return "IDLE", "High", metrics

        # 1. IoT / Ingestion (Доминируют вставки)
        # Много записей (> 100 за интервал), и почти все они - INSERT
        if d_writes > 50 and insert_ratio > 0.8:
            return "IoT / Ingestion", "High", metrics

        # [FIX] 2. Heavy OLAP (Аналитика)
        # Признаки: Высокая стоимость транзакции (> 0.05s) ИЛИ очень высокая задержка
        is_heavy_query = (tx_cost > 0.05) or (metrics["Max Latency (s)"] > 1.0) # Подняли порог задержки до 1.0с

        if (rw_ratio > 50) or (is_heavy_query and tps < 100):
            # Защита от ложного срабатывания в фоне:
            # Если TPS экстремально низкий (< 5), считаем это OLAP только если
            # реально загружен CPU (db_time_rate > 1.0). Иначе это просто "шум".
            if tps < 5.0 and db_time_rate < 1.0:
                 return "IDLE", "Low", metrics # Скорее всего просто медленный фоновый процесс

            # Если CPU реально пашет:
            if io_waits > avg_active_sessions * 0.3:
                return "Disk-Bound OLAP", "High", metrics
            else:
                return "Heavy OLAP", "High", metrics

        # 3. Mixed / HTAP (Смешанная нагрузка)
        # Доля вставок от 30% до 65% (активное изменение данных + чтение)
        if 0.30 <= insert_ratio <= 0.65:
            return "Mixed / HTAP", "Medium", metrics

        # [FIX] 4. OLTP vs TPC-C (Интенсивное обновление)
        # Доля вставок низкая (много UPDATE, мало INSERT)
        if insert_ratio < 0.30:
            # Разделяем по "тяжести" транзакции.
            # Classic OLTP (pgbench) обычно очень быстрый (Cost < 0.010s)
            # TPC-C содержит сложные join'ы и логику (Cost > 0.012s)

            # Ранее порог был 0.002, что слишком мало для Docker/Virtual. Подняли до 0.012 (12мс).
            if tx_cost > 0.012:
                return "TPC-C OLTP", "Medium", metrics
            else:
                return "Classic OLTP", "High", metrics

        # Fallback (Если нагрузка есть, но не попадает четко в категории)
        if tps > 5:
            return "Mixed / HTAP", "Low", metrics

        return "IDLE", "Low", metrics
