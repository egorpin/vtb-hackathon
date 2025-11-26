class ProfileAnalyzer:
    def analyze(self, prev, curr, duration):
        # Вычисляем дельты
        d_commits = curr["commits"] - prev["commits"]
        d_inserted = curr["tup_inserted"] - prev["tup_inserted"]
        d_fetched = curr["tup_fetched"] - prev["tup_fetched"]
        
        # Базовые метрики
        tps = d_commits / duration
        avg_active = (prev["active_sessions"] + curr["active_sessions"]) / 2
        
        # Основная формула: Стоимость одной транзакции в секундах работы БД
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

        # --- ДЕРЕВО РЕШЕНИЙ ---
        profile = "Unknown"
        confidence = "Low"

        if avg_active < 1 and tps < 5:
            profile = "IDLE"
            confidence = "High"
        
        elif d_inserted > 5000 and d_fetched < 100:
            profile = "Bulk Load"
            confidence = "High"

        elif tps > 50 and tx_cost < 0.05:
            # Логика IoT закомментирована в твоем коде, можешь раскомментировать при желании
            # if d_inserted > tps * 0.8: 
            #    profile = "IoT / Ingestion"
            # else:
            profile = "Classic OLTP"
            confidence = "High"

        elif tx_cost > 0.5 or curr["max_duration"] > 2.0:
            if io_waits > 0:
                profile = "Heavy OLAP (Disk Bound)"
            else:
                profile = "Heavy OLAP (CPU/Mem Bound)"
            confidence = "Medium"
            
            if tps > 10:
                profile = "Mixed / HTAP"
                confidence = "Medium"

        elif read_write_ratio > 100 and tps > 5:
            profile = "Web / Read-Only"
            confidence = "High"
            
        else:
            profile = "Mixed / HTAP"
            confidence = "Low"

        return profile, confidence, metrics