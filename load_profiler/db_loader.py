import psycopg2
from config import DB_CONFIG

def load_profiles_from_db():
    """Возвращает словарь: { 'ProfileName': {param: value} }"""
    recs = {}
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Проверяем наличие таблицы
        cur.execute("SELECT to_regclass('public.load_profiles');")
        if not cur.fetchone()[0]:
            conn.close()
            return {}

        cur.execute("SELECT profile_name, recommendations FROM load_profiles")
        rows = cur.fetchall()
        
        for name, rec_json in rows:
            recs[name] = rec_json
            
        conn.close()
        return recs
    except Exception:
        return {}