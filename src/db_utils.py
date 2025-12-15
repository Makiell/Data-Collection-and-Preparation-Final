import sqlite3
from pathlib import Path

class DatabaseManager:
    def __init__(self, db_path="/opt/airflow/data/app.db"):
        self.db_path = Path(db_path)

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def create_events_table(self):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                uri TEXT PRIMARY KEY,
                title TEXT,
                url TEXT,
                source TEXT,
                published_at TEXT,
                language TEXT
            )
        """)
        conn.commit()
        conn.close()

    def insert_events(self, events: list) -> int:
        """
        Вставка списка событий. Используется INSERT OR IGNORE для предотвращения дубликатов по uri.
        """
        conn = self.get_connection()
        cur = conn.cursor()
        inserted = 0
        for e in events:
            cur.execute("""
                INSERT OR IGNORE INTO events
                (uri, title, url, source, published_at, language)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                str(e["uri"]), 
                e["title"],
                e["url"],
                e["source"],
                e["published_at"],
                e["language"]
            ))
            if cur.rowcount > 0:
                inserted += 1
        conn.commit()
        conn.close()
        return inserted

    def get_event_count(self) -> int:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM events")
        count = cur.fetchone()[0]
        conn.close()
        return count
