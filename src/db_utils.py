import sqlite3


def get_db_connection(db_path='data/app.db'):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def create_tables(db_path='data/app.db'):
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_uri TEXT,
            lang TEXT,
            datetime TEXT,
            data_type TEXT,
            url TEXT,
            title TEXT,
            body TEXT,
            source_uri TEXT,
            image_url TEXT,
            sentiment REAL,
            wgt INTEGER,
            relevance INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            summary_date TEXT UNIQUE,
            total_articles INTEGER,
            avg_sentiment REAL,
            min_sentiment REAL,
            max_sentiment REAL,
            top_source TEXT,
            language_distribution TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Database tables created or verified.")
