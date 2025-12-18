import json
from datetime import datetime
from src.db_utils import get_db_connection, create_tables


def compute_daily_analytics(db_path='data/app.db'):
    
    create_tables(db_path)
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) FROM events")
        total_events = cursor.fetchone()[0]
        print(f"total evennts {total_events}")
        
        if total_events == 0:
            print("no events to analyze")
            return 0
        
        cursor.execute("""
            SELECT 
                DATE(datetime) as event_date,
                article_uri,
                lang,
                sentiment,
                source_uri
            FROM events
            WHERE datetime IS NOT NULL AND datetime != ''
        """)
        
        rows = cursor.fetchall()
        
        if not rows:
            print("no rows for analysis")
            return 0

        date_groups = {}
        for row in rows:
            event_date = row[0]
            if event_date not in date_groups:
                date_groups[event_date] = []
            date_groups[event_date].append({
                'article_uri': row[1],
                'lang': row[2],
                'sentiment': row[3],
                'source_uri': row[4]
            })
        
        print(f"we found {len(date_groups)} uniques")
      
        summaries_created = 0
        for event_date, articles in date_groups.items():
            print(f" date - {event_date}")
            
            total_articles = len(articles)
            print(f"total - {total_articles}")
            
            sentiments = [a['sentiment'] for a in articles if a['sentiment'] is not None]
            if sentiments:
                avg_sentiment = sum(sentiments) / len(sentiments)
                min_sentiment = min(sentiments)
                max_sentiment = max(sentiments)
            else:
                avg_sentiment = 0.0
                min_sentiment = 0.0
                max_sentiment = 0.0
            
            print(f"mean - {avg_sentiment:.3f}, min - {min_sentiment:.3f}, max - {max_sentiment:.3f}")
            
            source_counts = {}
            for article in articles:
                source = article['source_uri'] if article['source_uri'] else 'unknown'
                source_counts[source] = source_counts.get(source, 0) + 1
            
            top_source = max(source_counts.items(), key=lambda x: x[1])[0] if source_counts else 'unknown'
            print(f"top surce - {top_source} ({source_counts.get(top_source, 0)} ")
            
            lang_counts = {}
            for article in articles:
                lang = article['lang'] if article['lang'] else 'unknown'
                lang_counts[lang] = lang_counts.get(lang, 0) + 1
            
            language_distribution = json.dumps(lang_counts, sort_keys=True)
            print(f"lang distribution - {language_distribution}")
            
            cursor.execute("""
                INSERT OR REPLACE INTO daily_summary 
                (summary_date, total_articles, avg_sentiment, min_sentiment, max_sentiment, 
                 top_source, language_distribution, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                event_date,
                total_articles,
                avg_sentiment,
                min_sentiment,
                max_sentiment,
                top_source,
                language_distribution
            ))
            
            conn.commit()
            summaries_created += 1
            print(f"summary saved {event_date}")
        
        print(f" {summaries_created} date processed")
        
        cursor.execute("""
            SELECT summary_date, total_articles, avg_sentiment, top_source, language_distribution
            FROM daily_summary
            ORDER BY summary_date DESC
            LIMIT 10
        """)
        
        summary_rows = cursor.fetchall()
        for row in summary_rows:
            print(f"  date {row[0]}")
            print(f"  total {row[1]}")
            print(f"  mean sentiment {row[2]:.3f}")
            print(f"  top surce {row[3]}")
            print(f"  langs {row[4]}")
  
        
        return summaries_created
        
    except Exception as e:
        print(f"error in analytics - {e}")
        import traceback
        traceback.print_exc()
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    compute_daily_analytics(db_path='data/app.db')

