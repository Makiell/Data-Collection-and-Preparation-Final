import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
from datetime import datetime
import os


def create_visualizations(db_path='data/app.db', output_dir='plots'):

    os.makedirs(output_dir, exist_ok=True)
    
    conn = sqlite3.connect(db_path)

    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = (12, 6)
    plt.rcParams['font.size'] = 10

    df_summary = pd.read_sql_query(
        "SELECT * FROM daily_summary ORDER BY summary_date", 
        conn
    )
    
    df_events = pd.read_sql_query(
        "SELECT * FROM events WHERE datetime IS NOT NULL AND datetime != ''", 
        conn
    )
    
    if df_summary.empty:
        print("no data in daily_summary table, run third job")
        return

    df_summary['summary_date'] = pd.to_datetime(df_summary['summary_date'])

    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(df_summary['summary_date'], df_summary['avg_sentiment'], 
            marker='o', linewidth=2, markersize=8, label='average sentiment', color='#2E86AB')
    ax.fill_between(df_summary['summary_date'], 
                     df_summary['min_sentiment'], 
                     df_summary['max_sentiment'], 
                     alpha=0.3, color='#A23B72', label='min-max range')
    
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.5)
    ax.set_xlabel('date', fontsize=12, fontweight='bold')
    ax.set_ylabel('sentiment score', fontsize=12, fontweight='bold')
    ax.set_title('article sentiment trends over time', fontsize=14, fontweight='bold', pad=20)
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/sentiment_trends.png', dpi=300, bbox_inches='tight')
    plt.close()
 
    fig, ax = plt.subplots(figsize=(12, 6))
    
    bars = ax.bar(df_summary['summary_date'], df_summary['total_articles'], 
                   color='#F18F01', alpha=0.8, edgecolor='black', linewidth=1.2)

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    ax.set_xlabel('date', fontsize=12, fontweight='bold')
    ax.set_ylabel('number of articles', fontsize=12, fontweight='bold')
    ax.set_title('daily article collection volume', fontsize=14, fontweight='bold', pad=20)
    ax.grid(True, alpha=0.3, axis='y')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/daily_volume.png', dpi=300, bbox_inches='tight')
    plt.close()

    all_langs = {}
    for lang_json in df_summary['language_distribution']:
        lang_dict = json.loads(lang_json)
        for lang, count in lang_dict.items():
            all_langs[lang] = all_langs.get(lang, 0) + count
    
    sorted_langs = sorted(all_langs.items(), key=lambda x: x[1], reverse=True)
    languages = [item[0] for item in sorted_langs]
    counts = [item[1] for item in sorted_langs]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    colors = sns.color_palette("husl", len(languages))
    bars = ax1.bar(languages, counts, color=colors, alpha=0.8, edgecolor='black', linewidth=1.2)
    
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    ax1.set_xlabel('language', fontsize=12, fontweight='bold')
    ax1.set_ylabel('article count', fontsize=12, fontweight='bold')
    ax1.set_title('articles by language bar chart)', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='y')
    
    ax2.pie(counts, labels=languages, autopct='%1.1f%%', startangle=90,
            colors=colors, textprops={'fontsize': 10, 'fontweight': 'bold'})
    ax2.set_title('language distribution pie chart', fontsize=12, fontweight='bold')
    
    plt.suptitle('overall language distribution across all articles', 
                 fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/language_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()

    if not df_events.empty:
        source_counts = df_events['source_uri'].value_counts().head(10)
        
        fig, ax = plt.subplots(figsize=(12, 7))
        
        bars = ax.barh(range(len(source_counts)), source_counts.values, 
                       color=sns.color_palette("viridis", len(source_counts)), 
                       alpha=0.8, edgecolor='black', linewidth=1.2)
        
        ax.set_yticks(range(len(source_counts)))
        ax.set_yticklabels(source_counts.index, fontsize=10)
        ax.set_xlabel('number of articles', fontsize=12, fontweight='bold')
        ax.set_ylabel('source', fontsize=12, fontweight='bold')
        ax.set_title('top 10 news sources by article count', fontsize=14, fontweight='bold', pad=20)
        
        for i, (bar, value) in enumerate(zip(bars, source_counts.values)):
            ax.text(value, i, f' {int(value)}', 
                   va='center', fontsize=10, fontweight='bold')
        
        ax.grid(True, alpha=0.3, axis='x')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/top_sources.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    if not df_events.empty and 'sentiment' in df_events.columns:
        fig, ax = plt.subplots(figsize=(12, 6))
        
        sentiments = df_events['sentiment'].dropna()
        
        ax.hist(sentiments, bins=30, color='#C73E1D', alpha=0.7, 
               edgecolor='black', linewidth=1.2)
        ax.axvline(sentiments.mean(), color='blue', linestyle='--', 
                  linewidth=2, label=f'Mean: {sentiments.mean():.3f}')
        ax.axvline(0, color='gray', linestyle='--', linewidth=1, alpha=0.5)
        
        ax.set_xlabel('sentiment score', fontsize=12, fontweight='bold')
        ax.set_ylabel('frequency', fontsize=12, fontweight='bold')
        ax.set_title('distribution of article sentiment scores', fontsize=14, fontweight='bold', pad=20)
        ax.legend(fontsize=10)
        ax.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/sentiment_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    lang_by_date = []
    for _, row in df_summary.iterrows():
        lang_dict = json.loads(row['language_distribution'])
        for lang, count in lang_dict.items():
            lang_by_date.append({
                'date': row['summary_date'].strftime('%Y-%m-%d'),
                'language': lang,
                'count': count
            })
    
    df_heatmap = pd.DataFrame(lang_by_date)
    pivot_table = df_heatmap.pivot(index='language', columns='date', values='count').fillna(0)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.heatmap(pivot_table, annot=True, fmt='.0f', cmap='YlOrRd', 
                linewidths=1, linecolor='gray', cbar_kws={'label': 'Article Count'},
                ax=ax)
    ax.set_xlabel('date', fontsize=12, fontweight='bold')
    ax.set_ylabel('language', fontsize=12, fontweight='bold')
    ax.set_title('language distribution across dates heatmap', fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/language_heatmap.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    conn.close()
    

if __name__ == '__main__':
    create_visualizations(db_path='data/app.db', output_dir='plots')

