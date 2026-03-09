
import os
import pandas as pd
from database import get_database

def check_database():
    print("Checking Database Connection...")
    try:
        db = get_database()
        print(f"Database Type: {db.db_type}")
        print(f"Database URL: {db.db_url}")
        
        count = db.get_leads_count()
        print(f"Total Leads: {count}")
        
        if count > 0:
            df = db.get_all_leads(limit=5)
            print("\nColumns found in database:")
            print(df.columns.tolist())
            print("\nSample Data:")
            print(df.head().to_dict('records'))
        else:
            print("Database is currently empty.")
            
    except Exception as e:
        print(f"Error checking database: {e}")
        
        # Fallback to local SQLite check if needed
        if os.path.exists('leads.db'):
            import sqlite3
            print("\nFound local leads.db, checking it...")
            try:
                conn = sqlite3.connect('leads.db')
                df = pd.read_sql_query("SELECT * FROM leads LIMIT 1", conn)
                print(f"SQLite Columns: {df.columns.tolist()}")
                conn.close()
            except Exception as se:
                print(f"SQLite check failed: {se}")

if __name__ == "__main__":
    check_database()
