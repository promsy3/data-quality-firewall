import sqlite3
from sqlalchemy import create_engine

# Keep your engine for SQLAlchemy needs
engine = create_engine("mysql+pymysql://root:$irsquad13@127.0.0.1:3306/parks_and_recreation")

def init_db(db_path):
    """Creates the necessary tables in SQLite if they don't exist."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            level TEXT,
            message TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            duration REAL,
            passed BOOLEAN,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def log_event(conn, filename, level, message):
    """Logs a specific event (INFO, PASS, FAIL) to the database."""
    cursor = conn.cursor()
    cursor.execute("INSERT INTO events (filename, level, message) VALUES (?, ?, ?)", 
                   (filename, level, message))
    conn.commit()

def log_scan(conn, result, duration):
    """Logs the final summary of a file scan."""
    cursor = conn.cursor()
    cursor.execute("INSERT INTO scans (filename, duration, passed) VALUES (?, ?, ?)", 
                   (result['filename'], duration, result['passed']))
    conn.commit()
