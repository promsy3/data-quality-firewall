import os
import shutil
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# Note: make sure 'engine' is defined and exported in your db.py
from db import init_db, log_event, log_scan, engine 
from quality_checks import check_nulls, check_outliers
from report_gen import generate_pdf_report

# ── Directory layout ──────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
INCOMING_DIR = BASE_DIR / "incoming"
PROCESSED_DIR= BASE_DIR / "processed"
REPORTS_DIR  = BASE_DIR / "reports"
LOGS_DIR     = BASE_DIR / "logs"
DB_PATH      = LOGS_DIR / "logs.db"

for d in (INCOMING_DIR, PROCESSED_DIR, REPORTS_DIR, LOGS_DIR):
    d.mkdir(exist_ok=True)

executor = ThreadPoolExecutor(max_workers=4)
in_flight: set[str] = set()           
in_flight_lock = threading.Lock()

def process_csv(filepath: str) -> dict:
    path = Path(filepath)
    filename = path.name
    started = datetime.now()
    conn = sqlite3.connect(DB_PATH)

    result = {
        "filename": filename,
        "started": started.isoformat(),
        "rows": 0, "columns": 0,
        "null_issues": [], "outlier_issues": [],
        "passed": False, "error": None,
    }

    try:
        df = pd.read_csv(filepath)
        result["rows"]    = len(df)
        result["columns"] = len(df.columns)

        log_event(conn, filename, "INFO", f"Loaded — {result['rows']} rows")

        with ThreadPoolExecutor(max_workers=2) as qc_pool:
            null_future    = qc_pool.submit(check_nulls,    df, filename, conn)
            outlier_future = qc_pool.submit(check_outliers, df, filename, conn)

        result["null_issues"]    = null_future.result()
        result["outlier_issues"] = outlier_future.result()
        all_issues = result["null_issues"] + result["outlier_issues"]

        if not all_issues:
            # ─── PUSH TO MYSQL WORKBENCH ───
            df.to_sql('employee_demographics', con=engine, if_exists='append', index=False)
            
            dest = PROCESSED_DIR / filename
            shutil.move(str(path), str(dest))
            result["passed"] = True
            log_event(conn, filename, "PASS", "Pushed to MySQL & moved.")
        else:
            for issue in all_issues:
                log_event(conn, filename, "FAIL", issue)

    except Exception as exc:
        result["error"] = str(exc)
        log_event(conn, filename, "ERROR", str(exc))

    finally:
        result["finished"] = datetime.now().isoformat()
        duration = (datetime.now() - started).total_seconds()
        log_scan(conn, result, duration)
        conn.close()

    pdf_path = REPORTS_DIR / f"report_{Path(filename).stem}_{started.strftime('%Y%m%d_%H%M%S')}.pdf"
    generate_pdf_report(result, str(pdf_path))
    print(f"[Firewall] {'✅ PASSED' if result['passed'] else '❌ REJECTED'} {filename}")
    return result

class CSVHandler(FileSystemEventHandler):
    def on_created(self, event):
        path = str(event.src_path)
        if not event.is_directory and path.lower().endswith(".csv"):
            self._enqueue(path)

    def on_moved(self, event):
        path = str(event.dest_path)
        if not event.is_directory and path.lower().endswith(".csv"):
            self._enqueue(path)

    def _enqueue(self, filepath: str):
        with in_flight_lock:
            if filepath in in_flight: return
            in_flight.add(filepath)
        time.sleep(0.5)
        future = executor.submit(process_csv, filepath)
        future.add_done_callback(lambda f: in_flight.discard(filepath))

def start_firewall():
    init_db(DB_PATH)
    print(f"FIREWALL ACTIVE. Watching: {INCOMING_DIR}")
    handler, observer = CSVHandler(), Observer()
    observer.schedule(handler, str(INCOMING_DIR), recursive=False)
    observer.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        executor.shutdown(wait=True)
    observer.join()

if __name__ == "__main__":
    start_firewall()
