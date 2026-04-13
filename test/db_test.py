import os
import psycopg2

conn_str = os.environ.get(
    "LANDING_DB_CONN_LOCAL",
    "postgresql://landing:landing@localhost:5433/landing_zone",
)

print("============= Connection Test =============")
try:
    conn = psycopg2.connect(conn_str)
    print(f"Connected to: {conn_str}")
except Exception as e:
    print(f"Connection failed: {e}")
    exit(1)

cur = conn.cursor()

print("\n============= Tables =============")
cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
""")
tables = cur.fetchall()
if tables:
    for t in tables:
        print(f"  - {t[0]}")
else:
    print("  (no tables found)")

print("\n============= yfinance_raw =============")
cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'yfinance_raw')")
if cur.fetchone()[0]:
    cur.execute("SELECT count(*) FROM yfinance_raw")
    count = cur.fetchone()[0]
    print(f"  Rows: {count}")

    if count > 0:
        cur.execute("SELECT ticker, extracted_at FROM yfinance_raw ORDER BY extracted_at DESC LIMIT 5")
        rows = cur.fetchall()
        print("  Latest:")
        for r in rows:
            print(f"    {r[0]} — {r[1]}")
else:
    print("  (table does not exist)")

print("\n============= yfinance_run_metadata =============")
cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'yfinance_run_metadata')")
if cur.fetchone()[0]:
    cur.execute("SELECT count(*) FROM yfinance_run_metadata")
    count = cur.fetchone()[0]
    print(f"  Rows: {count}")

    if count > 0:
        cur.execute("SELECT run_at, tickers_count, latest_price_date, rows_inserted FROM yfinance_run_metadata ORDER BY run_at DESC LIMIT 5")
        rows = cur.fetchall()
        print("  Latest runs:")
        for r in rows:
            print(f"    {r[0]} — tickers: {r[1]}, price_date: {r[2]}, inserted: {r[3]}")
else:
    print("  (table does not exist)")

cur.close()
conn.close()
print("\n============= Done =============")
