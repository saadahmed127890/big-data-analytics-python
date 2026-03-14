
import json
from pathlib import Path
import duckdb

Path("data/curated").mkdir(parents=True,exist_ok=True)
Path("data/serving").mkdir(parents=True,exist_ok=True)

con=duckdb.connect(database="data/topic8.duckdb")

raw_path="data/raw/*.jsonl"

con.execute("DROP VIEW IF EXISTS raw_events;")
con.execute(f'''
CREATE VIEW raw_events AS
SELECT *
FROM read_json_auto('{raw_path}',format='newline_delimited');
''')

con.execute("DROP VIEW IF EXISTS clean_events;")
con.execute("""
CREATE VIEW clean_events AS
SELECT *
FROM raw_events
WHERE event_id IS NOT NULL
AND user_id IS NOT NULL
AND session_id IS NOT NULL;
""")

con.execute("DROP VIEW IF EXISTS events_no_bots;")
con.execute("""
CREATE VIEW events_no_bots AS
SELECT *
FROM clean_events
WHERE lower(user_agent) NOT LIKE '%bot%';
""")

with open("data/reference/product_catalog.json","r",encoding="utf-8") as f:
    catalog=json.load(f)

con.execute("DROP TABLE IF EXISTS product_catalog;")
con.execute("CREATE TABLE product_catalog(product_id VARCHAR,category VARCHAR,brand VARCHAR);")

con.executemany("INSERT INTO product_catalog VALUES (?,?,?);",
[(p["product_id"],p["category"],p["brand"]) for p in catalog])

con.execute("DROP VIEW IF EXISTS enriched_events;")
con.execute("""
CREATE VIEW enriched_events AS
SELECT e.*,p.category,p.brand
FROM events_no_bots e
LEFT JOIN product_catalog p USING(product_id);
""")

con.execute("DROP TABLE IF EXISTS kpi_daily;")
con.execute("""
CREATE TABLE kpi_daily AS
SELECT
date_trunc('day',event_ts) AS event_date,
COUNT(DISTINCT session_id) sessions,
SUM(CASE WHEN event_type='purchase' THEN price*qty ELSE 0 END) revenue
FROM enriched_events
GROUP BY event_date
ORDER BY event_date;
""")

con.execute("COPY kpi_daily TO 'data/serving/kpi_daily.parquet' (FORMAT 'parquet');")

print("Serving tables built")
