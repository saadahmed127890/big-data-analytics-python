import duckdb

con = duckdb.connect(database=":memory:")
path = "lake/curated/events_parquet/partitioned/**/*.parquet"

q1 = """
SELECT country,
       ROUND(SUM(price), 2) AS revenue,
       COUNT(*) FILTER (WHERE event_type='purchase') AS purchases
FROM read_parquet(?)
WHERE event_type='purchase'
GROUP BY country
ORDER BY revenue DESC;
"""
print("Query 1: Revenue by country")
print(con.execute(q1, [path]).fetchdf())

q2 = """
SELECT event_date, country, COUNT(*) AS total_events
FROM read_parquet(?)
WHERE event_date = '2026-02-21' AND country = 'CA'
GROUP BY event_date, country;
"""
print("\nQuery 2: Filtered slice (2026-02-21, CA)")
print(con.execute(q2, [path]).fetchdf())
