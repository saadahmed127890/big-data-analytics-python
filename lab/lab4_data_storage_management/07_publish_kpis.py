import duckdb
from pathlib import Path

out = Path("lake/serving/kpis")
out.mkdir(parents=True, exist_ok=True)

path = "lake/curated/events_parquet/partitioned/**/*.parquet"
con = duckdb.connect(database=":memory:")

rev = con.execute("""
SELECT country,
       ROUND(SUM(price), 2) AS revenue,
       COUNT(*) FILTER (WHERE event_type='purchase') AS purchases,
       ROUND(AVG(price), 2) AS aov
FROM read_parquet(?)
WHERE event_type='purchase'
GROUP BY country
ORDER BY revenue DESC;
""", [path]).fetchdf()

top_products = con.execute("""
SELECT product_id,
       ROUND(SUM(price), 2) AS revenue,
       COUNT(*) AS num_purchases
FROM read_parquet(?)
WHERE event_type='purchase'
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;
""", [path]).fetchdf()

rev.to_csv(out / "revenue_by_country.csv", index=False)
top_products.to_csv(out / "top_products.csv", index=False)

print("Published KPI tables to:", out)
print("Files:", [p.name for p in out.glob("*.csv")])
