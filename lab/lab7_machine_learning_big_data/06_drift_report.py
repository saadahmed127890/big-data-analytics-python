
import duckdb
import pandas as pd

con=duckdb.connect(database=":memory:")

df=con.execute("SELECT * FROM read_parquet('data/serving/session_scores.parquet')").fetchdf()

def slice_stats(name,sdf):
    return pd.DataFrame([{
        "slice":name,
        "rows":len(sdf),
        "avg_score":float(sdf["p1"].mean())
    }])

overall=slice_stats("overall",df)

out=overall
print(out)

out.to_csv("data/serving/drift_report.csv",index=False)
