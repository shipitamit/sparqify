import polars as pl
df = pl.read_csv("sales.csv")
out = df.filter(pl.col("amount") > 100).with_columns((pl.col("amount") * 0.9).alias("net")).group_by("region").agg(pl.col("net").sum().alias("total")).sort("total", descending=True)
out.write_parquet("out.parquet")
