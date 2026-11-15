import polars as pl
df = pl.read_csv("sales.csv")
df = df.with_columns((pl.col("amount") * 0.9).alias("net"))
agg = df.group_by("region").agg(pl.col("net").sum().alias("total"), pl.col("id").count().alias("n")).sort("region")
big = df.filter((pl.col("qty") > 3) & (pl.col("region") != "west")).select(["id", "region", "amount"])
