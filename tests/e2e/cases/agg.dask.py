import dask.dataframe as dd
df = dd.read_csv("sales.csv")
df["net"] = df["amount"] * 0.9
agg = df.groupby("region").agg({"net": "sum", "qty": "max"}).reset_index()
big = df[df["qty"] > 3][["id", "region", "amount"]]
