import dask.dataframe as dd
df = dd.read_csv("sales-*.csv")
out = df[df.amount > 100].groupby("region").amount.sum().compute()
out.to_csv("out.csv")
