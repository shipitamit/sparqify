import dask.dataframe as dd

df = dd.read_csv('s3://bucket/events-*.csv')
df = df[df.amount > 100].repartition(npartitions=16)
res = df.groupby('region').amount.sum().compute()
print(df.amount.mean().compute())
