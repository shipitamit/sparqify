import polars as pl

df = pl.scan_csv('events.csv')
out = (
    df.filter(pl.col('amount') > 100)
      .with_columns(
          (pl.col('amount') * pl.col('qty')).alias('total'),
          pl.when(pl.col('amount') > 500).then(pl.lit('big')).otherwise(pl.lit('small')).alias('tier'),
          pl.col('ts').dt.year().alias('yr'),
      )
      .group_by('region', 'tier')
      .agg(pl.col('total').sum().alias('revenue'), pl.len().alias('n'))
      .sort('revenue', descending=True)
      .collect()
)
out.write_parquet('out.parquet')
