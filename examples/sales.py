import pandas as pd
import numpy as np

df = pd.read_csv('sales.csv', sep=',')
customers = pd.read_parquet('customers.parquet')

df = df[df['amount'] > 100]
df = df.dropna(subset=['region', 'qty'])
df['total'] = df['amount'] * df['qty']
df['tier'] = np.where(df['total'] > 1000, 'gold', 'basic')
df['year'] = pd.to_datetime(df['order_date']).dt.year
df['label'] = df['region'].str.upper() + '-' + df['tier']
df.loc[df['qty'] == 0, 'total'] = 0

merged = df.merge(customers, on='customer_id', how='left')
out = (merged.groupby('region')
             .agg(revenue=('total', 'sum'), orders=('order_id', 'count'), avg_qty=('qty', 'mean'))
             .reset_index())
out = out.sort_values('revenue', ascending=False).head(10)
print(len(out))
print(merged['total'].sum())
counts = merged['tier'].value_counts()
df.rename(columns={'amount': 'amt'}, inplace=True)
out.to_csv('out.csv', index=False)

def clean(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.drop_duplicates(subset=['order_id'])
    return frame.assign(big=lambda d: d['amt'] > 500)

for i, row in df.iterrows():
    df.loc[i, 'amt2'] = row['amt'] * 2
