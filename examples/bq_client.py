from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project="my-proj")

SQL = """
SELECT o.customer_id, SUM(o.amount) AS revenue, COUNTIF(o.status = 'ok') AS ok_orders,
       DATE_TRUNC(o.order_date, MONTH) AS month
FROM `my-proj.sales.orders` o
WHERE o.order_date >= @start
GROUP BY customer_id, month
"""
df = client.query(SQL).to_dataframe()
df = df[df['revenue'] > 100]
df['tier'] = df['revenue'].apply(lambda x: 'gold' if x > 1000 else 'basic')
big = df.groupby('month').agg(total=('revenue', 'sum')).reset_index()
client.load_table_from_dataframe(big, "my-proj.marts.monthly_revenue", job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()
client.delete_table("my-proj.tmp.x")
