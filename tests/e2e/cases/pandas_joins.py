import pandas as pd
sales = pd.read_csv("sales.csv")
customers = pd.read_csv("customers.csv")
regions = pd.read_csv("regions.csv")
inner = sales.merge(customers, on="customer", how="inner")
outer = sales.merge(regions, on="region", how="outer")
agg = inner.groupby(["segment", "region"]).agg(total=("amount", "sum"), avg_qty=("qty", "mean"), biggest=("amount", "max")).reset_index()
agg = agg.sort_values("total", ascending=False).head(5)
renamed = regions.rename(columns={"manager": "owner"})[["region", "owner"]]
