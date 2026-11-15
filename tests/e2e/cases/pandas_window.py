import pandas as pd
df = pd.read_csv("sales.csv", parse_dates=["dt"])
df = df.sort_values(["customer", "dt", "id"])
df["running"] = df.groupby("customer")["amount"].cumsum()
df["rank"] = df.groupby("region")["amount"].rank(method="first", ascending=False)
top = df[df["rank"] <= 3][["region", "id", "amount", "rank"]]
cust = df.merge(pd.read_csv("customers.csv"), on="customer")
seg = cust.groupby("segment")["amount"].mean().reset_index()
