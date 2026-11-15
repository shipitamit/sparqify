import pandas as pd
df = pd.read_csv("sales.csv", parse_dates=["dt"])
df = df.sort_values(["customer", "id"])
df["r_avg"] = df.groupby("region")["qty"].rank()
df["r_max"] = df.groupby("region")["qty"].rank(method="max", ascending=False)
df["r_all"] = df["qty"].rank(method="average")
small = df[df["qty"] <= 3].copy()
small["prod"] = small.groupby("customer")["qty"].cumprod()
res = small[["id", "customer", "qty", "prod"]]
ranks = df[["id", "region", "qty", "r_avg", "r_max", "r_all"]]
