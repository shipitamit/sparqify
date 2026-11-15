import pandas as pd
df = pd.read_csv("sales.csv", parse_dates=["dt"])
df["net"] = df["amount"] * 0.9
df["ym"] = df["dt"].dt.strftime("%Y-%m")
g = df.groupby(["region", "ym"]).agg(total=("net", "sum"), n=("net", "count")).reset_index()
out = g.merge(pd.read_csv("regions.csv"), on="region", how="left")
out["vs_target"] = out["total"] - out["target"]
big = df[(df["qty"] > 3) & (df["region"] != "west")][["id", "region", "amount"]]
