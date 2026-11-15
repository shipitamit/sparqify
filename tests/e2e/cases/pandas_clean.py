import pandas as pd
df = pd.read_csv("sales.csv", parse_dates=["dt"])
df["customer"] = df["customer"].str.upper()
df["is_big"] = df["amount"] > 250
df["tier"] = df["qty"].apply(lambda q: "bulk" if q >= 8 else "single")
df["month"] = df["dt"].dt.month
df["amount"] = df["amount"].fillna(0).round(1)
sel = df[df["region"].isin(["north", "south"]) & df["customer"].str.startswith("C1")]
dedup = df[["customer", "region"]].drop_duplicates().sort_values(["customer", "region"])
counts = df.groupby("tier")["id"].count().reset_index()
