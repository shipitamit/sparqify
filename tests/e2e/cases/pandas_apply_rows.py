import pandas as pd
df = pd.read_csv("sales.csv", parse_dates=["dt"])
df["total"] = df.apply(lambda r: r["amount"] * r["qty"], axis=1)
df["size"] = df.apply(lambda r: "big" if r["amount"] > 250 else "small", axis=1)
df["label"] = df.apply(lambda r: r["region"].upper() + "-" + str(r["id"]), axis=1)
df["flag"] = df.apply(lambda r: 1 if r["qty"] > 3 and r["region"] == "east" else 0, axis=1)
df["adj"] = df.apply(lambda r: r["amount"] * 2 if r["qty"] > 5 or r["region"] == "north" else r["amount"], axis=1)
out = df[["id", "total", "size", "label", "flag", "adj"]]
