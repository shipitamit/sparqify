import pandas as pd
df = pd.read_csv("events.csv").sort_values(["user", "ts"])
df["prev"] = df.groupby("user")["amt"].shift(1)
df["run"] = df.groupby("user")["amt"].cumsum()
df["ma3"] = df["amt"].rolling(3).mean()
for i, row in df.iterrows():
    print(row["user"], row["amt"])
