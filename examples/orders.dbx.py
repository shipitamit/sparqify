# Databricks notebook source
# MAGIC %md
# MAGIC # Orders

# COMMAND ----------

import pandas as pd
df = pd.read_csv("/dbfs/tmp/orders.csv")
df["total"] = df["amount"] * df["qty"]
df = df[df["total"] > 0]

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, SUM(total) AS revenue FROM orders GROUP BY region

# COMMAND ----------

print(df.groupby("region")["total"].sum())
