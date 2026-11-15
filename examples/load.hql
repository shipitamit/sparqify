-- nightly load
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.reduce.tasks=20;
SET hivevar:run_date=2024-01-01;
USE sales;
ADD JAR /opt/udf/my-udf.jar;
INSERT OVERWRITE TABLE daily_orders PARTITION (dt)
SELECT o.order_id, o.customer_id, SUM(o.amount) AS amt, o.dt
FROM orders o
LATERAL VIEW explode(o.items) t AS item
WHERE o.dt = '${hivevar:run_date}'
GROUP BY o.order_id, o.customer_id, o.dt
DISTRIBUTE BY o.dt;
COMPUTE STATS daily_orders;
INVALIDATE METADATA daily_orders;
MSCK REPAIR TABLE daily_orders;
UPSERT INTO kudu_t SELECT * FROM daily_orders;
