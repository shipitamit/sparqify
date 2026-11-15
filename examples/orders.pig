-- revenue per customer
orders = LOAD '/data/orders.csv' USING PigStorage(',') AS (order_id:int, customer_id:int, region:chararray, amount:double, qty:int);
custs  = LOAD '/data/customers.csv' USING PigStorage(',') AS (customer_id:int, name:chararray);
big    = FILTER orders BY amount > 100 AND region == 'EU' AND region is not null;
calc   = FOREACH big GENERATE order_id, customer_id, amount * qty AS total, UPPER(region) AS region;
j      = JOIN calc BY customer_id LEFT OUTER, custs BY customer_id;
g      = GROUP j BY name;
agg    = FOREACH g GENERATE group, SUM(j.total) AS revenue, COUNT(j) AS n;
srt    = ORDER agg BY revenue DESC;
top    = LIMIT srt 10;
STORE top INTO '/out/top' USING PigStorage(',');
bad    = FOREACH orders GENERATE FLATTEN(TOKENIZE(region));
x      = FOREACH orders GENERATE MyUdf(amount);
