SELECT region, SAFE_DIVIDE(SUM(amount), SUM(qty)) AS unit, COUNTIF(qty > 5) AS big_orders
FROM `sales`
GROUP BY region
ORDER BY region;

SELECT DATE_TRUNC(dt, MONTH) AS month, SUM(amount) AS total
FROM `sales`
GROUP BY month
ORDER BY month;

SELECT id, FORMAT_DATE('%Y-%m', dt) AS ym, IFNULL(customer, 'none') AS c, EXTRACT(YEAR FROM dt) AS y
FROM `sales`
WHERE id <= 5 ORDER BY id;

SELECT region, STRING_AGG(customer, ',' ORDER BY customer) AS cs
FROM (SELECT DISTINCT region, customer FROM `sales` WHERE id <= 12)
GROUP BY region ORDER BY region;
