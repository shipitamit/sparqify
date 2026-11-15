SELECT region, SUM(amount) / NULLIF(SUM(qty), 0) AS unit, COUNT(*) FILTER (WHERE qty > 5) AS big_orders
FROM sales GROUP BY region ORDER BY region;

SELECT DATE_TRUNC('month', CAST(dt AS DATE)) AS month, SUM(amount) AS total
FROM sales GROUP BY 1 ORDER BY 1;

SELECT id, STRFTIME(CAST(dt AS DATE), '%Y-%m') AS ym, COALESCE(customer, 'none') AS c, EXTRACT(YEAR FROM CAST(dt AS DATE)) AS y
FROM sales WHERE id <= 5 ORDER BY id;

SELECT region, STRING_AGG(customer, ',' ORDER BY customer) AS cs
FROM (SELECT DISTINCT region, customer FROM sales WHERE id <= 12)
GROUP BY region ORDER BY region;
