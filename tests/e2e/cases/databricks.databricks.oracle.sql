SELECT region, SUM(amount) / NULLIF(SUM(qty), 0) AS unit, COUNT(*) FILTER (WHERE qty > 5) AS big_orders, ARG_MAX(id, amount) AS top_id
FROM sales GROUP BY region ORDER BY region;

SELECT id, DATE_TRUNC('month', CAST(dt AS DATE)) AS m, COALESCE(customer, 'none') AS c, STRFTIME(CAST(dt AS DATE), '%Y-%m') AS ym
FROM sales WHERE id <= 5 ORDER BY id;

SELECT region, STRING_AGG(customer, ',' ORDER BY customer) AS cs, COUNT(DISTINCT customer) AS buyers
FROM sales WHERE id <= 12 GROUP BY region ORDER BY region;
