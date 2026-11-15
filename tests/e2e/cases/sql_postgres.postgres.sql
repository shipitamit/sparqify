SELECT region, DATE_TRUNC('month', CAST(dt AS DATE)) AS month, SUM(amount)::numeric(12,2) AS total
FROM sales
GROUP BY region, DATE_TRUNC('month', CAST(dt AS DATE))
ORDER BY region, month;

SELECT id, customer
FROM sales
WHERE customer ILIKE 'c1%'
ORDER BY id;
