SELECT region, COALESCE(SUM(amount), 0) AS total, SUM(amount) / NULLIF(COUNT(*), 0) AS avg_amount
FROM sales GROUP BY region ORDER BY region;

SELECT id, region, amount FROM sales
QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC, id) <= 2
ORDER BY region, id;

SELECT id, amount FROM sales ORDER BY amount DESC, id LIMIT 5;

SELECT customer, SUBSTR(customer, 1, 1) || '-' || CAST(qty AS VARCHAR) AS tag, STRPOS(customer, '1') AS pos
FROM sales WHERE id <= 5 ORDER BY id;
