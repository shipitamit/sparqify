WITH per_customer AS (
  SELECT customer, SUM(amount) AS spend, COUNT(*) AS orders
  FROM sales
  GROUP BY customer
)
SELECT p.customer, c.segment, p.spend,
       CASE WHEN p.spend > 1000 THEN 'high' WHEN p.spend > 400 THEN 'mid' ELSE 'low' END AS band
FROM per_customer p
LEFT JOIN customers c ON p.customer = c.customer
ORDER BY p.customer;

SELECT region, COUNT(DISTINCT customer) AS buyers
FROM sales
WHERE dt >= '2024-02-01'
GROUP BY region
ORDER BY region;
