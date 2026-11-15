SELECT region, COUNT(*) AS n, SUM(amount) AS total
FROM sales
WHERE qty >= 2
GROUP BY region
ORDER BY total DESC;

SELECT s.id, s.amount, r.manager
FROM sales s JOIN regions r ON s.region = r.region
WHERE s.amount > 400;

SELECT customer, COALESCE(SUM(amount), 0) AS spend,
       ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) AS rk
FROM sales
GROUP BY customer;
