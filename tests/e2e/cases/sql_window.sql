SELECT id, region, amount,
       SUM(amount) OVER (PARTITION BY region ORDER BY id) AS running,
       RANK() OVER (PARTITION BY region ORDER BY amount DESC, id) AS rk,
       LAG(amount) OVER (PARTITION BY region ORDER BY id) AS prev_amount
FROM sales;

SELECT region, customer, SUM(qty) AS q
FROM sales
GROUP BY region, customer
HAVING SUM(qty) > 5
ORDER BY region, customer;
