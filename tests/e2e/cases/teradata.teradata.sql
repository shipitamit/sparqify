SEL region, ZEROIFNULL(SUM(amount)) AS total, SUM(amount) / NULLIFZERO(COUNT(*)) AS avg_amount
FROM sales
GROUP BY region
ORDER BY region;

SEL id, region, amount
FROM sales
QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC, id) <= 2
ORDER BY region, id;

SELECT TOP 5 id, amount FROM sales ORDER BY amount DESC, id;

SEL customer, SUBSTR(customer, 1, 1) || '-' || CAST(qty AS VARCHAR(5)) AS tag, INDEX(customer, '1') AS pos
FROM sales WHERE id <= 5 ORDER BY id;
