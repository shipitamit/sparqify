SELECT TOP 5 c.name, ISNULL(SUM(o.amount),0) AS total, DATEDIFF(day, MIN(o.dt), GETDATE()) AS age
FROM dbo.customers c LEFT JOIN dbo.orders o ON o.cid = c.id
WHERE o.dt >= DATEADD(month,-3,GETDATE()) AND c.name LIKE 'A%'
GROUP BY c.name HAVING SUM(o.amount) > 100 ORDER BY total DESC;
