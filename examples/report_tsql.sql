-- monthly report (SQL Server)
SELECT TOP 10
    c.[CustomerName],
    ISNULL(o.Region, 'n/a') AS region,
    CONVERT(VARCHAR(10), o.OrderDate, 120) AS d,
    DATEADD(month, 1, o.OrderDate) AS next_month,
    DATEDIFF(day, o.OrderDate, GETDATE()) AS age,
    STRING_AGG(p.Name, ', ') AS products,
    LEN(c.CustomerName) AS n
FROM dbo.Orders o WITH (NOLOCK)
JOIN dbo.Customers c ON c.Id = o.CustomerId
JOIN products p ON p.Id = o.ProductId
GROUP BY c.[CustomerName], o.Region, o.OrderDate
ORDER BY age DESC;
