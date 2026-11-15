SEL TOP 3 region, SUM(amt) FROM sales WHERE sale_dt > DATE '2024-01-01' GROUP BY 1 QUALIFY RANK(SUM(amt) DESC) <= 3;
