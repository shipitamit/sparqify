SELECT region, try_divide(SUM(amount), SUM(qty)) AS unit, count_if(qty > 5) AS big_orders, max_by(id, amount) AS top_id
FROM sales
GROUP BY region
ORDER BY region;

SELECT id, date_trunc('MONTH', dt) AS m, nvl(customer, 'none') AS c, date_format(dt, 'yyyy-MM') AS ym
FROM sales
WHERE id <= 5
ORDER BY id;

SELECT region, array_join(sort_array(collect_list(customer)), ',') AS cs, size(collect_set(customer)) AS buyers
FROM sales
WHERE id <= 12
GROUP BY region
ORDER BY region;
