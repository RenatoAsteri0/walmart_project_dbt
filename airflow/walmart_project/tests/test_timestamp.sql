SELECT
    order_id,
    COUNT(*) AS quantidade
FROM walmart.gold.eph_orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY quantidade DESC;