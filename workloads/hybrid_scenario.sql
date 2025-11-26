-- Mixed workload simulating real-world scenario
\timing on

-- OLTP-style short transactions
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE account_id = 1;
UPDATE accounts SET balance = balance + 100 WHERE account_id = 2;
INSERT INTO transactions (from_account, to_account, amount, timestamp)
VALUES (1, 2, 100, NOW());
COMMIT;

-- OLAP-style analytical query
SELECT
    product_id,
    COUNT(*) as sales_count,
    SUM(quantity * price) as total_revenue
FROM order_items
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 10;

-- Mixed workload: real-time analytics on recent data
SELECT
    customer_id,
    COUNT(*) as recent_orders,
    AVG(order_total) as avg_order_size
FROM orders
WHERE order_date > NOW() - INTERVAL '1 hour'
GROUP BY customer_id
HAVING COUNT(*) > 1;

\timing off
