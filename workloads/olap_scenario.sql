-- Heavy analytical queries for OLAP profile
\timing on

-- Create large analytics table if not exists
CREATE TABLE IF NOT EXISTS test_analytics AS
SELECT 
    generate_series(1, 100000) as id,
    md5(random()::text) as data,
    (random() * 1000)::numeric(10,2) as value,
    (random() * 100)::integer as category_id;

-- Query 1: Large table scan with complex aggregations
SELECT 
    category_id,
    COUNT(*) as total_rows,
    AVG(value) as avg_value,
    SUM(value) as sum_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    STDDEV(value) as std_value
FROM test_analytics 
GROUP BY category_id 
ORDER BY total_rows DESC;

-- Query 2: Window functions and analytical calculations
WITH ranked_data AS (
    SELECT 
        id,
        value,
        category_id,
        AVG(value) OVER (PARTITION BY category_id) as category_avg,
        RANK() OVER (PARTITION BY category_id ORDER BY value DESC) as value_rank,
        PERCENT_RANK() OVER (PARTITION BY category_id ORDER BY value) as value_percentile
    FROM test_analytics
    WHERE category_id <= 10
)
SELECT 
    category_id,
    COUNT(*) as records,
    AVG(value) as actual_avg,
    AVG(category_avg) as window_avg,
    MAX(value_rank) as max_rank
FROM ranked_data
GROUP BY category_id
ORDER BY category_id;

-- Query 3: Complex joins and subqueries
SELECT 
    c.customer_id,
    c.name,
    COUNT(o.order_id) as order_count,
    SUM(o.order_total) as total_spent,
    AVG(o.order_total) as avg_order_value,
    (SELECT AVG(order_total) FROM test_orders) as overall_avg
FROM test_customers c
LEFT JOIN test_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
HAVING COUNT(o.order_id) > 0
ORDER BY total_spent DESC
LIMIT 20;

\timing off
