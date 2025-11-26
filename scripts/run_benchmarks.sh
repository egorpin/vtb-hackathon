#!/bin/bash

set -e

METRICS_DIR="/results/metrics"
REPORTS_DIR="/results/reports"
mkdir -p $METRICS_DIR $REPORTS_DIR

echo "Starting comprehensive load testing..."

# Initialize database
./init_db.sh

# Function to collect metrics
collect_metrics() {
    local scenario=$1
    echo "Collecting metrics for scenario: $scenario"
    
    psql -h postgres -U postgres -d tpc_tests <<EOSQL > $METRICS_DIR/${scenario}_metrics.json 2>/dev/null || echo "{}"
SELECT json_build_object(
    'timestamp', now(),
    'scenario', '$scenario',
    'database_stats', (SELECT row_to_json(pg_stat_database) FROM pg_stat_database WHERE datname = 'tpc_tests'),
    'active_sessions', (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = 'tpc_tests'),
    'total_transactions', (SELECT xact_commit + xact_rollback FROM pg_stat_database WHERE datname = 'tpc_tests')
);
EOSQL
}

# Scenario 1: OLTP-like workload
echo "=== SCENARIO 1: OLTP-like Workload ==="
collect_metrics "before_oltp"

# Generate OLTP workload (inserts, updates, simple selects)
psql -h postgres -U postgres -d tpc_tests <<EOSQL > $METRICS_DIR/oltp_workload.log 2>&1
\timing on
DO \$\$
DECLARE
    i INTEGER;
    customer_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO customer_count FROM test_customers;
    
    FOR i IN 1..500 LOOP
        -- Insert new order
        INSERT INTO test_orders (customer_id, order_total, status)
        VALUES (
            (i % customer_count) + 1,
            (random() * 1000)::numeric(10,2),
            CASE WHEN random() > 0.1 THEN 'completed' ELSE 'pending' END
        );
        
        -- Update some orders
        IF i % 10 = 0 THEN
            UPDATE test_orders 
            SET order_total = order_total * 1.1 
            WHERE order_id = (SELECT order_id FROM test_orders ORDER BY order_id DESC LIMIT 1);
        END IF;
        
        -- Simple select
        PERFORM COUNT(*) FROM test_orders WHERE customer_id = (i % customer_count) + 1;
    END LOOP;
END
\$\$;
\timing off
EOSQL

collect_metrics "after_oltp"

# Scenario 2: OLAP-like workload
echo "=== SCENARIO 2: OLAP-like Workload ==="
collect_metrics "before_olap"

# Run analytical queries
psql -h postgres -U postgres -d tpc_tests -f /workloads/olap_scenario.sql > $METRICS_DIR/olap_workload.log 2>&1

collect_metrics "after_olap"

echo "All benchmarks completed!"
