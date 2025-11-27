#!/bin/bash

set -e

echo "Initializing PostgreSQL database for TPC testing..."

# Wait for PostgreSQL to be ready
for i in {1..5}; do
  # Проверяем доступность сервера через подключение к стандартной базе "postgres"
  if psql -h postgres -U postgres -d postgres -c '\q' 2>/dev/null; then
    echo "PostgreSQL is ready!"
    break
  fi
  echo "PostgreSQL is unavailable - sleeping (attempt $i/30)"
  sleep 2
done

# Create extensions and setup
psql -h postgres -U postgres -d tpc_tests <<EOSQL
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
SELECT pg_stat_statements_reset();

-- Create test tables
CREATE TABLE IF NOT EXISTS test_orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    order_total DECIMAL(10,2),
    order_date TIMESTAMP DEFAULT NOW(),
    status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS test_customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_date TIMESTAMP DEFAULT NOW()
);

-- Generate some test data
INSERT INTO test_customers (name, email)
SELECT
    'Customer_' || i,
    'customer_' || i || '@test.com'
FROM generate_series(1, 100) i;
EOSQL

echo "Database initialized successfully!"
