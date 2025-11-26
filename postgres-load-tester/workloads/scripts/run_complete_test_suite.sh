#!/bin/bash

set -e

echo "🎯 STARTING COMPLETE POSTGRESQL LOAD TESTING SUITE"

# Create results directories
mkdir -p /results/{metrics,reports,classifications}

# Initialize database
echo "🔄 Initializing database..."
/scripts/init_db.sh

# Run benchmarks
echo "🧪 Running benchmarks..."
/scripts/run_benchmarks.sh

# Run workload classification
echo "📊 Running workload classification..."
python3 /scripts/workload_classifier.py

echo "✅ ALL TESTS COMPLETED SUCCESSFULLY"
echo "📁 Results available in /results/"
