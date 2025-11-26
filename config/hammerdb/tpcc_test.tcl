#!/bin/tclsh

# Database configuration
dbset db pg
diset connection pg_host postgres
diset connection pg_port 5432
diset connection pg_username postgres
diset connection pg_password password
diset connection pg_ssl false

# TPC-C Configuration
diset tpcc pg_count_ware 20
diset tpcc pg_num_vu 16
diset tpcc pg_rampup 2
diset tpcc pg_duration 10
diset tpcc pg_allwarehouse true
diset tpcc pg_timeprofile true
diset tpcc pg_async_scale false
diset tpcc pg_vacuum true

# Build Schema
print "Building TPC-C schema..."
schema
load

# Run benchmark
print "Starting TPC-C benchmark..."
loadscript
vuset created
vuset run
vuwait

# Save results
print [vucomplete]
print "Saving results to /results/hammerdb_output.log"
