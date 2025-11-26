#!/bin/tclsh

dbset db pg
diset connection pg_host db
diset connection pg_port 5432
diset connection pg_sslmode prefer

diset tpcc pg_count_ware 5
diset tpcc pg_num_vu 4
diset tpcc pg_superuser user
diset tpcc pg_superuserpass password
diset tpcc pg_defaultdbase mydb
diset tpcc pg_user user
diset tpcc pg_pass password

print dict

buildschema
waittocomplete

vuset logtotemp 1
vuset timestamps 1
vuset unique 1
vuset delay 100

vucreate
vurun

# 2 минуты теста
after 120000

vudestroy

puts "TPC-C Test Completed"
