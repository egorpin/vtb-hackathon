\set aid random(1, 100000)
\set bid random(1, 10000)
\set tid random(1, 1000)

SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
SELECT tbalance FROM pgbench_tellers WHERE tid = :tid;
SELECT sum(tbalance) FROM pgbench_tellers;

UPDATE pgbench_tellers SET tbalance = tbalance + 1 WHERE tid = :tid;

\sleep 1
