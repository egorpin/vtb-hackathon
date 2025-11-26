\set aid random(1, 100000)
\set bid random(1, 100000)
SELECT
    count(*),
    avg(a.abalance)
FROM
    pgbench_accounts a,
    pgbench_branches b
WHERE
    a.bid = b.bid
    AND a.aid BETWEEN :aid AND :aid + 10000
    AND b.bid = :bid;
