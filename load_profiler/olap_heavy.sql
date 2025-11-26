\set aid random(1, 100000)
\set bid random(1, 10000)

SELECT
    b.bid,
    count(a.aid) AS account_count,
    avg(a.abalance) AS avg_balance
FROM
    pgbench_accounts a
JOIN
    pgbench_branches b ON a.bid = b.bid
WHERE
    a.abalance > 0
GROUP BY
    b.bid
HAVING
    count(a.aid) > 100
ORDER BY
    avg_balance DESC
LIMIT 10;
