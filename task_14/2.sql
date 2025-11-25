SELECT
    s.acctbal,
    s.name       AS supplier_name,
    n.name       AS nation_name,
    p.partkey,
    p.mfgr,
    s.address,
    s.phone,
    s.comment
FROM tpch.sf1.part     AS p
JOIN tpch.sf1.partsupp AS ps ON p.partkey = ps.partkey
JOIN tpch.sf1.supplier AS s  ON s.suppkey = ps.suppkey
JOIN tpch.sf1.nation   AS n  ON s.nationkey = n.nationkey
JOIN tpch.sf1.region   AS r  ON n.regionkey = r.regionkey
WHERE
    p.size = 15
    AND p.type LIKE '%BRASS'
    AND r.name = 'EUROPE'
    AND ps.supplycost = (
        SELECT MIN(ps2.supplycost)
        FROM tpch.sf1.partsupp AS ps2
        JOIN tpch.sf1.supplier AS s2 ON s2.suppkey = ps2.suppkey
        JOIN tpch.sf1.nation   AS n2 ON s2.nationkey = n2.nationkey
        JOIN tpch.sf1.region   AS r2 ON n2.regionkey = r2.regionkey
        WHERE
            ps2.partkey = p.partkey
            AND r2.name = 'EUROPE'
    )
ORDER BY
    s.acctbal DESC,
    n.name,
    s.name,
    p.partkey;
