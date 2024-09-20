WITH numbers AS (
    SELECT 2 AS num
    UNION ALL
    SELECT num + 1
    FROM numbers
    WHERE num + 1 <= 1000
),
prime_numbers AS (
    SELECT num
    FROM numbers n
    WHERE 
    NOT EXISTS (
        SELECT 1
        FROM numbers m
        WHERE m.num < n.num
        AND n.num % m.num = 0)
     )
SELECT STRING_AGG(num, '&') WITHIN GROUP (ORDER BY num) AS primes
FROM prime_numbers
OPTION (MAXRECURSION 1001);