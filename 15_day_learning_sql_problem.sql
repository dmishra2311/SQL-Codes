/*Julia conducted a 15 days of learning SQL contest. The start date of the contest was March 01, 2016 and the end date was March 15, 2016.

Write a query to print total number of unique hackers who made at least 1 submission each day (starting on the first day of the contest), 
and find the hacker_id and name of the hacker who made maximum number of submissions each day. 
If more than one such hacker has a maximum number of submissions, print the lowest hacker_id. 
The query should print this information for each day of the contest, sorted by the date.*/

WITH a AS (
    SELECT
        submission_date,
        hacker_id,
        ROW_NUMBER() OVER (PARTITION BY hacker_id ORDER BY submission_date) AS running_total
    FROM
        (SELECT DISTINCT submission_date, hacker_id FROM submissions) s
),
b AS (
    SELECT 
        submission_date,
        COUNT(1) AS cnt_unique
    FROM 
        a
    WHERE 
        RIGHT(CONVERT(VARCHAR, submission_date, 23), 2) = running_total
    GROUP BY 
        submission_date
),
c AS (
    SELECT
        submission_date,
        hacker_id,
        COUNT(1) AS num
    FROM
        submissions
    GROUP BY
        submission_date,
        hacker_id
),
d AS (
    SELECT
        submission_date,
        ranked.hacker_id,
        name.name
    FROM (
        SELECT
            submission_date,
            hacker_id,
            ROW_NUMBER() OVER (PARTITION BY submission_date ORDER BY num DESC, hacker_id ASC) AS rn
        FROM
            c
    ) AS ranked
    LEFT JOIN hackers name 
    ON ranked.hacker_id = name.hacker_id
    WHERE
        ranked.rn = 1
)
SELECT 
    b.submission_date, 
    b.cnt_unique, 
    d.hacker_id,
    d.name
FROM 
    b 
JOIN 
    d ON b.submission_date = d.submission_date
ORDER BY 
    b.submission_date;
