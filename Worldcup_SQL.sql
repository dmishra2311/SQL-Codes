WITH host AS (
    SELECT 
        th.team_id,
        th.team_name,
        CASE 
            WHEN m.host_goals > m.guest_goals THEN 3
            WHEN m.host_goals < m.guest_goals THEN 0
            WHEN m.host_goals = m.guest_goals THEN 1
            ELSE 0
        END AS num_points
    FROM 
        teams th 
    LEFT JOIN 
        matches m ON m.host_team = th.team_id
    
    UNION ALL
    
    SELECT 
        th.team_id,
        th.team_name,
        CASE 
            WHEN m.host_goals < m.guest_goals THEN 3
            WHEN m.host_goals > m.guest_goals THEN 0
            WHEN m.host_goals = m.guest_goals THEN 1
            ELSE 0
        END AS num_points
    FROM 
        teams th 
    LEFT JOIN 
        matches m ON m.guest_team = th.team_id
)

SELECT 
    team_id,
    team_name, 
    SUM(num_points) AS num_points
FROM 
    host h
GROUP BY 
    team_id,
    team_name
ORDER BY 
    SUM(num_points) DESC,
    team_id ASC;
