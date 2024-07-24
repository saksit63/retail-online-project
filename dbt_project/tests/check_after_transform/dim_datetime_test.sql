SELECT 
    *
FROM 
    {{ ref('dim_datetime') }}
WHERE
    weekday NOT BETWEEN 0 AND 6
