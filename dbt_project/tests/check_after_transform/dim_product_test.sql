SELECT 
    *
FROM 
    {{ ref('dim_product') }}
WHERE 
    price < 0