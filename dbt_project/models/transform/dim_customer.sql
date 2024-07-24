-- dim_customer.sql

-- Create the dimension table
WITH customer_cte AS (
	SELECT DISTINCT
	    {{ dbt_utils.generate_surrogate_key(['CustomerID', 'Country']) }} as customer_id,
        -- สร้าง key ที่ไม่ซ้ำกับจากคอลัม CustomerID และ Country
	    Country AS country
	FROM {{ source('retail_project', 'raw_invoices') }}
	WHERE CustomerID IS NOT NULL
)
SELECT
    t.*,
	cm.iso
FROM customer_cte t
LEFT JOIN {{ source('retail_project', 'country') }} cm ON t.country = cm.nicename