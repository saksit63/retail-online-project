-- เลือก 10 product ที่ขายได้จำนวนมากที่สุด
SELECT
  dim_product.product_id,
  dim_product.stock_code,
  dim_product.description,
  SUM(fct_invoices.quantity) AS total_quantity_sold
FROM {{ ref('fct_invoices') }} fct_invoices
JOIN {{ ref('dim_product') }} dim_product ON fct_invoices.product_id = p.product_id
GROUP BY dim_product.product_id, dim_product.stock_code, dim_product.description
ORDER BY total_quantity_sold DESC
LIMIT 10