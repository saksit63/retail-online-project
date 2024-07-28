-- เลือก 10 ประเทศที่ทำไรได้มากที่สุด
SELECT
  dim_customer.country,
  dim_customer.iso,
  COUNT(fct_invoices.invoice_id) AS total_invoices,
  SUM(fct_invoices.total) AS total_revenue
FROM {{ ref('fct_invoices') }} fct_invoices
JOIN {{ ref('dim_customer') }} dim_customer ON fct_invoices.customer_id = dim_customer.customer_id
GROUP BY dim_customer.country, dim_customer.iso
ORDER BY total_revenue DESC
LIMIT 10
