-- แสดงจำนวนการขายและรายได้ในแต่ละปี
SELECT
  dim_datetime.year,
  dim_datetime.month,
  COUNT(DISTINCT fct_invoices.invoice_id) AS num_invoices,
  SUM(fct_invoices.total) AS total_revenue
FROM {{ ref('fct_invoices') }} fct_invoices
JOIN {{ ref('dim_datetime') }} dim_datetime ON fct_invoices.datetime_id = dim_datetime.datetime_id
GROUP BY dim_datetime.year, dim_datetime.month
ORDER BY dim_datetime.year, dim_datetime.month