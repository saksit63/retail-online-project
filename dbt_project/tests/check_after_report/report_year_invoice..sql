SELECT 
    *
FROM
    {{ ref('report_year_invoice') }}
WHERE
    num_invoices < 0