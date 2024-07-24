SELECT  
    *
FROM 
    {{ ref('report_customer_invoice') }}
WHERE   
    total_invoices < 0