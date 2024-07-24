SELECT
    *
FROM
    {{ ref('fct_invoices')}}
WHERE
    total < 0