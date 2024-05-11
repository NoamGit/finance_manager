SELECT datetime,
       date_format(datetime, '%Y-%m') AS yearmonth
FROM {{ref('stg_expense_transaction')}}
group by datetime