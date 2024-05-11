SELECT datetime,
       category_id,
       account_number,
       max(category) as category,
       SUM(charged) AS daily_expense
FROM {{ref('stg_expense_transaction')}}
GROUP BY
    account_number , datetime, category_id