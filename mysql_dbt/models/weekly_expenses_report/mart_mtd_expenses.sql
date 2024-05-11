select datetime,
       category,
       category_id,
       account_number,
       month_to_date_expense
from {{ref('int_mtd_expenses')}}
