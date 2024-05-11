select date_format(date, '%Y-%m-%d') as datetime,
       category,
       category_id,
       charged,
       account_number,
       transaction_id
from {{ source('mysql_db', 'clean_transactions') }}
where transaction_type = 'expense'
    and category_id not in (41, 47)  -- investment or ignore
   or category_id is null