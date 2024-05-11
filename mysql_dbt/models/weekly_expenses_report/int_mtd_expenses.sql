SELECT de.datetime,
       de.category,
       de.category_id,
       de.account_number,
       SUM(de.daily_expense) OVER (
            PARTITION BY de.category_id, de.account_number, fdom.yearmonth
            ORDER BY de.datetime
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS month_to_date_expense
FROM {{ref('int_daily_expenses')}} de
         JOIN
     {{ref('int_first_day_of_month')}} fdom
     ON
         de.datetime = fdom.datetime