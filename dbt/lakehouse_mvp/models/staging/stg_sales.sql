with source as (
    select * from raw.sales
),

cleaned as (
    select
        order_id,
        customer_id,
        cast(order_date as date)                              as order_date,
        product,
        quantity,
        amount,
        region,
        date_part('year',    cast(order_date as date))::int  as order_year,
        date_part('month',   cast(order_date as date))::int  as order_month,
        date_part('quarter', cast(order_date as date))::int  as order_quarter,
        quantity * amount                                     as line_total
    from source
    where quantity > 0
      and amount   > 0
)

select * from cleaned
