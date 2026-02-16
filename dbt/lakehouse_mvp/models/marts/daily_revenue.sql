with stg as (
    select * from {{ ref('stg_sales') }}
)

select
    order_date,
    region,
    product,
    count(distinct order_id)    as order_count,
    count(distinct customer_id) as customer_count,
    sum(quantity)               as total_units,
    round(sum(line_total), 2)   as revenue,
    round(avg(amount), 2)       as avg_order_value
from stg
group by order_date, region, product
order by order_date, region, product
