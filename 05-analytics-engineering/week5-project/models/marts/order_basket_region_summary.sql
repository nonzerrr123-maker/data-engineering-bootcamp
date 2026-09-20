select
    basket_size,
    us_region,
    count(distinct order_guid) as order_count,
    sum(total_quantity) as item_quantity
from {{ ref('int_orders_enriched') }}
group by 1, 2
order by 1, 2
