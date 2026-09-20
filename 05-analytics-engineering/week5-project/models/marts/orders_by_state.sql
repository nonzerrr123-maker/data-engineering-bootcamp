select
    state,
    count(distinct order_guid) as order_count
from {{ ref('int_orders_enriched') }}
where state is not null
group by 1
order by order_count desc, state
