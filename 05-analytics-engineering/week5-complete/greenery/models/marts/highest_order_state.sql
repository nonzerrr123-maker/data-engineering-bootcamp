select *
from {{ ref('orders_by_state') }}
qualify row_number() over (order by order_count desc, state) = 1
