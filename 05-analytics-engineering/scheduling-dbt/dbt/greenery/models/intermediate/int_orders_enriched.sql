with orders as (
    select * from {{ ref('stg_greenery__orders') }}
),

addresses as (
    select * from {{ ref('stg_greenery__addresses') }}
),

order_quantities as (
    select
        order_guid,
        sum(quantity) as total_quantity
    from {{ ref('stg_greenery__order_items') }}
    group by 1
),

final as (
    select
        o.*,
        a.state,
        a.country,
        coalesce(q.total_quantity, 0) as total_quantity,
        case
            when coalesce(q.total_quantity, 0) <= 2 then 'S'
            when coalesce(q.total_quantity, 0) <= 4 then 'M'
            else 'L'
        end as basket_size,
        {{ us_region('a.state') }} as us_region
    from orders o
    left join addresses a using (address_guid)
    left join order_quantities q using (order_guid)
)

select * from final
