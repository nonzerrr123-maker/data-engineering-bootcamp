with
orders as (
    select * from {{ ref('stg_greenery__orders') }}
),

users as (
    select * from {{ ref('stg_greenery__users') }}
),

addresses as (
    select * from {{ ref('stg_greenery__addresses') }}
),

products as (
    select * from {{ ref('stg_greenery__products') }}
),

order_items as (
    select * from {{ ref('stg_greenery__order_items') }}
),

final as (
    select
        o.order_guid,
        o.order_created_at_utc,
        o.order_cost_usd,
        o.shipping_cost_usd,
        o.order_total_usd,
        o.shipping_service,
        o.estimated_delivery_at_utc,
        o.delivered_at_utc,
        o.order_status,
        o.user_guid,
        o.promo_guid,
        u.created_at_utc as user_created_at_utc,
        a.zipcode,
        a.state,
        a.country,
        oi.quantity,
        p.product_name,
        p.price,
        p.inventory
    from orders as o
    left join users as u on o.user_guid = u.user_guid
    left join addresses as a on o.address_guid = a.address_guid
    left join order_items as oi on oi.order_guid = o.order_guid
    left join products as p on oi.product_guid = p.product_guid
)

select * from final
