{{
    config(
        materialized = 'view'
    )
}}

with
orders as (
    select * from {{ ref('stg_greenery__orders') }}
),

order_items as (
    select * from {{ ref('stg_greenery__order_items') }}
),

products as (
    select * from {{ ref('stg_greenery__products') }}
),

final as (
    select
        o.order_guid,
        o.user_guid,
        p.product_guid,
        p.product_name,
        o.promo_guid,
        o.order_cost_usd,
        o.shipping_cost_usd,
        o.order_total_usd,
        o.shipping_service,
        oi.quantity,
        p.inventory,
        o.order_created_at_utc,
        o.estimated_delivery_at_utc,
        o.delivered_at_utc,
        o.order_status
    from orders as o
    left join order_items as oi
        on o.order_guid = oi.order_guid
    left join products as p
        on oi.product_guid = p.product_guid
)

select * from final
