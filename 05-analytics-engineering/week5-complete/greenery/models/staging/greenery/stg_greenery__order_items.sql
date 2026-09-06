with source as (
    select *
    from {{ source('greenery', 'order_items') }}
),

renamed_recasted as (
    select
        order_id as order_guid,
        product_id as product_guid,
        cast(quantity as int64) as quantity
    from source
)

select * from renamed_recasted
