with source as (
    select *
    from {{ source('greenery', 'products') }}
),

renamed_recasted as (
    select
        product_id as product_guid,
        name as product_name,
        cast(price as numeric) as price_usd,
        cast(inventory as int64) as inventory_quantity
    from source
)

select * from renamed_recasted
