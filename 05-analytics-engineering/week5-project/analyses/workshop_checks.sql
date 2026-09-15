-- Reference queries for manual verification after dbt run/build.
select 'users' as metric, user_count as value from {{ ref('number_of_users') }}
union all
select 'orders' as metric, order_count as value from {{ ref('number_of_orders') }}
