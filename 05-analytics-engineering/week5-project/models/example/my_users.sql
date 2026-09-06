select *
from {{ source('greenery', 'users') }}
where regexp_contains(
    email,
    r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
)
