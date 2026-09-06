{% macro us_region(state_col) %}
case
    when {{ state_col }} in (
        'Connecticut','Maine','Massachusetts','New Hampshire','Rhode Island','Vermont',
        'New Jersey','New York','Pennsylvania'
    ) then 'Northeast'
    when {{ state_col }} in (
        'Indiana','Illinois','Michigan','Ohio','Wisconsin',
        'Iowa','Kansas','Minnesota','Missouri','Nebraska','North Dakota','South Dakota'
    ) then 'Midwest'
    when {{ state_col }} in (
        'Delaware','Florida','Georgia','Maryland','North Carolina','South Carolina',
        'Virginia','District of Columbia','West Virginia',
        'Alabama','Kentucky','Mississippi','Tennessee',
        'Arkansas','Louisiana','Oklahoma','Texas'
    ) then 'South'
    when {{ state_col }} in (
        'Arizona','Colorado','Idaho','Montana','Nevada','New Mexico','Utah','Wyoming',
        'Alaska','California','Hawaii','Oregon','Washington'
    ) then 'West'
    else 'Other'
end
{% endmacro %}
