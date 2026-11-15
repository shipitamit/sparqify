{{ config(materialized='table') }}
-- orders mart
select o.id, c.name, sum(o.amount) as revenue
from {{ ref('stg_orders') }} o
join {{ source('crm', 'customers') }} c on c.id = o.customer_id
{% if is_incremental() %}
where o.updated_at > (select max(updated_at) from {{ this }})
{% endif %}
group by o.id, c.name
