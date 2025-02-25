{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT
    order_id,
    orderTimestamp + (CURRENT_DATE - MAX(orderTimestamp) OVER()) AS orderTimestamp
FROM webshop.order

{% if is_incremental() %}
WHERE orderTimestamp > (SELECT MAX(orderTimestamp) FROM {{ this }})
{% endif %}

