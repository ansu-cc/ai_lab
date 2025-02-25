{{ config(
    materialized='incremental',
    unique_key='id'
) }}

WITH latest_date AS (
    -- Get the latest ordertimestamp from the existing table
    SELECT MAX(ordertimestamp) AS last_order_date FROM {{ this }}
)

SELECT
    id,
    customerid,
    ordertimestamp + (SELECT CURRENT_DATE - last_order_date FROM latest_date) AS ordertimestamp,
    shippingaddressid,
    total,
    shippingcost,
    created,
    updated
FROM webshop.order

{% if is_incremental() %}
WHERE ordertimestamp > (SELECT MAX(ordertimestamp) FROM {{ this }})
{% endif %}
