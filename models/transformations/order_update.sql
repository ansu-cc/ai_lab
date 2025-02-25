{{ config(
    materialized='incremental',
    unique_key='id',
    pre_hook="
        CREATE SCHEMA IF NOT EXISTS webshoporg;
        CREATE TABLE IF NOT EXISTS webshoporg.order AS SELECT * FROM webshop.order;
    "
) }}

WITH date_shift AS (
    SELECT CURRENT_DATE - MAX(orderTimestamp) AS shift_days
    FROM webshop.order
)

UPDATE webshop.order
SET orderTimestamp = orderTimestamp + (SELECT shift_days FROM date_shift);

SELECT
    id,
    customerid,
    ordertimestamp,
    shippingaddressid,
    total,
    shippingcost,
    created,
    updated
FROM webshop.order

{% if is_incremental() %}
WHERE ordertimestamp > (SELECT MAX(ordertimestamp) FROM {{ this }})
{% endif %}
