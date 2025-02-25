{{ config(
    materialized='incremental',
    unique_key='id',
    pre_hook="
        CREATE TABLE IF NOT EXISTS WebShopORG.order AS SELECT * FROM webshop.order;
        DROP TABLE IF EXISTS webshop.order;
        CREATE TABLE webshop.order AS SELECT * FROM WebShopORG.order;
    ",
    post_hook="
        UPDATE webshop.order
        SET orderTimestamp = orderTimestamp + (SELECT CURRENT_DATE - MAX(orderTimestamp) FROM webshop.order);
    "
) }}

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

