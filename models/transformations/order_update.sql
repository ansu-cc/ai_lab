{{ config(
    materialized='incremental',
    unique_key='id'
) }}

-- Step 1: Backup today's orders into WebShopORG if not already backed up
CREATE TABLE IF NOT EXISTS WebShopORG.order AS
SELECT * FROM webshop.order;

-- Step 2: Drop and Restore WebShop.order from WebShopORG
DROP TABLE IF EXISTS webshop.order;
CREATE TABLE webshop.order AS
SELECT * FROM WebShopORG.order;

-- Step 3: Compute the date shift
WITH date_shift AS (
    SELECT CURRENT_DATE - MAX(orderTimestamp) AS shift_days
    FROM webshop.order
)

-- Step 4: Apply the date shift to all orders
UPDATE webshop.order
SET orderTimestamp = orderTimestamp + (SELECT shift_days FROM date_shift);

-- Step 5: Ensure only new records are added incrementally
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
