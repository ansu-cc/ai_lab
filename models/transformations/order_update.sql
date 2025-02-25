WITH date_shift AS (
    SELECT CURRENT_DATE - MAX(orderTImestamp) AS shift_days
    FROM webshop.order
)
UPDATE webshop.order
SET orderTImestamp = orderTImestamp + (SELECT shift_days FROM date_shift);
