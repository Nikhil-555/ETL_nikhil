{{ config(
    materialized='incremental',
    unique_key=["summary_date","dw_product_id"]
) }}

WITH batch_data AS (
    -- Extract the latest batch metadata
    SELECT
        etl_batch_date AS etl_batch_date,
        etl_batch_no AS etl_batch_no
    FROM metadata.batch_control
),

order_summary AS (
    SELECT
        TO_CHAR(O.orderDate, 'YYYY-MM-DD')::DATE AS summary_date,
        P.dw_product_id,
        1 AS customer_apd,
        SUM(OD.quantityOrdered * P.buyPrice) AS product_cost_amount,
        SUM(OD.quantityOrdered * OD.priceEach) AS product_amount,
        SUM(OD.quantityOrdered * P.MSRP) AS product_mrp_amount,
        0 AS cancelled_product_qty,
        0 AS cancelled_amount,
        0 AS cancelled_cost_amount,
        0 AS cancelled_mrp_amount,
        0 AS cancelled_order_apd
    FROM {{ ref('orders') }} O
    JOIN {{ ref('orderdetails') }} OD ON O.src_orderNumber = OD.src_orderNumber
    JOIN {{ ref('products') }} P ON P.src_productCode = OD.src_productCode
    CROSS JOIN batch_data
    WHERE TO_CHAR(O.orderDate, 'YYYY-MM-DD') >= batch_data.etl_batch_date
    GROUP BY summary_date, P.dw_product_id
),

cancelled_order_summary AS (
    SELECT
        TO_CHAR(O.cancelledDate, 'YYYY-MM-DD')::DATE AS summary_date,
        P.dw_product_id,
        0 AS customer_apd,
        0 AS product_cost_amount,
        0 AS product_amount,
        0 AS product_mrp_amount,
        COUNT(DISTINCT O.src_orderNumber) AS cancelled_product_qty,
        SUM(OD.quantityOrdered * OD.priceEach) AS cancelled_amount,
        SUM(OD.quantityOrdered * P.buyPrice) AS cancelled_cost_amount,
        SUM(OD.quantityOrdered * P.MSRP) AS cancelled_mrp_amount,
        1 AS cancelled_order_apd
    FROM {{ ref('orders') }} O
    JOIN {{ ref('orderdetails') }} OD ON O.src_orderNumber = OD.src_orderNumber
    JOIN {{ ref('products') }} P ON P.src_productCode = OD.src_productCode
    CROSS JOIN batch_data
    WHERE O.status = 'Cancelled'
      AND TO_CHAR(O.cancelledDate, 'YYYY-MM-DD') >= batch_data.etl_batch_date
    GROUP BY summary_date, P.dw_product_id
),

latest_data AS (
    SELECT
        summary_date,
        dw_product_id,
        SUM(customer_apd) AS customer_apd,
        SUM(product_cost_amount) AS product_cost_amount,
        SUM(product_amount) AS product_amount,
        SUM(product_mrp_amount) AS product_mrp_amount,
        SUM(cancelled_product_qty) AS cancelled_product_qty,
        SUM(cancelled_amount) AS cancelled_amount,
        SUM(cancelled_cost_amount) AS cancelled_cost_amount,
        SUM(cancelled_mrp_amount) AS cancelled_mrp_amount,
        SUM(cancelled_order_apd) AS cancelled_order_apd,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp
    FROM (
        SELECT * FROM order_summary
        UNION ALL
        SELECT * FROM cancelled_order_summary
    ) aggregated
    GROUP BY summary_date, dw_product_id
)

SELECT
    l.*, 
    b.etl_batch_date, 
    b.etl_batch_no
FROM latest_data l
CROSS JOIN batch_data b