-- This DBT model updates and inserts records into the `monthly_product_summary` table based on data from `daily_product_summary`.
-- Filename: monthly_product_summary.sql

{{ config(
    materialized='incremental',
    unique_key=['dw_product_id', "start_of_the_month_date"]
) }}

WITH batch_metadata AS (
    SELECT 
        etl_batch_date AS etl_batch_date,
        etl_batch_no AS etl_batch_no
    FROM metadata.batch_control  -- Assuming `batch_control` is a model in your dbt project
), base AS (
    SELECT 
        TO_DATE(summary_date, 'YYYY-MM-DD') AS summary_date,
        DATE_TRUNC('month', TO_DATE(summary_date, 'YYYY-MM-DD')) AS start_of_the_month_date,
        *
    FROM {{ ref('daily_product_summary') }}
    CROSS JOIN batch_metadata b
    WHERE TO_DATE(summary_date, 'YYYY-MM-DD') >= b.etl_batch_date::DATE
),

update_data AS (
    SELECT
        tar.start_of_the_month_date,
        tar.dw_product_id,
        GREATEST(tar.customer_apd, src.customer_apd) AS customer_apd,
        LEAST(tar.customer_apd + src.customer_apd, 1) AS customer_apm,
        tar.product_cost_amount + src.product_cost_amount AS product_cost_amount,
        tar.product_mrp_amount + src.product_mrp_amount AS product_mrp_amount,
        tar.cancelled_product_qty + src.cancelled_product_qty AS cancelled_product_qty,
        tar.cancelled_cost_amount + src.cancelled_cost_amount AS cancelled_cost_amount,
        tar.cancelled_mrp_amount + src.cancelled_mrp_amount AS cancelled_mrp_amount,
        GREATEST(tar.cancelled_order_apd, src.cancelled_order_apd) AS cancelled_order_apd,
        LEAST(tar.cancelled_order_apd + src.cancelled_order_apd, 1) AS cancelled_order_apm,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp
    FROM devdw.monthly_product_summary tar
    JOIN base src
    ON tar.dw_product_id = src.dw_product_id
    AND tar.start_of_the_month_date = src.start_of_the_month_date
),

insert_data AS (
    SELECT
        base.start_of_the_month_date,
        base.dw_product_id,
        MAX(base.customer_apd) AS customer_apd,
        CASE WHEN MAX(base.customer_apd) = 1 THEN 1 ELSE 0 END AS customer_apm,
        SUM(base.product_cost_amount) AS product_cost_amount,
        SUM(base.product_mrp_amount) AS product_mrp_amount,
        SUM(base.cancelled_product_qty) AS cancelled_product_qty,
        SUM(base.cancelled_cost_amount) AS cancelled_cost_amount,
        SUM(base.cancelled_mrp_amount) AS cancelled_mrp_amount,
        MAX(base.cancelled_order_apd) AS cancelled_order_apd,
        CASE WHEN MAX(base.cancelled_order_apd) = 1 THEN 1 ELSE 0 END AS cancelled_order_apm,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp
    FROM base
    LEFT JOIN devdw.monthly_product_summary tar
    ON tar.dw_product_id = base.dw_product_id
    AND tar.start_of_the_month_date = base.start_of_the_month_date
    WHERE tar.dw_product_id IS NULL
    GROUP BY base.start_of_the_month_date, base.dw_product_id
)

-- Final merge
SELECT u.*, b.etl_batch_date :: DATE, b.etl_batch_no
FROM update_data u
CROSS JOIN batch_metadata b

UNION ALL

SELECT i.*, b.etl_batch_date :: DATE, b.etl_batch_no
FROM insert_data i
CROSS JOIN batch_metadata b
