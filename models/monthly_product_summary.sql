-- models/monthly_product_summary.sql

{{ config(materialized='incremental') }}

WITH batch_control AS (
    SELECT 
        etl_batch_no,
        etl_batch_date
    FROM etl_metadata.batch_control
),

update_existing AS (
    SELECT
        DATE_TRUNC('month', src.summary_date) AS start_of_the_month_date,
        src.dw_product_id,
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
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        batch_control.etl_batch_no AS etl_batch_no,
        batch_control.etl_batch_date AS etl_batch_date
    FROM {{ ref('daily_product_summary') }} src
    JOIN {{ this }} tar
        ON tar.dw_product_id = src.dw_product_id
        AND DATE_TRUNC('month', src.summary_date) = tar.start_of_the_month_date
    JOIN batch_control
        ON src.summary_date >= batch_control.etl_batch_date
),

new_records AS (
    SELECT
        DATE_TRUNC('month', src.summary_date) AS start_of_the_month_date,
        src.dw_product_id,
        MAX(src.customer_apd) AS customer_apd,
        CASE 
            WHEN MAX(src.customer_apd) = 1 THEN 1
            ELSE 0
        END AS customer_apm,
        SUM(src.product_cost_amount) AS product_cost_amount,
        SUM(src.product_mrp_amount) AS product_mrp_amount,
        SUM(src.cancelled_product_qty) AS cancelled_product_qty,
        SUM(src.cancelled_cost_amount) AS cancelled_cost_amount,
        SUM(src.cancelled_mrp_amount) AS cancelled_mrp_amount,
        MAX(src.cancelled_order_apd) AS cancelled_order_apd,
        CASE 
            WHEN MAX(src.cancelled_order_apd) = 1 THEN 1
            ELSE 0
        END AS cancelled_order_apm,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        batch_control.etl_batch_no AS etl_batch_no,
        batch_control.etl_batch_date AS etl_batch_date
    FROM {{ ref('daily_product_summary') }} src
    LEFT JOIN {{ this }} tar
        ON tar.dw_product_id = src.dw_product_id
        AND DATE_TRUNC('month', src.summary_date) = tar.start_of_the_month_date
    JOIN batch_control
        ON TRUE
    WHERE tar.dw_product_id IS NULL
        AND tar.start_of_the_month_date IS NULL
    GROUP BY 
        DATE_TRUNC('month', src.summary_date), 
        src.dw_product_id,
        batch_control.etl_batch_no,
        batch_control.etl_batch_date
)

-- Combine both update and insert results without UNION
SELECT
    start_of_the_month_date,
    dw_product_id,
    customer_apd,
    customer_apm,
    product_cost_amount,
    product_mrp_amount,
    cancelled_product_qty,
    cancelled_cost_amount,
    cancelled_mrp_amount,
    cancelled_order_apd,
    cancelled_order_apm,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
FROM update_existing

WHERE {{ is_incremental() }} -- Process only incremental data in updates

UNION ALL -- Ensure same column structure for both parts
SELECT
    start_of_the_month_date,
    dw_product_id,
    customer_apd,
    customer_apm,
    product_cost_amount,
    product_mrp_amount,
    cancelled_product_qty,
    cancelled_cost_amount,
    cancelled_mrp_amount,
    cancelled_order_apd,
    cancelled_order_apm,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
FROM new_records
