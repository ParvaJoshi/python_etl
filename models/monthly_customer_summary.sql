{{ config(
    materialized='incremental',
    unique_key=['dw_customer_id', 'start_of_the_month_date']  
) }}

-- CTE for fetching the current batch metadata
WITH batch_control AS (
    SELECT 
        etl_batch_no, 
        etl_batch_date
    FROM etl_metadata.batch_control
    LIMIT 1
),
source_data AS (
    SELECT 
        DATE_TRUNC('month', summary_date) AS start_of_the_month_date,
        dw_customer_id,
        SUM(order_count) AS order_count,
        MAX(order_apd) AS order_apd,
        CASE WHEN MAX(order_apd) = 1 THEN 1 ELSE 0 END AS order_apm,
        sum(order_amount) as order_amount,
        SUM(order_cost_amount) AS order_cost_amount,
        SUM(cancelled_order_count) AS cancelled_order_count,
        SUM(cancelled_order_amount) AS cancelled_order_amount,
        MAX(cancelled_order_apd) AS cancelled_order_apd,
        CASE WHEN MAX(cancelled_order_apd) = 1 THEN 1 ELSE 0 END AS cancelled_order_apm,
        SUM(shipped_order_count) AS shipped_order_count,
        SUM(shipped_order_amount) AS shipped_order_amount,
        MAX(shipped_order_apd) AS shipped_order_apd,
        CASE WHEN MAX(shipped_order_apd) = 1 THEN 1 ELSE 0 END AS shipped_order_apm,
        MAX(payment_apd) AS payment_apd,
        CASE WHEN MAX(payment_apd) = 1 THEN 1 ELSE 0 END AS payment_apm,
        SUM(payment_amount) AS payment_amount,
        SUM(products_ordered_qty) AS products_ordered_qty,
        SUM(products_items_qty) AS products_items_qty,
        SUM(order_mrp_amount) AS order_mrp_amount,
        MAX(new_customer_apd) AS new_customer_apd,
        CASE WHEN MAX(new_customer_apd) = 1 THEN 1 ELSE 0 END AS new_customer_apm,
        0 AS new_customer_paid_apd,
        0 AS new_customer_paid_apm,
        '{{ run_started_at }}'::timestamp AS dw_create_timestamp,
        '{{ run_started_at }}'::timestamp AS dw_update_timestamp,
        max(bc.etl_batch_no) as etl_batch_no,
        max(bc.etl_batch_date) as etl_batch_date
    FROM {{ ref('daily_customer_summary') }} AS src
    CROSS JOIN batch_control AS bc
    WHERE src.summary_date >= bc.etl_batch_date
    GROUP BY 1, 2
)

-- Main incremental query
SELECT
    sd.start_of_the_month_date,
    sd.dw_customer_id,
    sd.order_count,
    GREATEST(tar.order_apd, sd.order_apd) AS order_apd,
    LEAST(COALESCE(tar.order_apm, 0) + sd.order_apd, 1) AS order_apm,
    COALESCE(tar.order_amount, 0) + sd.order_amount AS order_amount,
    COALESCE(tar.order_cost_amount, 0) + sd.order_cost_amount AS order_cost_amount,
    COALESCE(tar.cancelled_order_count, 0) + sd.cancelled_order_count AS cancelled_order_count,
    COALESCE(tar.cancelled_order_amount, 0) + sd.cancelled_order_amount AS cancelled_order_amount,
    GREATEST(tar.cancelled_order_apd, sd.cancelled_order_apd) AS cancelled_order_apd,
    LEAST(COALESCE(tar.cancelled_order_apm, 0) + sd.cancelled_order_apd, 1) AS cancelled_order_apm,
    COALESCE(tar.shipped_order_count, 0) + sd.shipped_order_count AS shipped_order_count,
    COALESCE(tar.shipped_order_amount, 0) + sd.shipped_order_amount AS shipped_order_amount,
    GREATEST(tar.shipped_order_apd, sd.shipped_order_apd) AS shipped_order_apd,
    LEAST(COALESCE(tar.shipped_order_apm, 0) + sd.shipped_order_apd, 1) AS shipped_order_apm,
    GREATEST(tar.payment_apd, sd.payment_apd) AS payment_apd,
    LEAST(COALESCE(tar.payment_apm, 0) + sd.payment_apd, 1) AS payment_apm,
    COALESCE(tar.payment_amount, 0) + sd.payment_amount AS payment_amount,
    COALESCE(tar.products_ordered_qty, 0) + sd.products_ordered_qty AS products_ordered_qty,
    COALESCE(tar.products_items_qty, 0) + sd.products_items_qty AS products_items_qty,
    COALESCE(tar.order_mrp_amount, 0) + sd.order_mrp_amount AS order_mrp_amount,
    sd.new_customer_apd,
    sd.new_customer_apm,
    sd.new_customer_paid_apd,
    sd.new_customer_paid_apm,
    sd.dw_create_timestamp,
    '{{ run_started_at }}'::timestamp AS dw_update_timestamp,
    sd.etl_batch_no,
    sd.etl_batch_date

FROM source_data AS sd
LEFT JOIN {{ this }} AS tar
    ON tar.dw_customer_id = sd.dw_customer_id
    AND tar.start_of_the_month_date = sd.start_of_the_month_date

