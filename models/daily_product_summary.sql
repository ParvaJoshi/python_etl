{{ config(
    materialized='incremental'
)}}
WITH batch_metadata AS (
    SELECT
        etl_batch_no,
        etl_batch_date
    FROM etl_metadata.batch_control
   
),
x as(
        -- Orders data aggregation
        SELECT CAST(TO_CHAR(O.orderDate, 'YYYY-MM-DD') AS DATE) AS summary_date,
            p.dw_product_id,
            1 AS customer_apd,
            SUM(OD.quantityOrdered * p.buyPrice) AS product_cost_amount,
            SUM(OD.quantityOrdered * p.MSRP) AS product_mrp_amount,
            0 AS cancelled_product_qty,
            0 AS cancelled_cost_amount,
            0 AS cancelled_mrp_amount,
            0 AS cancelled_order_apd
        FROM {{ref('orders')}} O
        JOIN {{ref('orderdetails')}} OD ON O.src_orderNumber = OD.src_orderNumber
        JOIN {{ref('products')}} p ON p.src_productCode = OD.src_productCode
        cross join batch_metadata b
        WHERE CAST(O.orderDate AS DATE) >= b.etl_batch_date
        GROUP BY summary_date, p.dw_product_id

        UNION ALL

        -- Cancellations data aggregation
        SELECT CAST(TO_CHAR(O.cancelledDate, 'YYYY-MM-DD') AS DATE) AS summary_date,
            p.dw_product_id,
            0 AS customer_apd,
            0 AS product_cost_amount,
            0 AS product_mrp_amount,
            SUM(OD.quantityOrdered) AS cancelled_product_qty,
            SUM(OD.quantityOrdered * p.buyPrice) AS cancelled_cost_amount,
            SUM(OD.quantityOrdered * p.MSRP) AS cancelled_mrp_amount,
            1 AS cancelled_order_apd
        FROM {{ref('orders')}} O
        JOIN {{ref('orderdetails')}} OD ON O.src_orderNumber = OD.src_orderNumber
        JOIN {{ref('products')}} p ON p.src_productCode = OD.src_productCode
        cross join batch_metadata b
        WHERE O.status = 'Cancelled'
        AND CAST(O.cancelledDate AS DATE) >= b.etl_batch_date
        GROUP BY summary_date, p.dw_product_id
    ) 
    SELECT summary_date,
        dw_product_id,
        SUM(customer_apd) AS customer_apd,
        SUM(product_cost_amount) AS product_cost_amount,
        SUM(product_mrp_amount) AS product_mrp_amount,
        SUM(cancelled_product_qty) AS cancelled_product_qty,
        SUM(cancelled_cost_amount) AS cancelled_cost_amount,
        SUM(cancelled_mrp_amount) AS cancelled_mrp_amount,
        SUM(cancelled_order_apd) AS cancelled_order_apd,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        max(b.etl_batch_no) AS etl_batch_no,
        max(b.etl_batch_date) AS etl_batch_date
    FROM x
    cross join batch_metadata b
    GROUP BY summary_date, dw_product_id