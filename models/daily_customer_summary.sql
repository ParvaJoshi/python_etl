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
      SELECT CAST(src_create_timestamp AS DATE) AS summary_date,
               c.dw_customer_id,
               0 AS order_count,
               0 AS order_apd,
               0 as order_amount,
               0 AS order_cost_amount,
               0 AS cancelled_order_count,
               0 AS cancelled_order_amount,
               0 AS cancelled_order_apd,
               0 AS shipped_order_count,
               0 AS shipped_order_amount,
               0 AS shipped_order_apd,
               0 AS payment_apd,
               0 AS payment_amount,
               0 AS products_ordered_qty,
               0 AS products_items_qty,
               0 AS order_mrp_amount,
               1 AS new_customer_apd,
               0 AS new_customer_paid_apd
        FROM {{ref('customers')}} c
        cross join batch_metadata b
        WHERE CAST(src_create_timestamp AS DATE) >= b.etl_batch_date
        UNION ALL
        SELECT CAST(o.cancelledDate AS DATE) AS summary_date,
               o.dw_customer_id,
               0 AS order_count,
               0 AS order_apd,
               0 as order_amount,
               0 AS order_cost_amount,
               COUNT(DISTINCT o.dw_order_id) AS cancelled_order_count,
               SUM(od.priceeach*od.quantityordered) AS cancelled_order_amount,
               1 AS cancelled_order_apd,
               0 AS shipped_order_count,
               0 AS shipped_order_amount,
               0 AS shipped_order_apd,
               0 AS payment_apd,
               0 AS payment_amount,
               0 AS products_ordered_qty,
               0 AS products_items_qty,
               0 AS order_mrp_amount,
               0 AS new_customer_apd,
               0 AS new_customer_paid_apd
        FROM {{ref('orders')}} o
        JOIN {{ref('orderdetails')}} od ON o.dw_order_id = od.dw_order_id
        cross join batch_metadata b
        WHERE CAST(o.cancelledDate AS DATE) >= b.etl_batch_date
        AND   o.status = 'Cancelled'
        GROUP BY o.dw_customer_id,
                 CAST(o.cancelledDate AS DATE)
        UNION ALL
        SELECT CAST(p.paymentDate AS DATE) AS summary_date,
               p.dw_customer_id,
               0 AS order_count,
               0 AS order_apd,
               0 as order_amount,
               0 AS order_cost_amount,
               0 AS cancelled_order_count,
               0 AS cancelled_order_amount,
               0 AS cancelled_order_apd,
               0 AS shipped_order_count,
               0 AS shipped_order_amount,
               0 AS shipped_order_apd,
               1 AS payment_apd,
               SUM(p.amount) AS payment_amount,
               0 AS products_ordered_qty,
               0 AS products_items_qty,
               0 AS order_mrp_amount,
               0 AS new_customer_apd,
               CASE
                 WHEN rank1 = 1 THEN 1
                 ELSE 0
               END AS new_customer_paid_apd
        FROM (SELECT paymentdate,
                     dw_customer_id,
                     amount,
                     RANK() OVER (PARTITION BY dw_customer_id ORDER BY paymentdate) AS rank1
              FROM {{ref('payments')}}) p
              cross join batch_metadata b
        WHERE CAST(p.paymentDate AS DATE) >= b.etl_batch_date
        GROUP BY CAST(p.paymentDate AS DATE),
                 p.dw_customer_id,
                 p.rank1
        UNION ALL
        SELECT CAST(o.ShippedDate AS DATE) AS summary_date,
               o.dw_customer_id,
               0 AS order_count,
               0 AS order_apd,
               0 as order_amount,
               0 AS order_cost_amount,
               0 AS cancelled_order_count,
               0 AS cancelled_order_amount,
               0 AS cancelled_order_apd,
               COUNT(DISTINCT o.dw_order_id) AS shipped_order_count,
               SUM(od.priceEach*od.quantityOrdered) AS shipped_order_amount,
               1 AS shipped_order_apd,
               0 AS payment_apd,
               0 AS payment_amount,
               0 AS products_ordered_qty,
               0 AS products_items_qty,
               0 AS order_mrp_amount,
               0 AS new_customer_apd,
               0 AS new_customer_paid_apd
        FROM {{ref('orders')}} o
        JOIN {{ref('orderdetails')}} od ON o.dw_order_id = od.dw_order_id
        cross join batch_metadata b
        WHERE CAST(o.ShippedDate AS DATE) >= b.etl_batch_date
        AND   o.status = 'Shipped'
        GROUP BY CAST(o.ShippedDate AS DATE),
                 o.dw_customer_id
        UNION ALL
        SELECT CAST(o.OrderDate AS DATE) AS summary_date,
               o.dw_customer_id,
               COUNT(DISTINCT o.dw_order_id) AS order_count,
               1 AS order_apd,
               SUM(od.priceEach*od.quantityOrdered) AS order_amount,
               SUM(p.buyPrice*od.quantityOrdered) AS order_cost_amount,
               0 AS cancelled_order_count,
               0 AS cancelled_order_amount,
               0 AS cancelled_order_apd,
               0 AS shipped_order_count,
               0 AS shipped_order_amount,
               0 AS shipped_order_apd,
               0 AS payment_apd,
               0 AS payment_amount,
               COUNT(DISTINCT od.src_productCode) AS products_ordered_qty,
               COUNT(od.quantityOrdered) AS products_items_qty,
               SUM(od.quantityOrdered*p.msrp) AS order_mrp_amount,
               0 AS new_customer_apd,
               0 AS new_customer_paid_apd
        FROM {{ref('orders')}} o
        JOIN {{ref('orderdetails')}} od ON o.dw_order_id = od.dw_order_id
        JOIN {{ref('products')}} p ON od.src_productCode = p.src_productCode
        cross join batch_metadata b
        WHERE CAST(o.OrderDate AS DATE) >= b.etl_batch_date
        GROUP BY CAST(o.OrderDate AS DATE),
                 o.dw_customer_id)
SELECT 
        summary_date,
        dw_customer_id,
        MAX(order_count) AS order_count,
        MAX(order_apd) AS order_apd,
        max(order_amount) as order_amount,
        MAX(order_cost_amount) AS order_cost_amount,
        MAX(cancelled_order_count) AS cancelled_order_count,
        MAX(cancelled_order_amount) AS cancelled_order_amount,
        MAX(cancelled_order_apd) AS cancelled_order_apd,
        MAX(shipped_order_count) AS shipped_order_count,
        MAX(shipped_order_amount) AS shipped_order_amount,
        MAX(shipped_order_apd) AS shipped_order_apd,
        MAX(payment_apd) AS payment_apd,
        MAX(payment_amount) AS payment_amount,
        MAX(products_ordered_qty) AS products_ordered_qty,
        MAX(products_items_qty) AS products_items_qty,
        MAX(order_mrp_amount) AS order_mrp_amount,
        MAX(new_customer_apd) AS new_customer_apd,
        MAX(new_customer_paid_apd) AS new_customer_paid_apd,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        max(b.etl_batch_no) AS etl_batch_no,
        max(b.etl_batch_date) AS etl_batch_date
    FROM X 
    cross join batch_metadata b
    GROUP BY summary_date,
             dw_customer_id