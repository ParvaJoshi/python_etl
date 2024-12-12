{{ config(
    materialized='incremental',
    unique_key='src_orderNumber'
) }}

-- Fetch the latest batch metadata
WITH batch_control AS (
    SELECT 
        etl_batch_no, 
        etl_batch_date
    from etl_metadata.batch_control
),

-- Fetch staging data for orders
staging_orders AS (
    SELECT
        orderNumber AS src_orderNumber,
        orderDate,
        requiredDate,
        shippedDate,
        status,
        cancelledDate,
        customerNumber AS src_customerNumber,
        create_timestamp AS src_create_timestamp,
        update_timestamp AS src_update_timestamp
    FROM {{ source('devstage', 'orders') }}
),
-- Fetch the corresponding dw_customer_id from the Customers table
customer_mapping AS (
    SELECT 
        src_customerNumber,
        dw_customer_id
    FROM {{ ref('customers') }}  -- Referring to the customers model in dbt
),
-- Determine the current max dw_order_id in the target table
max_id AS (
    SELECT 
        COALESCE(MAX(dw_order_id), 0) AS max_order_id
    FROM {{ this }}
),

-- Combine staging data with existing target table to identify new and updated records
final_data AS (
    SELECT
        st.src_orderNumber,
        st.orderDate,
        st.requiredDate,
        st.shippedDate,
        st.status,
        st.cancelledDate,
        st.src_customerNumber,
        st.src_create_timestamp,
        st.src_update_timestamp,
        cm.dw_customer_id,
        '' as comments,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        bc.etl_batch_no,
        bc.etl_batch_date,
        -- Generate auto-increment dw_order_id for new records
        coalesce(dw.dw_order_id,ROW_NUMBER() OVER () + (SELECT max_order_id FROM max_id)) AS dw_order_id
    FROM staging_orders AS st
    CROSS JOIN batch_control AS bc
    JOIN customer_mapping AS cm
        ON st.src_customerNumber = cm.src_customerNumber
    LEFT JOIN {{ this }} AS dw
        ON st.src_orderNumber = dw.src_orderNumber
    WHERE dw.src_orderNumber IS NULL
    OR st.src_update_timestamp > dw.src_update_timestamp
)

-- Insert or update records in the target table
SELECT *
FROM final_data
{% if is_incremental() %}
WHERE src_orderNumber IS NOT NULL -- Adjust this condition as needed
{% endif %}
