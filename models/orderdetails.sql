{{ config(
    materialized='incremental',
    unique_key=['src_orderNumber', 'src_productCode']
) }}

-- Fetch the latest batch metadata
WITH batch_control AS (
    SELECT 
        etl_batch_no, 
        etl_batch_date
    FROM etl_metadata.batch_control
),

-- Fetch staging data for orderdetails
staging_orderdetails AS (
    SELECT
        orderNumber AS src_orderNumber,
        productCode AS src_productCode,
        quantityOrdered,
        priceEach,
        orderLineNumber,
        update_timestamp AS src_update_timestamp,
        create_timestamp AS src_create_timestamp
    FROM {{ source('devstage', 'orderdetails') }}
),

-- Fetch existing data in target for incremental load (only records that need updating or inserting)
existing_orderdetails AS (
    SELECT 
        dw_orderdetail_id,  
        src_orderNumber,
        src_productCode,
        dw_order_id,
        dw_product_id
    FROM {{ this }}
),


-- Combine staging and existing data to determine which records need to be inserted or updated
final_data AS (
    SELECT
        st.src_orderNumber,
        st.src_productCode,
        st.quantityOrdered,
        st.priceEach,
        st.orderLineNumber,
        st.src_create_timestamp,
        st.src_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        bc.etl_batch_no,
        bc.etl_batch_date,
        -- Add foreign keys for dw_order_id and dw_product_id
        o.dw_order_id,
        p.dw_product_id,
        row_number() over() + coalesce(max(dw.dw_orderdetail_id)over(),0) dw_orderdetail_id
    FROM staging_orderdetails AS st
    CROSS JOIN batch_control AS bc
    LEFT JOIN existing_orderdetails AS dw
        ON st.src_orderNumber = dw.src_orderNumber
        AND st.src_productCode = dw.src_productCode
    left join {{ref("orders")}} o  on st.src_ordernumber=o.src_ordernumber
    left join {{ref("products")}} p on st.src_productcode=p.src_productcode
    WHERE dw.src_orderNumber IS NULL
)

-- Insert or update records in the target table
SELECT
    dw_orderdetail_id,
    src_orderNumber,
    src_productCode,
    quantityOrdered,
    priceEach,
    orderLineNumber,
    src_create_timestamp,
    src_update_timestamp,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date,
    dw_order_id,
    dw_product_id
FROM final_data
{% if is_incremental() %}
WHERE src_orderNumber IS NOT NULL -- Adjust this condition as needed
{% endif %}
