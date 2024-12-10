{{ config(
    materialized='incremental',
    unique_key='productline'
) }}
WITH batch_metadata AS (
    SELECT
        etl_batch_no,
        etl_batch_date
    FROM etl_metadata.batch_control
    ORDER BY etl_batch_date DESC -- Adjust the order criteria as per your requirement
    LIMIT 1 -- Fetch only the latest batch
),
processed_data AS (
    SELECT
        st.productline,
        '' AS textdescription,
        COALESCE(dw.src_create_timestamp, st.create_timestamp) AS src_create_timestamp,
        COALESCE(st.update_timestamp, dw.src_update_timestamp) AS src_update_timestamp,
        batch.etl_batch_no AS etl_batch_no,
        batch.etl_batch_date AS etl_batch_date,
        COALESCE(dw.dw_update_timestamp, CURRENT_TIMESTAMP) AS dw_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        ROW_NUMBER() OVER (ORDER BY st.productline) 
            + COALESCE(MAX(dw.dw_product_line_id) OVER (), 0) AS dw_product_line_id,
           dw.productline AS dw_productline 
    FROM 
        {{source("devstage","productlines")}} AS st
    LEFT JOIN {{this}} AS dw
        ON st.productline = dw.productline
    CROSS JOIN batch_metadata batch
)
SELECT dw_product_line_id,productline,textdescription,src_create_timestamp,src_update_timestamp,dw_create_timestamp,dw_update_timestamp,
etl_batch_no,etl_batch_date
FROM processed_data

{% if is_incremental() %}
WHERE 
    processed_data.src_update_timestamp > processed_data.dw_update_timestamp
    OR processed_data.dw_productline IS NULL  -- Handle new and updated rows only
{% endif %}
