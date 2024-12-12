{{ config(
    materialized='incremental',
    unique_key=['dw_product_id', 'msrp'] 
)}}

-- CTE for fetching batch control data
WITH batch_control AS (
    SELECT 
        etl_batch_no, 
        etl_batch_date
    FROM etl_metadata.batch_control
    LIMIT 1
),
source_data AS (
    SELECT 
        a.dw_product_id,
        a.msrp,
        bc.etl_batch_date AS effective_from_date,
        DATEADD(day, -1, bc.etl_batch_date) AS effective_to_date,
        bc.etl_batch_no AS etl_batch_no,
        bc.etl_batch_date AS etl_batch_date
    FROM {{ ref('products') }} AS a
    CROSS JOIN batch_control AS bc
),
existing_data AS (
    SELECT 
        h.dw_product_id,
        h.msrp,
        h.dw_active_record_ind,
        h.effective_from_date,
        h.dw_create_timestamp,
        h.create_etl_batch_no,
        h.create_etl_batch_date
    FROM {{ this }} AS h
    WHERE h.dw_active_record_ind = 1
)
SELECT
    -- When creditLimit changes, set existing records to inactive
    ed.dw_product_id,
    ed.msrp,
    0 AS dw_active_record_ind,
    ed.effective_from_date,
    sd.effective_to_date,
    CURRENT_TIMESTAMP AS dw_update_timestamp,
    ed.dw_create_timestamp,
    ed.create_etl_batch_no,
    ed.create_etl_batch_date,
    sd.etl_batch_no as update_etl_batch_no,
    sd.etl_batch_date as update_etl_batch_date
FROM source_data AS sd
JOIN existing_data AS ed
    ON sd.dw_product_id = ed.dw_product_id 
    and sd.msrp  != ed.msrp
UNION ALL
SELECT
    -- When creditLimit changes, insert new records
    sd.dw_product_id,
    sd.msrp,
    1 AS dw_active_record_ind,
    sd.effective_from_date,
    '2099-12-31'::DATE as effective_to_date,
    CURRENT_TIMESTAMP AS dw_update_timestamp,
    CURRENT_TIMESTAMP as dw_create_timestamp,
    sd.etl_batch_no as create_etl_batch_no,
    sd.etl_batch_date as create_etl_batch_date,
    sd.etl_batch_no as update_etl_batch_no,
    sd.etl_batch_date as update_etl_batch_date
FROM source_data AS sd
 left JOIN existing_data AS ed
    ON sd.dw_product_id = ed.dw_product_id 
    and sd.msrp  = ed.msrp
where ed.dw_product_id is null