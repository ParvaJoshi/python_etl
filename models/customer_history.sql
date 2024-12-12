{{ config(
    materialized='incremental',
    unique_key=['dw_customer_id', 'creditlimit'] 
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
        a.dw_customer_id,
        a.creditlimit,
        bc.etl_batch_date AS effective_from_date,
        DATEADD(day, -1, bc.etl_batch_date) AS effective_to_date,
        bc.etl_batch_no AS etl_batch_no,
        bc.etl_batch_date AS etl_batch_date
    FROM {{ ref('customers') }} AS a
    CROSS JOIN batch_control AS bc
),
existing_data AS (
    SELECT 
        h.dw_customer_id,
        h.creditlimit,
        h.dw_active_record_ind,
        h.effective_from_date,
        h.dw_create_timestamp,
        h.create_etl_batch_no,
        h.create_etl_batch_date
    FROM {{ this }} AS h
    WHERE h.dw_active_record_ind = 1
)

-- Insert new and update existing data based on the creditLimit change
SELECT
    -- When creditLimit changes, set existing records to inactive
    ed.dw_customer_id,
    ed.creditlimit,
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
    ON sd.dw_customer_id = ed.dw_customer_id 
    and sd.creditlimit  != ed.creditlimit
UNION ALL
SELECT
    -- When creditLimit changes, insert new records
    sd.dw_customer_id,
    sd.creditlimit,
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
    ON sd.dw_customer_id = ed.dw_customer_id 
    and sd.creditlimit  = ed.creditlimit
where ed.dw_customer_id is null

