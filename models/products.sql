{{ config(
    materialized='incremental'
) }}

WITH batch_metadata AS (
    SELECT
        etl_batch_no,
        etl_batch_date
    FROM etl_metadata.batch_control
   
),

merged_data AS (
    SELECT
        s.productcode AS src_productcode,
        s.productname,
        s.productline,
        s.productscale,
        s.productvendor,
        s.quantityinstock,
        s.buyprice,
        s.msrp,
        s.create_timestamp,
        s.update_timestamp,
        bm.etl_batch_no,
        bm.etl_batch_date,
        e.src_productcode AS existing_src_productcode,
        e.dw_product_line_id AS existing_dw_product_line_id,
        e.src_create_timestamp AS existing_src_create_timestamp,
        e.src_update_timestamp AS existing_src_update_timestamp,
        e.dw_update_timestamp AS existing_dw_update_timestamp,
        e.dw_product_id AS existing_dw_product_id,
        pl.dw_product_line_id AS product_line_id
    FROM
        {{source("devstage","products")}} s
    CROSS JOIN
        batch_metadata bm
    LEFT JOIN
        {{this}} e
    ON
        s.productcode = e.src_productcode
    LEFT JOIN
        {{ref('productlines')}} pl
    ON
        s.productline = pl.productline
)

SELECT
    src_productcode,
    productname,
    productline,
    productscale,
    productvendor,
    quantityinstock,
    buyprice,
    msrp,
    '' as productdescription,
    COALESCE(product_line_id, existing_dw_product_line_id) AS dw_product_line_id,
    CASE
        WHEN existing_src_productcode IS NULL THEN create_timestamp
        ELSE existing_src_create_timestamp
    END AS src_create_timestamp,
    COALESCE(update_timestamp, existing_src_update_timestamp) AS src_update_timestamp,
    etl_batch_no,
    etl_batch_date,
    CURRENT_TIMESTAMP AS dw_create_timestamp,
    CASE
        WHEN src_productcode IS NOT NULL THEN CURRENT_TIMESTAMP
        ELSE existing_dw_update_timestamp
    END AS dw_update_timestamp,
    ROW_NUMBER() OVER (ORDER BY src_productcode) + COALESCE(MAX(existing_dw_product_id) OVER (), 0) AS dw_product_id
FROM
    merged_data

{% if is_incremental() %}
WHERE
    src_productcode IS NOT NULL  -- Only process new or updated rows
{% endif %}
