{{ config(
    materialized='incremental'
) }}
WITH source_data AS (
    SELECT
        st.officeCode,
        st.city,
        st.phone,
        st.addressLine1,
        st.addressLine2,
        st.state,
        st.country,
        st.postalCode,
        st.territory,
        st.create_timestamp,
        st.update_timestamp,
        bc.etl_batch_no,
        bc.etl_batch_date
    FROM {{source("devstage","offices")}} st
    CROSS JOIN (
        SELECT etl_batch_no, etl_batch_date
        FROM etl_metadata.batch_control
    ) bc
),
existing_data AS (
    SELECT
        dw.officeCode,
        dw.dw_office_id
    FROM {{this}} dw
),
ranked_data AS (
    SELECT
        s.*,
        COALESCE(MAX(dw.dw_office_id) OVER (), 0) + ROW_NUMBER() OVER (ORDER BY s.officeCode) AS dw_office_id
    FROM source_data s
    LEFT JOIN existing_data dw ON s.officeCode = dw.officeCode
)
SELECT *
FROM ranked_data
{% if is_incremental() %}
WHERE officeCode IS NOT NULL -- Adjust this condition as needed
{% endif %}
