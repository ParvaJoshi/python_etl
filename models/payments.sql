{{ config(
    materialized='incremental'
) }}

-- Fetch the latest batch metadata
WITH batch_control AS (
    SELECT 
        etl_batch_no, 
        etl_batch_date
    FROM etl_metadata.batch_control
),

-- Fetch new or updated payments records from the staging table
staging_payments AS (
    SELECT
        customerNumber AS src_customerNumber,
        checkNumber,
        paymentDate,
        amount,
        create_timestamp AS src_create_timestamp,
        update_timestamp AS src_update_timestamp
    FROM {{ source('devstage', 'payments') }}
),

-- Get the customer foreign key for payments
customer_lookup AS (
    SELECT
        src_customerNumber,
        dw_customer_id
    FROM {{ ref('customers') }}
),

-- Combine staging data with the target table to check for existing records
final_data AS (
    SELECT
        sp.src_customerNumber,
        sp.checkNumber,
        sp.paymentDate,
        sp.amount,
        sp.src_create_timestamp,
        sp.src_update_timestamp,
        coalesce(dw.dw_create_timestamp,CURRENT_TIMESTAMP) AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp,
        bc.etl_batch_no,
        bc.etl_batch_date,
        cl.dw_customer_id,
        coalesce(dw.dw_payment_id,row_number() over () + coalesce(max(dw.dw_payment_id) over (), 0)) as dw_payment_id
    FROM staging_payments AS sp
    CROSS JOIN batch_control AS bc
    LEFT JOIN {{ this }} AS dw
        ON sp.checkNumber = dw.checkNumber
    LEFT JOIN customer_lookup AS cl
        ON sp.src_customerNumber = cl.src_customerNumber
    WHERE dw.checkNumber IS NULL
)

-- Insert or update records in the target table
SELECT *
FROM final_data
{% if is_incremental() %}
WHERE checkNumber IS NOT NULL -- Adjust this condition as needed
{% endif %}
