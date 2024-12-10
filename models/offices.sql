
{{ config(
    materialized='incremental',
    unique_key='officecode'
) }}

with ranked_data as (
    select
        sd.officecode as officecode,
        sd.city,
        sd.phone,
        sd.addressline1,
        sd.addressline2,
        sd.state,
        sd.country,
        sd.postalcode,
        sd.territory,
        sd.create_timestamp as src_create_timestamp,
        coalesce(sd.update_timestamp, ed.src_update_timestamp) as src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        case
            when ed.officecode is null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_create_timestamp,
        case
            when ed.officecode is not null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_update_timestamp,
        row_number() over (order by sd.officecode) + coalesce(max(ed.dw_office_id) over (), 0) as dw_office_id
    from
        devstage.offices sd
    left join devdw.offices ed on sd.officecode = ed.officecode
    cross join etl_metadata.batch_control em
)

select *
from ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.officecode IS NOT NULL  -- Only process new or updated rows
{% endif %}
