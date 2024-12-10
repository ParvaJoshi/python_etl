{{ config(
    materialized='incremental',
    unique_key='employeenumber'
) }}

with batch_control as (
    select etl_batch_no, etl_batch_date
    from etl_metadata.batch_control
    limit 1
),
ranked_data as (
    select
        sd.employeenumber,
        sd.lastname,
        sd.firstname,
        sd.extension,
        sd.email,
        sd.officecode,
        sd.reportsto,
        sd.jobtitle,
        o.dw_office_id,
        sd.create_timestamp as src_create_timestamp,
        coalesce(sd.update_timestamp, ed.src_update_timestamp) as src_update_timestamp,
        bc.etl_batch_no,
        bc.etl_batch_date,
        current_timestamp as dw_update_timestamp,
        case
            when ed.employeenumber is null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_create_timestamp,
        row_number() over (order by sd.employeenumber) + coalesce(max(ed.dw_employee_id) over (), 0) as dw_employee_id,
        0 as dw_reporting_employee_id  -- Placeholder for reporting relationship
    from {{ source('devstage', 'employees') }} sd
    left join {{ this }} ed on sd.employeenumber = ed.employeenumber
    join {{ ref('offices') }} o on sd.officecode = o.officecode
    cross join batch_control bc
),
updated_reporting as (
    select
        rd.employeenumber,
        coalesce(dw2.dw_employee_id, 0) as dw_reporting_employee_id
    from ranked_data rd
    left join devdw.employees dw2 on rd.reportsto = dw2.employeenumber
),

final_data as (
    select
        rd.employeenumber,
        rd.lastname,
        rd.firstname,
        rd.extension,
        rd.email,
        rd.officecode,
        rd.reportsto,
        rd.jobtitle,
        rd.dw_office_id,
        rd.src_create_timestamp,
        rd.src_update_timestamp,
        rd.etl_batch_no,
        rd.etl_batch_date,
        rd.dw_create_timestamp,
        rd.dw_update_timestamp,
        rd.dw_employee_id,
        greatest(coalesce(ur.dw_reporting_employee_id, 0), rd.dw_reporting_employee_id) as dw_reporting_employee_id
    from ranked_data rd
    left join updated_reporting ur on rd.employeenumber = ur.employeenumber
)

select * from final_data

{% if is_incremental() %}
where employeenumber is not null
{% endif %}
