{{ config(
    materialized='incremental',
    unique_key=['dw_customer_id', 'creditLimit']
) }}

with new_customers as (
    -- Insert new records: Customers that are not in customer_history or have active records
    select
        C.dw_customer_id,
        C.creditLimit,
        B.etl_batch_no,
        B.etl_batch_date,
        C.dw_create_timestamp
    from
        {{ ref('customers') }} C
    left join {{ this }} ch on C.dw_customer_id = ch.dw_customer_id
                            and ch.dw_active_record_ind = 1
    cross join metadata.batch_control B
    where ch.dw_customer_id is null
),

updated_customers as (
    -- Update existing records where creditLimit has changed
    select
        ch.dw_customer_id,
        ch.dw_active_record_ind,
        ch.effective_from_date,
        ch.effective_to_date,
        ch.creditLimit,
        ch.create_etl_batch_no,
        ch.create_etl_batch_date,
        ch.dw_create_timestamp,
        case
            when C.creditLimit <> ch.creditLimit then 0  -- mark for update
            else ch.dw_active_record_ind
        end as updated_dw_active_record_ind,
        case
            when C.creditLimit <> ch.creditLimit then current_timestamp
            else ch.dw_update_timestamp
        end as updated_dw_update_timestamp,
        case
            when C.creditLimit <> ch.creditLimit then B.etl_batch_no
            else ch.update_etl_batch_no
        end as update_etl_batch_no,
        case
            when C.creditLimit <> ch.creditLimit then B.etl_batch_date
            else ch.update_etl_batch_date
        end as update_etl_batch_date,
        case
            when C.creditLimit <> ch.creditLimit then B.etl_batch_date - INTERVAL '1 day'
            else ch.effective_to_date
        end as updated_effective_to_date
    from
        {{ this }} ch
    left join {{ ref('customers') }} C on ch.dw_customer_id = C.dw_customer_id
    cross join metadata.batch_control B
    where ch.dw_active_record_ind = 1
),

changed_data as (
    select C.dw_customer_id,
            C.creditLimit, 
            B.etl_batch_date AS effective_from_date,
            1 AS dw_active_record_ind,
            current_timestamp as dw_create_timestamp,
            B.etl_batch_no AS create_etl_batch_no,
            B.etl_batch_date AS create_etl_batch_date
    from {{ ref('customers') }} C 
    cross join metadata.batch_control B
    join {{ this }} ch on C.dw_customer_id = ch.dw_customer_id
        where C.etl_batch_no = B.etl_batch_no
        and C.src_create_timestamp <> C.src_update_timestamp
        and C.creditLimit <> ch.creditLimit
)

-- Combine the new customers (insert) and updated records (update)
select
    dw_customer_id,
    creditLimit,
    effective_from_date,
    updated_effective_to_date as effective_to_date,
    updated_dw_active_record_ind as dw_active_record_ind,
    create_etl_batch_no,
    create_etl_batch_date,
    update_etl_batch_no,
    update_etl_batch_date,
    dw_create_timestamp,
    updated_dw_update_timestamp as dw_update_timestamp
from
    updated_customers
union all
select
    dw_customer_id,
    creditLimit,
    etl_batch_date as effective_from_date,
    null as effective_to_date,
    1 as dw_active_record_ind,
    etl_batch_no as create_etl_batch_no,
    etl_batch_date as create_etl_batch_date,
    null as update_etl_batch_no,
    null as update_etl_batch_date,
    dw_create_timestamp,
    null as dw_update_timestamp
from
    new_customers
union all
select dw_customer_id,
        creditLimit,
        effective_from_date,
        null as effective_to_date,
        dw_active_record_ind,
        create_etl_batch_no,
        create_etl_batch_date,
        null as update_etl_batch_no,
        null as update_etl_batch_date,
        dw_create_timestamp,
        null as dw_update_timestamp
from 
    changed_data

