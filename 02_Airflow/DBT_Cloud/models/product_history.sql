{{ config(
    materialized='incremental',
    unique_key=['dw_product_id', 'msrp']
) }}

with new_products as (
    -- Insert new records: products that are not in product_history or have active records
    select
        P.dw_product_id,
        P.msrp,
        B.etl_batch_no,
        B.etl_batch_date,
        P.dw_create_timestamp
    from
        {{ ref('products') }} P
    left join {{ this }} ph on P.dw_product_id = ph.dw_product_id
                            and ph.dw_active_record_ind = 1
    cross join metadata.batch_control B
    where ph.dw_product_id is null
),

updated_products as (
    -- Update existing records where msrp has changed
    select
        ph.dw_product_id,
        ph.dw_active_record_ind,
        ph.effective_from_date,
        ph.effective_to_date,
        ph.msrp,
        ph.create_etl_batch_no,
        ph.create_etl_batch_date,
        ph.dw_create_timestamp,
        case
            when P.msrp <> ph.msrp then 0  
            else ph.dw_active_record_ind
        end as updated_dw_active_record_ind,
        case
            when P.msrp <> ph.msrp then current_timestamp
            else ph.dw_update_timestamp
        end as updated_dw_update_timestamp,
        case
            when P.msrp <> ph.msrp then B.etl_batch_no
            else ph.update_etl_batch_no
        end as update_etl_batch_no,
        case
            when P.msrp <> ph.msrp then B.etl_batch_date
            else ph.update_etl_batch_date
        end as update_etl_batch_date,
        case
            when P.msrp <> ph.msrp then B.etl_batch_date - INTERVAL '1 day'
            else ph.effective_to_date
        end as updated_effective_to_date
    from
        {{ this }} ph
    left join {{ ref('products') }} P on ph.dw_product_id = P.dw_product_id
    cross join metadata.batch_control B
    where ph.dw_active_record_ind = 1
)

-- Combine the new products (insert) and updated records (update)
select
    dw_product_id,
    msrp,
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
    updated_products
union all
select
    dw_product_id,
    msrp,
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
    new_products
