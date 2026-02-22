-- dim_clinics.sql
-- Clinic dimension. Small lookup table — built from RAW.CLINICS reference data.

with source as (
    select * from {{ source('raw', 'CLINICS') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['clinic_id']) }}   as clinic_sk,
        clinic_id                                               as clinic_id,
        clinic_name                                             as clinic_name,
        city                                                    as city,
        province                                                as province,
        true                                                    as is_active,
        current_timestamp()                                     as dbt_updated_at
    from source
    where clinic_id is not null
)

select * from final
