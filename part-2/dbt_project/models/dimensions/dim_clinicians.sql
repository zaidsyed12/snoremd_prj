-- dim_clinicians.sql
-- Clinician dimension — joins clinician reference data to clinic.

with clinicians as (
    select * from {{ source('raw', 'CLINICIANS') }}
),

clinics as (
    select clinic_sk, clinic_id from {{ ref('dim_clinics') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['cl.clinician_id']) }}     as clinician_sk,
        cl.clinician_id                                                  as clinician_id,
        trim(cl.first_name)                                              as first_name,
        trim(cl.last_name)                                               as last_name,
        trim(cl.first_name) || ' ' || trim(cl.last_name)                as full_name,
        cl.specialty                                                     as specialty,
        cl.clinic_id                                                     as clinic_id,
        c.clinic_sk                                                      as clinic_sk,
        true                                                             as is_active,
        current_timestamp()                                              as dbt_updated_at
    from clinicians cl
    left join clinics c on cl.clinic_id = c.clinic_id
    where cl.clinician_id is not null
)

select * from final
