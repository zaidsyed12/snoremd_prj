-- dim_patients.sql
-- Patient dimension.
-- PHI handling: email and phone are SHA-256 hashed (masked) to avoid storing
-- identifiable data in the analytics layer. Source data in RAW schema is
-- access-controlled separately.

with stg as (
    select * from {{ ref('stg_patients') }}
),

clinics as (
    select clinic_sk, clinic_id from {{ ref('dim_clinics') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['p.patient_id']) }}         as patient_sk,
        p.patient_id                                                      as patient_id,
        p.first_name                                                      as first_name,
        p.last_name                                                       as last_name,
        p.first_name || ' ' || p.last_name                               as full_name,
        p.date_of_birth                                                   as date_of_birth,
        datediff('year', p.date_of_birth, current_date())                as age,
        p.gender                                                          as gender,
        -- PHI masking: store hash of email/phone, not plaintext
        sha2(lower(trim(p.email)), 256)                                   as email_hash,
        sha2(regexp_replace(p.phone, '[^0-9]', ''), 256)                 as phone_hash,
        p.clinic_id                                                       as clinic_id,
        c.clinic_sk                                                       as clinic_sk,
        p.is_active                                                       as is_active,
        p.created_at                                                      as created_at,
        p.updated_at                                                      as updated_at,
        current_timestamp()                                               as dbt_updated_at
    from stg p
    left join clinics c on p.clinic_id = c.clinic_id
)

select * from final
