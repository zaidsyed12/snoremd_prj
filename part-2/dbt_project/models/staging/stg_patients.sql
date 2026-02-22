with source as (
    select * from {{ source('raw', 'PATIENTS') }}
),
cleaned as (
    select
        patient_id                              as patient_id,
        trim(first_name)                        as first_name,
        trim(last_name)                         as last_name,
        date_of_birth                           as date_of_birth,
        upper(trim(gender))                     as gender,
        lower(trim(email))                      as email,
        trim(phone)                             as phone,
        clinic_id                               as clinic_id,
        coalesce(is_active, true)               as is_active,
        created_at                              as created_at,
        updated_at                              as updated_at
    from source
    where patient_id is not null
)
select * from cleaned
