with source as (
    select * from {{ source('raw', 'APPOINTMENTS') }}
),
cleaned as (
    select
        appointment_id                                      as appointment_id,
        patient_id                                          as patient_id,
        clinic_id                                           as clinic_id,
        clinician_id                                        as clinician_id,
        appointment_date                                    as appointment_date,
        lower(trim(appointment_type))                       as appointment_type,
        lower(trim(status))                                 as status,
        coalesce(duration_minutes, 30)                      as duration_minutes,
        created_at                                          as created_at
    from source
    where appointment_id is not null
)
select * from cleaned
