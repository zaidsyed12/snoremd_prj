with source as (
    select * from {{ source('raw', 'CLINICIAN_NOTES') }}
),
cleaned as (
    select
        note_id                                 as note_id,
        patient_id                              as patient_id,
        clinician_id                            as clinician_id,
        appointment_id                          as appointment_id,
        note_date                               as note_date,
        lower(trim(note_type))                  as note_type,
        trim(content)                           as content,
        created_at                              as created_at
    from source
    where note_id is not null
      and patient_id is not null
)
select * from cleaned
