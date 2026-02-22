with source as (
    select * from {{ source('raw', 'SLEEP_STUDIES') }}
),
cleaned as (
    select
        study_id                                as study_id,
        patient_id                              as patient_id,
        clinic_id                               as clinic_id,
        clinician_id                            as clinician_id,
        study_date                              as study_date,
        lower(trim(study_type))                 as study_type,
        lower(trim(status))                     as status,
        ahi_score                               as ahi_score,
        odi_score                               as odi_score,
        spo2_nadir                              as spo2_nadir,
        report_generated_at                     as report_generated_at,
        created_at                              as created_at
    from source
    where study_id is not null
)
select * from cleaned
