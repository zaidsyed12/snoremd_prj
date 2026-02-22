-- fct_sleep_studies.sql
-- Fact table at sleep study grain (one row per sleep study).
--
-- Key derived fields:
--   is_completed       : study status = 'completed'
--   severity_category  : derived from AHI score (clinical standard thresholds)
--                        Normal  : AHI < 5
--                        Mild    : 5 <= AHI < 15
--                        Moderate: 15 <= AHI < 30
--                        Severe  : AHI >= 30

with studies as (
    select * from {{ ref('stg_sleep_studies') }}
),

patients as (
    select patient_sk, patient_id from {{ ref('dim_patients') }}
),

clinics as (
    select clinic_sk, clinic_id from {{ ref('dim_clinics') }}
),

clinicians as (
    select clinician_sk, clinician_id from {{ ref('dim_clinicians') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['s.study_id']) }}          as study_sk,

        -- Natural key
        s.study_id                                                       as study_id,

        -- Foreign keys
        p.patient_sk                                                     as patient_sk,
        c.clinic_sk                                                      as clinic_sk,
        cl.clinician_sk                                                  as clinician_sk,

        -- Study attributes
        s.study_date                                                     as study_date,
        date_trunc('month', s.study_date)                                as study_month,
        s.study_type                                                     as study_type,
        s.status                                                         as status,

        -- Derived: completion
        (s.status = 'completed')                                         as is_completed,

        -- Clinical scores (null for incomplete studies)
        s.ahi_score                                                      as ahi_score,
        s.odi_score                                                      as odi_score,
        s.spo2_nadir                                                     as spo2_nadir,

        -- Derived: OSA severity category (AASM standard)
        case
            when s.ahi_score is null     then 'unknown'
            when s.ahi_score < 5         then 'normal'
            when s.ahi_score < 15        then 'mild'
            when s.ahi_score < 30        then 'moderate'
            else                              'severe'
        end                                                              as severity_category,

        s.report_generated_at                                            as report_generated_at,
        datediff('day', s.study_date, s.report_generated_at)            as days_to_report,

        s.created_at                                                     as created_at,
        current_timestamp()                                              as dbt_updated_at

    from studies s
    left join patients   p  on s.patient_id   = p.patient_id
    left join clinics    c  on s.clinic_id    = c.clinic_id
    left join clinicians cl on s.clinician_id = cl.clinician_id
)

select * from final
