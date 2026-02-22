-- fct_appointments.sql
-- Fact table at appointment grain (one row per appointment).
--
-- derived fields:
--   is_completed             
--   is_follow_up              
--   next_appointment_date      
--   days_to_next_appointment  
--   has_follow_up_within_30d  

with appts as (
    select * from {{ ref('stg_appointments') }}
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

-- Calculate next appointment per patient using LEAD window function
appts_with_next as (
    select
        appointment_id,
        patient_id,
        clinic_id,
        clinician_id,
        appointment_date,
        appointment_type,
        status,
        duration_minutes,
        created_at,

        lead(appointment_date) over (
            partition by patient_id
            order by appointment_date asc
        ) as next_appointment_date,

        lead(appointment_type) over (
            partition by patient_id
            order by appointment_date asc
        ) as next_appointment_type

    from appts
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['a.appointment_id']) }}    as appointment_sk,

        -- Natural key
        a.appointment_id                                                 as appointment_id,

        -- Foreign keys to dimensions
        p.patient_sk                                                     as patient_sk,
        c.clinic_sk                                                      as clinic_sk,
        cl.clinician_sk                                                  as clinician_sk,

        -- Appointment attributes
        a.appointment_date                                               as appointment_date,
        date_trunc('month', a.appointment_date)                          as appointment_month,
        a.appointment_type                                               as appointment_type,
        a.status                                                         as status,
        a.duration_minutes                                               as duration_minutes,

        -- Derived: completion flags
        (a.status = 'completed')                                         as is_completed,
        (a.appointment_type in ('follow_up', 'cpap_followup'))           as is_follow_up,

        -- Derived: 30-day follow-up logic
        a.next_appointment_date                                          as next_appointment_date,
        datediff('day', a.appointment_date, a.next_appointment_date)     as days_to_next_appointment,

        case
            when a.next_appointment_date is not null
             and datediff('day', a.appointment_date, a.next_appointment_date) <= 30
            then true
            else false
        end                                                              as has_follow_up_within_30d,

        a.created_at                                                     as created_at,
        current_timestamp()                                              as dbt_updated_at

    from appts_with_next a
    left join patients  p  on a.patient_id   = p.patient_id
    left join clinics   c  on a.clinic_id    = c.clinic_id
    left join clinicians cl on a.clinician_id = cl.clinician_id
)

select * from final
