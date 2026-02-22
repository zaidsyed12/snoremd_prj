-- mart_clinic_monthly_metrics.sql
-- Pre-aggregated mart for the Retool dashboard.
-- Grain: one row per clinic per calendar month.
--
-- Powers:
--   - KPI tiles  (total studies, completion rate, follow-up compliance rate)
--   - Monthly trend chart
--   - Clinic selector parameterised queries

with appointments as (
    select
        clinic_sk,
        appointment_month,
        count(*)                                                    as total_appointments,
        sum(case when is_completed then 1 else 0 end)               as completed_appointments,
        sum(case when status = 'cancelled' then 1 else 0 end)       as cancelled_appointments,
        sum(case when status = 'no_show' then 1 else 0 end)         as no_show_appointments,
        -- Follow-up compliance: patients who HAD a completed appt and got a follow-up within 30d
        sum(case when is_completed and has_follow_up_within_30d then 1 else 0 end) as patients_with_followup_30d,
        sum(case when is_completed then 1 else 0 end)               as patients_needing_followup
    from {{ ref('fct_appointments') }}
    group by 1, 2
),

studies as (
    select
        clinic_sk,
        study_month,
        count(*)                                                    as total_studies,
        sum(case when is_completed then 1 else 0 end)               as completed_studies
    from {{ ref('fct_sleep_studies') }}
    group by 1, 2
),

billing as (
    select
        c.clinic_sk,
        date_trunc('month', b.service_date)                         as billing_month,
        sum(b.amount)                                               as total_billed_amount,
        sum(case when b.billing_status = 'paid' then b.amount
                 when b.billing_status = 'approved' then b.amount
                 else 0 end)                                        as total_collected_amount
    from {{ ref('stg_billing') }} b
    join {{ ref('dim_clinics') }} c on b.clinic_id = c.clinic_id
    group by 1, 2
),

clinics as (
    select clinic_sk, clinic_id, clinic_name, city, province
    from {{ ref('dim_clinics') }}
),

-- Union all months from both appointments and studies to ensure completeness
all_months as (
    select clinic_sk, appointment_month as month_date from appointments
    union
    select clinic_sk, study_month        as month_date from studies
),

final as (
    select
        cl.clinic_sk                                                as clinic_sk,
        cl.clinic_id                                                as clinic_id,
        cl.clinic_name                                              as clinic_name,
        cl.city                                                     as city,
        cl.province                                                 as province,
        m.month_date                                                as month_date,
        to_varchar(m.month_date, 'YYYY-MM')                         as month_year,

        -- Appointment KPIs
        coalesce(a.total_appointments, 0)                           as total_appointments,
        coalesce(a.completed_appointments, 0)                       as completed_appointments,
        coalesce(a.cancelled_appointments, 0)                       as cancelled_appointments,
        coalesce(a.no_show_appointments, 0)                         as no_show_appointments,
        case
            when coalesce(a.total_appointments, 0) = 0 then null
            else round(a.completed_appointments / a.total_appointments, 4)
        end                                                         as appointment_completion_rate,

        -- Sleep study KPIs
        coalesce(s.total_studies, 0)                                as total_studies,
        coalesce(s.completed_studies, 0)                            as completed_studies,
        case
            when coalesce(s.total_studies, 0) = 0 then null
            else round(s.completed_studies / s.total_studies, 4)
        end                                                         as study_completion_rate,

        -- Follow-up compliance KPI
        coalesce(a.patients_with_followup_30d, 0)                   as patients_with_followup_30d,
        coalesce(a.patients_needing_followup, 0)                    as patients_needing_followup,
        case
            when coalesce(a.patients_needing_followup, 0) = 0 then null
            else round(a.patients_with_followup_30d / a.patients_needing_followup, 4)
        end                                                         as followup_compliance_rate,

        -- Billing KPIs
        coalesce(b.total_billed_amount, 0)                          as total_billed_amount,
        coalesce(b.total_collected_amount, 0)                       as total_collected_amount,

        current_timestamp()                                         as dbt_updated_at

    from all_months m
    join clinics cl on m.clinic_sk = cl.clinic_sk
    left join appointments a on m.clinic_sk = a.clinic_sk and m.month_date = a.appointment_month
    left join studies      s on m.clinic_sk = s.clinic_sk and m.month_date = s.study_month
    left join billing      b on m.clinic_sk = b.clinic_sk and m.month_date = b.billing_month
)

select * from final
order by clinic_id, month_date
