-- assert_completion_rate_valid.sql
-- Data quality check: all non-null completion rates in the mart must be
-- between 0.0 and 1.0 (i.e. valid percentages as decimals).
-- dbt test passes when this query returns 0 rows.

select
    clinic_id,
    month_year,
    study_completion_rate,
    followup_compliance_rate,
    appointment_completion_rate
from {{ ref('mart_clinic_monthly_metrics') }}
where
    (study_completion_rate        is not null and (study_completion_rate < 0        or study_completion_rate > 1))
    or
    (followup_compliance_rate     is not null and (followup_compliance_rate < 0     or followup_compliance_rate > 1))
    or
    (appointment_completion_rate  is not null and (appointment_completion_rate < 0  or appointment_completion_rate > 1))
