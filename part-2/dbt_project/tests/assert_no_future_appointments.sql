-- assert_no_future_appointments.sql
-- Data quality check: no appointment_date should be in the future.
-- dbt test passes when this query returns 0 rows.

select
    appointment_id,
    appointment_date,
    current_date() as today
from {{ ref('fct_appointments') }}
where appointment_date > current_date()
