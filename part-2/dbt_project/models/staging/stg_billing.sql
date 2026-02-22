with source as (
    select * from {{ source('raw', 'BILLING') }}
),
cleaned as (
    select
        billing_id                              as billing_id,
        patient_id                              as patient_id,
        clinic_id                               as clinic_id,
        appointment_id                          as appointment_id,
        service_date                            as service_date,
        upper(trim(service_code))               as service_code,
        trim(service_description)               as service_description,
        amount                                  as amount,
        trim(insurance_provider)                as insurance_provider,
        lower(trim(billing_status))             as billing_status,
        trim(billing_month)                     as billing_month,
        created_at                              as created_at
    from source
    where billing_id is not null
      and amount > 0
)
select * from cleaned
