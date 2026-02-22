-- 1. Virtual Warehouse 
CREATE WAREHOUSE IF NOT EXISTS SNOREMD_WH
    WAREHOUSE_SIZE    = 'X-SMALL'
    AUTO_SUSPEND      = 60          
    AUTO_RESUME       = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT           = 'Snore MD analytics warehouse';

USE WAREHOUSE SNOREMD_WH;

-- 2. Database & Schemas (Medallion Architecture) 
CREATE DATABASE IF NOT EXISTS SNOREMD_DB
    COMMENT = 'Snore MD unified analytics database';

USE DATABASE SNOREMD_DB;

-- Bronze: raw ingested data, never modified after load
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Bronze layer — raw ingested tables';

-- Silver: dbt staging views (cleaned, typed, renamed)
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Silver layer — dbt staging models';

-- Gold: dbt analytics tables (star schema + marts)
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Gold layer — star schema dimensions, facts, and marts';

-- 3. RAW Tables

USE SCHEMA RAW;

-- Ingestion audit log — written to by Python ingestion scripts
CREATE TABLE IF NOT EXISTS RAW.INGESTION_LOG (
    log_id          NUMBER AUTOINCREMENT PRIMARY KEY,
    source_name     VARCHAR(100)    NOT NULL,
    file_name       VARCHAR(255)    NOT NULL,
    rows_ingested   INTEGER,
    ingested_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    status          VARCHAR(20)     NOT NULL,   -- 'success' | 'failed'
    error_message   VARCHAR(2000)
);

-- Clinic reference data
CREATE TABLE IF NOT EXISTS RAW.CLINICS (
    clinic_id       VARCHAR(10)     NOT NULL,
    clinic_name     VARCHAR(100)    NOT NULL,
    city            VARCHAR(50),
    province        VARCHAR(50),
    loaded_at       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Clinician reference data
CREATE TABLE IF NOT EXISTS RAW.CLINICIANS (
    clinician_id    VARCHAR(36)     NOT NULL,
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    specialty       VARCHAR(100),
    clinic_id       VARCHAR(10),
    loaded_at       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Patients (from CRM / CSV export)
CREATE TABLE IF NOT EXISTS RAW.PATIENTS (
    patient_id      VARCHAR(36)     NOT NULL,
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    date_of_birth   DATE,
    gender          VARCHAR(10),
    email           VARCHAR(150),
    phone           VARCHAR(30),
    clinic_id       VARCHAR(10),
    is_active       BOOLEAN,
    created_at      TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    _ingested_at    TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Appointments (from scheduling API / JSON)
CREATE TABLE IF NOT EXISTS RAW.APPOINTMENTS (
    appointment_id      VARCHAR(36)     NOT NULL,
    patient_id          VARCHAR(36),
    clinic_id           VARCHAR(10),
    clinician_id        VARCHAR(36),
    appointment_date    DATE,
    appointment_type    VARCHAR(50),
    status              VARCHAR(20),
    duration_minutes    INTEGER,
    created_at          TIMESTAMP_NTZ,
    _ingested_at        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Sleep studies (from Azure Blob CSV exports)
CREATE TABLE IF NOT EXISTS RAW.SLEEP_STUDIES (
    study_id                VARCHAR(36)     NOT NULL,
    patient_id              VARCHAR(36),
    clinic_id               VARCHAR(10),
    clinician_id            VARCHAR(36),
    study_date              DATE,
    study_type              VARCHAR(30),
    status                  VARCHAR(20),
    ahi_score               FLOAT,
    odi_score               FLOAT,
    spo2_nadir              FLOAT,
    report_generated_at     TIMESTAMP_NTZ,
    created_at              TIMESTAMP_NTZ,
    _ingested_at            TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Clinician notes (from S3 JSON files)
CREATE TABLE IF NOT EXISTS RAW.CLINICIAN_NOTES (
    note_id         VARCHAR(36)     NOT NULL,
    patient_id      VARCHAR(36),
    clinician_id    VARCHAR(36),
    appointment_id  VARCHAR(36),
    note_date       TIMESTAMP_NTZ,
    note_type       VARCHAR(50),
    content         VARCHAR(4000),
    created_at      TIMESTAMP_NTZ,
    _ingested_at    TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Billing (from monthly Excel files)
CREATE TABLE IF NOT EXISTS RAW.BILLING (
    billing_id          VARCHAR(36)     NOT NULL,
    patient_id          VARCHAR(36),
    clinic_id           VARCHAR(10),
    appointment_id      VARCHAR(36),
    service_date        DATE,
    service_code        VARCHAR(10),
    service_description VARCHAR(100),
    amount              FLOAT,
    insurance_provider  VARCHAR(100),
    billing_status      VARCHAR(20),
    billing_month       VARCHAR(7),     -- YYYY-MM
    created_at          TIMESTAMP_NTZ,
    _ingested_at        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- 4. Indexes / Clustering (snowflake-> micro-partition pruning) 
ALTER TABLE RAW.APPOINTMENTS  CLUSTER BY (appointment_date, clinic_id);
ALTER TABLE RAW.SLEEP_STUDIES CLUSTER BY (study_date, clinic_id);
ALTER TABLE RAW.BILLING       CLUSTER BY (billing_month, clinic_id);

SELECT 'Snore MD Snowflake setup complete.' AS status;
