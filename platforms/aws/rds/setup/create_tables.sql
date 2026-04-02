-- ── Insurance schema ─────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS insurance;

CREATE TABLE IF NOT EXISTS insurance.claims (
    claim_id            VARCHAR(20)     PRIMARY KEY,
    policy_number       VARCHAR(20)     NOT NULL,
    claimant_name       VARCHAR(100)    NOT NULL,
    ssn_last4           VARCHAR(4)      NOT NULL,
    claim_date          DATE            NOT NULL,
    claim_type          VARCHAR(50)     NOT NULL,
    amount_claimed      NUMERIC(12,2)   NOT NULL,
    amount_approved     NUMERIC(12,2),
    status              VARCHAR(20)     NOT NULL,
    adjuster_id         VARCHAR(10),
    incident_location   VARCHAR(100),
    medical_expenses    NUMERIC(12,2)   DEFAULT 0,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS insurance.policies (
    policy_number       VARCHAR(20)     PRIMARY KEY,
    policy_holder       VARCHAR(100)    NOT NULL,
    policy_type         VARCHAR(50)     NOT NULL,
    start_date          DATE            NOT NULL,
    end_date            DATE            NOT NULL,
    premium_amount      NUMERIC(10,2)   NOT NULL,
    coverage_limit      NUMERIC(12,2)   NOT NULL,
    status              VARCHAR(20)     NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ── Pharma schema ─────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS pharma;

CREATE TABLE IF NOT EXISTS pharma.adverse_events (
    event_id                VARCHAR(20)     PRIMARY KEY,
    report_date             DATE            NOT NULL,
    drug_name               VARCHAR(100)    NOT NULL,
    ndc_code                VARCHAR(20),
    patient_age_group       VARCHAR(10),
    patient_gender          CHAR(1),
    adverse_event_type      VARCHAR(50),
    severity                VARCHAR(20)     NOT NULL,
    outcome                 VARCHAR(30),
    reporter_type           VARCHAR(30),
    country                 VARCHAR(5)      DEFAULT 'US',
    narrative               TEXT,
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pharma.drugs (
    ndc_code                VARCHAR(20)     PRIMARY KEY,
    drug_name               VARCHAR(100)    NOT NULL,
    manufacturer            VARCHAR(100),
    drug_class              VARCHAR(50),
    approval_date           DATE,
    status                  VARCHAR(20)     DEFAULT 'active',
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ── Audit log table (compliance pattern) ─────────────────────────────

CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id              SERIAL          PRIMARY KEY,
    pipeline_name       VARCHAR(100)    NOT NULL,
    source_table        VARCHAR(100),
    target_location     VARCHAR(255),
    records_extracted   INTEGER,
    status              VARCHAR(20),
    started_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    completed_at        TIMESTAMP,
    error_message       TEXT
);