-- Fleet Telemetry bootcamp schema (PostgreSQL 15)
-- Loaded automatically by docker compose on first start.

CREATE TABLE customers (
    id      SERIAL PRIMARY KEY,
    name    TEXT NOT NULL UNIQUE,
    type    TEXT NOT NULL CHECK (type IN ('fleet', 'rental', 'dealer', 'lender', 'insurer')),
    status  TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'churned'))
);

CREATE TABLE vehicles (
    vin          CHAR(17) PRIMARY KEY,
    customer_id  INT NOT NULL REFERENCES customers(id),
    oem          TEXT NOT NULL,
    model_year   INT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'deprovisioned')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_vehicles_customer ON vehicles (customer_id);

CREATE TABLE devices (
    id            SERIAL PRIMARY KEY,
    vin           CHAR(17) NOT NULL REFERENCES vehicles(vin),
    provider      TEXT NOT NULL,
    firmware      TEXT NOT NULL,
    last_seen_at  TIMESTAMPTZ
);

CREATE TABLE subscriptions (
    id           SERIAL PRIMARY KEY,
    customer_id  INT NOT NULL REFERENCES customers(id),
    vin          CHAR(17) NOT NULL REFERENCES vehicles(vin),
    plan         TEXT NOT NULL,
    start_date   DATE NOT NULL,
    end_date     DATE,               -- NULL = open-ended
    status       TEXT NOT NULL CHECK (status IN ('active', 'cancelled', 'expired'))
);

-- One row per vehicle signal. (vin, ts_utc) is unique so re-delivered events are ignored.
CREATE TABLE telemetry (
    id           BIGSERIAL PRIMARY KEY,
    vin          CHAR(17) NOT NULL,
    ts_utc       TIMESTAMPTZ NOT NULL,
    speed_kmph   NUMERIC(6,2) NOT NULL,
    odometer_km  NUMERIC(12,1) NOT NULL,
    lat          DOUBLE PRECISION NOT NULL,
    lon          DOUBLE PRECISION NOT NULL,
    heading      INT,
    soc_pct      NUMERIC(5,2),
    ignition     BOOLEAN NOT NULL DEFAULT true,
    source_oem   TEXT NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_telemetry_vin_ts UNIQUE (vin, ts_utc)   -- also serves as the (vin, ts_utc) index
);

CREATE TABLE alerts (
    id          BIGSERIAL PRIMARY KEY,
    vin         CHAR(17) NOT NULL,
    type        TEXT NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    details     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_alert UNIQUE (vin, type, started_at)
);

CREATE TABLE audit_log (
    id          BIGSERIAL PRIMARY KEY,
    table_name  TEXT NOT NULL,
    change      TEXT NOT NULL,
    ticket      TEXT,
    changed_by  TEXT NOT NULL DEFAULT current_user,
    at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
