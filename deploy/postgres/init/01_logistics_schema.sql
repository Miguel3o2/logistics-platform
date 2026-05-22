-- =============================================================================
-- deploy/postgres/init/01_logistics_schema.sql
-- Operational database for the Global Logistics Platform
--
-- This is the SOURCE of CDC via Debezium.
-- All tables use wal_level=logical (set in docker-compose.yml).
-- =============================================================================

-- ── Extensions ────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- ── Schemas ───────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS logistics;
CREATE SCHEMA IF NOT EXISTS audit;

-- ── Audit trigger ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION logistics.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

-- =============================================================================
-- VEHICLES
-- =============================================================================
CREATE TABLE logistics.vehicles (
    vehicle_id       VARCHAR(20)    PRIMARY KEY,
    plate_number     VARCHAR(20)    NOT NULL UNIQUE,
    make             VARCHAR(50)    NOT NULL,
    model            VARCHAR(50)    NOT NULL,
    year             INTEGER        NOT NULL CHECK (year BETWEEN 1990 AND 2030),
    capacity_kg      NUMERIC(10,2)  NOT NULL CHECK (capacity_kg > 0),
    fleet_id         VARCHAR(20)    NOT NULL,
    status           VARCHAR(20)    NOT NULL DEFAULT 'ACTIVE'
                       CHECK (status IN ('ACTIVE','MAINTENANCE','DECOMMISSIONED')),
    last_maintenance DATE,
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX vehicles_fleet_idx  ON logistics.vehicles(fleet_id);
CREATE INDEX vehicles_status_idx ON logistics.vehicles(status);

CREATE TRIGGER vehicles_updated_at
    BEFORE UPDATE ON logistics.vehicles
    FOR EACH ROW EXECUTE FUNCTION logistics.set_updated_at();

-- =============================================================================
-- DRIVERS
-- =============================================================================
CREATE TABLE logistics.drivers (
    driver_id    VARCHAR(20)   PRIMARY KEY,
    name         VARCHAR(100)  NOT NULL,
    license_no   VARCHAR(30)   NOT NULL UNIQUE,
    license_expiry DATE        NOT NULL,
    fleet_id     VARCHAR(20)   NOT NULL,
    status       VARCHAR(20)   NOT NULL DEFAULT 'ACTIVE'
                   CHECK (status IN ('ACTIVE','INACTIVE','SUSPENDED')),
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TRIGGER drivers_updated_at
    BEFORE UPDATE ON logistics.drivers
    FOR EACH ROW EXECUTE FUNCTION logistics.set_updated_at();

-- =============================================================================
-- SHIPMENTS
-- =============================================================================
CREATE TABLE logistics.shipments (
    shipment_id      UUID          PRIMARY KEY DEFAULT uuid_generate_v4(),
    vehicle_id       VARCHAR(20)   REFERENCES logistics.vehicles(vehicle_id),
    driver_id        VARCHAR(20)   REFERENCES logistics.drivers(driver_id),
    origin           VARCHAR(100)  NOT NULL,
    destination      VARCHAR(100)  NOT NULL,
    status           VARCHAR(20)   NOT NULL DEFAULT 'PENDING'
                       CHECK (status IN ('PENDING','IN_TRANSIT','DELIVERED','DELAYED','RETURNED')),
    weight_kg        NUMERIC(10,2) NOT NULL CHECK (weight_kg > 0),
    scheduled_eta    TIMESTAMPTZ   NOT NULL,
    actual_delivery  TIMESTAMPTZ,
    notes            TEXT,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX shipments_vehicle_idx  ON logistics.shipments(vehicle_id);
CREATE INDEX shipments_status_idx   ON logistics.shipments(status);
CREATE INDEX shipments_eta_idx      ON logistics.shipments(scheduled_eta);

CREATE TRIGGER shipments_updated_at
    BEFORE UPDATE ON logistics.shipments
    FOR EACH ROW EXECUTE FUNCTION logistics.set_updated_at();

-- =============================================================================
-- MAINTENANCE RECORDS (ground truth for engine failure model)
-- =============================================================================
CREATE TABLE logistics.maintenance_records (
    id           UUID          PRIMARY KEY DEFAULT uuid_generate_v4(),
    vehicle_id   VARCHAR(20)   REFERENCES logistics.vehicles(vehicle_id),
    fault_code   VARCHAR(10),
    description  TEXT          NOT NULL,
    severity     VARCHAR(20)   NOT NULL CHECK (severity IN ('LOW','MEDIUM','HIGH','CRITICAL')),
    resolved     BOOLEAN       NOT NULL DEFAULT FALSE,
    maintenance_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX maintenance_vehicle_idx ON logistics.maintenance_records(vehicle_id);

CREATE TRIGGER maintenance_updated_at
    BEFORE UPDATE ON logistics.maintenance_records
    FOR EACH ROW EXECUTE FUNCTION logistics.set_updated_at();

-- =============================================================================
-- Seed data
-- =============================================================================

INSERT INTO logistics.vehicles (vehicle_id, plate_number, make, model, year,
    capacity_kg, fleet_id, status) VALUES
('TRK-0001','ISB-7842','Hino',  '500 Series', 2021, 15000, 'FK-NORTH',   'ACTIVE'),
('TRK-0002','ISB-6531','Isuzu', 'FVR34',      2020, 12000, 'FK-NORTH',   'ACTIVE'),
('TRK-0003','LHR-2219','Hino',  '300 Series', 2022, 8000,  'FK-SOUTH',   'ACTIVE'),
('TRK-0004','LHR-4402','FAW',   'J6',         2019, 20000, 'FK-SOUTH',   'ACTIVE'),
('TRK-0005','KHI-8871','Hino',  '700 Series', 2023, 25000, 'FK-EXPRESS', 'ACTIVE'),
('TRK-0006','KHI-3310','MAN',   'TGX',        2022, 22000, 'FK-CARGO',   'MAINTENANCE'),
('TRK-0007','PES-1102','Isuzu', 'NPR',        2021, 5000,  'FK-NORTH',   'ACTIVE'),
('TRK-0008','QTA-5544','Hino',  '500 Series', 2020, 15000, 'FK-SOUTH',   'ACTIVE');

INSERT INTO logistics.drivers (driver_id, name, license_no, license_expiry, fleet_id) VALUES
('DRV-1001', 'Ahmed Khan',    'PK-ISB-12345', '2027-06-30', 'FK-NORTH'),
('DRV-1002', 'Muhammad Ali',  'PK-LHR-67890', '2026-12-31', 'FK-SOUTH'),
('DRV-1003', 'Hassan Raza',   'PK-KHI-11111', '2028-03-15', 'FK-EXPRESS'),
('DRV-1004', 'Usman Tariq',   'PK-ISB-22222', '2027-09-20', 'FK-NORTH'),
('DRV-1005', 'Bilal Qureshi', 'PK-PES-33333', '2026-08-10', 'FK-CARGO');

INSERT INTO logistics.shipments
    (vehicle_id, driver_id, origin, destination, status,
     weight_kg, scheduled_eta) VALUES
('TRK-0001','DRV-1001','Islamabad','Lahore',   'IN_TRANSIT', 8500,  NOW() + INTERVAL '4 hours'),
('TRK-0002','DRV-1004','Islamabad','Peshawar', 'IN_TRANSIT', 6200,  NOW() + INTERVAL '2 hours'),
('TRK-0003','DRV-1002','Lahore',   'Karachi',  'IN_TRANSIT', 11000, NOW() + INTERVAL '18 hours'),
('TRK-0005','DRV-1003','Karachi',  'Quetta',   'DELAYED',    19000, NOW() - INTERVAL '1 hour'),
('TRK-0007','DRV-1004','Peshawar', 'Islamabad','PENDING',    3200,  NOW() + INTERVAL '6 hours');

-- Add a completed delivery for training data
INSERT INTO logistics.shipments
    (vehicle_id, driver_id, origin, destination, status,
     weight_kg, scheduled_eta, actual_delivery) VALUES
('TRK-0001','DRV-1001','Islamabad','Lahore','DELIVERED', 9000,
    NOW() - INTERVAL '2 days' + INTERVAL '5 hours',
    NOW() - INTERVAL '2 days' + INTERVAL '4 hours 45 minutes');

-- =============================================================================
-- Debezium publication (CDC slot for Debezium connector)
-- =============================================================================
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');

CREATE PUBLICATION logistics_cdc FOR TABLE
    logistics.vehicles,
    logistics.drivers,
    logistics.shipments,
    logistics.maintenance_records;
