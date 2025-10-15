-- ===============================================
-- Staging таблица
-- ===============================================

DROP TABLE IF EXISTS student47.stg_orders CASCADE;

CREATE TABLE student47.stg_orders (
    ship_mode TEXT,
    segment TEXT,
    country TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    region TEXT,
    category TEXT,
    sub_category TEXT,
    sales NUMERIC(12,2),
    quantity SMALLINT,
    discount NUMERIC(5,2),
    profit NUMERIC(12,2),
    load_dts TIMESTAMP DEFAULT now(),
    record_source TEXT DEFAULT 'SampleSuperstore.csv'
) DISTRIBUTED RANDOMLY;
