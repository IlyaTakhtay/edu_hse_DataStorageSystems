-- ===============================================
-- Hub таблицы
-- ===============================================

CREATE TABLE IF NOT EXISTS student47.hub_location (
    location_hk CHAR(32) PRIMARY KEY,
    country TEXT,
    postal_code TEXT,
    load_dts TIMESTAMP DEFAULT now(),
    record_source TEXT DEFAULT 'SampleSuperstore.csv'
) DISTRIBUTED BY (location_hk);

CREATE TABLE IF NOT EXISTS student47.hub_product (
    product_hk CHAR(32) PRIMARY KEY,
    sub_category TEXT NOT NULL,
    load_dts TIMESTAMP DEFAULT now(),
    record_source TEXT DEFAULT 'SampleSuperstore.csv'
) DISTRIBUTED BY (product_hk);

CREATE TABLE IF NOT EXISTS student47.hub_category (
    category_hk CHAR(32) PRIMARY KEY,
    category TEXT NOT NULL,
    load_dts TIMESTAMP DEFAULT now(),
    record_source TEXT DEFAULT 'SampleSuperstore.csv'
) DISTRIBUTED BY (category_hk);

CREATE TABLE IF NOT EXISTS student47.hub_ship_mode (
    ship_mode_hk CHAR(32) PRIMARY KEY,
    ship_mode TEXT NOT NULL,
    load_dts TIMESTAMP DEFAULT now(),
    record_source TEXT DEFAULT 'SampleSuperstore.csv'
) DISTRIBUTED REPLICATED;

CREATE TABLE IF NOT EXISTS student47.hub_segment (
    segment_hk CHAR(32) PRIMARY KEY,
    segment TEXT NOT NULL,
    load_dts TIMESTAMP DEFAULT now(),
    record_source TEXT DEFAULT 'SampleSuperstore.csv'
) DISTRIBUTED REPLICATED;
