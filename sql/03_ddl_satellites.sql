-- ===============================================
-- Satellite таблицы
-- ===============================================

CREATE TABLE IF NOT EXISTS student47.sat_location_details (
    location_hk CHAR(32) NOT NULL,
    hashdiff CHAR(32) NOT NULL,
    city TEXT,
    state TEXT,
    region TEXT,
    load_dts TIMESTAMP DEFAULT now(),
    record_source TEXT DEFAULT 'SampleSuperstore.csv',
    PRIMARY KEY (location_hk, hashdiff)  -- ← ДОБАВИТЬ!
) DISTRIBUTED BY (location_hk);

CREATE TABLE IF NOT EXISTS student47.sat_product_category (
    product_hk CHAR(32) NOT NULL,
    hashdiff CHAR(32) NOT NULL,
    category_hk CHAR(32) NOT NULL,
    load_dts TIMESTAMP DEFAULT now(),
    record_source TEXT DEFAULT 'SampleSuperstore.csv',
    PRIMARY KEY (product_hk, hashdiff)
) DISTRIBUTED BY (product_hk);

CREATE TABLE IF NOT EXISTS student47.sat_order_line_metrics (
    order_line_hk CHAR(32) NOT NULL,
    hashdiff CHAR(32) NOT NULL,
    sales NUMERIC(12,2),
    quantity SMALLINT,
    discount NUMERIC(5,2),
    profit NUMERIC(12,2),
    load_dts TIMESTAMP DEFAULT now(),
    record_source TEXT DEFAULT 'SampleSuperstore.csv',
    PRIMARY KEY (order_line_hk, hashdiff)
) DISTRIBUTED BY (order_line_hk);
