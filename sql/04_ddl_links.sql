-- ===============================================
-- Link таблицы
-- ===============================================

CREATE TABLE IF NOT EXISTS student47.link_product_hierarchy (
    hierarchy_hk CHAR(32) PRIMARY KEY,
    product_hk CHAR(32) NOT NULL,
    category_hk CHAR(32) NOT NULL,
    load_dts TIMESTAMP DEFAULT now(),
    record_source TEXT DEFAULT 'SampleSuperstore.csv'
) DISTRIBUTED BY (hierarchy_hk);

CREATE TABLE IF NOT EXISTS student47.link_order_line (
    order_line_hk CHAR(32) PRIMARY KEY,
    location_hk CHAR(32) NOT NULL,
    product_hk CHAR(32) NOT NULL,
    ship_mode_hk CHAR(32) NOT NULL,
    segment_hk CHAR(32) NOT NULL,
    load_dts TIMESTAMP DEFAULT now(),
    record_source TEXT DEFAULT 'SampleSuperstore.csv'
) DISTRIBUTED BY (order_line_hk);
