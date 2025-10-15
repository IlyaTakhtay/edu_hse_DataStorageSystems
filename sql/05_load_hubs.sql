-- ===============================================
-- Загрузка Hub таблиц из staging
-- ===============================================

-- Очистка всех Hubs перед загрузкой
TRUNCATE TABLE student47.hub_location CASCADE;
TRUNCATE TABLE student47.hub_product CASCADE;
TRUNCATE TABLE student47.hub_category CASCADE;
TRUNCATE TABLE student47.hub_ship_mode CASCADE;
TRUNCATE TABLE student47.hub_segment CASCADE;

-- HUB_LOCATION
INSERT INTO student47.hub_location (location_hk, country, postal_code, load_dts, record_source)
SELECT DISTINCT
    MD5(country || '|' || postal_code) as location_hk,
    country,
    postal_code,
    CURRENT_TIMESTAMP as load_dts,
    record_source
FROM student47.stg_orders;

-- HUB_PRODUCT
INSERT INTO student47.hub_product (product_hk, sub_category, load_dts, record_source)
SELECT DISTINCT
    MD5(sub_category) as product_hk,
    sub_category,
    CURRENT_TIMESTAMP as load_dts,
    record_source
FROM student47.stg_orders;

-- HUB_CATEGORY
INSERT INTO student47.hub_category (category_hk, category, load_dts, record_source)
SELECT DISTINCT
    MD5(category) as category_hk,
    category,
    CURRENT_TIMESTAMP as load_dts,
    record_source
FROM student47.stg_orders;

-- HUB_SHIP_MODE
INSERT INTO student47.hub_segment (segment_hk, segment, load_dts, record_source)
SELECT DISTINCT
    MD5(segment) as segment_hk,
    segment,
    CURRENT_TIMESTAMP as load_dts,
    record_source
FROM student47.stg_orders;

-- HUB_SEGMENT
INSERT INTO student47.hub_ship_mode (ship_mode_hk, ship_mode, load_dts, record_source)
SELECT DISTINCT
    MD5(ship_mode) as ship_mode_hk,
    ship_mode,
    CURRENT_TIMESTAMP as load_dts,
    record_source
FROM student47.stg_orders;
