-- ===============================================
-- Загрузка Link таблиц
-- ===============================================

-- Очистка всех Links перед загрузкой
TRUNCATE TABLE student47.link_product_hierarchy CASCADE;
TRUNCATE TABLE student47.link_order_line CASCADE;

-- LINK_PRODUCT_HIERARCHY
INSERT INTO student47.link_product_hierarchy (hierarchy_hk, product_hk, category_hk, load_dts, record_source)
SELECT DISTINCT
    MD5(p.product_hk || '|' || c.category_hk) as hierarchy_hk,
    p.product_hk,
    c.category_hk,
    CURRENT_TIMESTAMP as load_dts,
    s.record_source
FROM student47.stg_orders s
JOIN student47.hub_product p ON p.sub_category = s.sub_category
JOIN student47.hub_category c ON c.category = s.category;

-- LINK_ORDER_LINE
INSERT INTO student47.link_order_line (
    order_line_hk, location_hk, product_hk, 
    ship_mode_hk, segment_hk, load_dts, record_source
)
SELECT DISTINCT
    MD5(l.location_hk || '|' || p.product_hk || '|' || sm.ship_mode_hk || '|' || sg.segment_hk) as order_line_hk,
    l.location_hk,
    p.product_hk,
    sm.ship_mode_hk,
    sg.segment_hk,
    CURRENT_TIMESTAMP as load_dts,
    s.record_source
FROM student47.stg_orders s
JOIN student47.hub_location l ON l.postal_code = s.postal_code AND l.country = s.country
JOIN student47.hub_product p ON p.sub_category = s.sub_category
JOIN student47.hub_ship_mode sm ON sm.ship_mode = s.ship_mode
JOIN student47.hub_segment sg ON sg.segment = s.segment;
