-- ===============================================
-- Загрузка Satellite таблиц
-- ===============================================

-- Очищаем сателлиты перед загрузкой
TRUNCATE TABLE student47.sat_location_details;
TRUNCATE TABLE student47.sat_product_category;
TRUNCATE TABLE student47.sat_order_line_metrics;

-- SAT_LOCATION_DETAILS
WITH unique_locations AS (
    SELECT 
        l.location_hk,
        s.city,
        s.state,
        s.region,
        s.record_source
    FROM student47.stg_orders s
    JOIN student47.hub_location l ON l.postal_code = s.postal_code AND l.country = s.country
    GROUP BY l.location_hk, s.city, s.state, s.region, s.record_source
)
INSERT INTO student47.sat_location_details (
    location_hk, hashdiff, city, state, region, load_dts, record_source
)
SELECT 
    ul.location_hk,
    MD5(ul.city || '|' || ul.state || '|' || ul.region) as hashdiff,
    ul.city,
    ul.state,
    ul.region,
    CURRENT_TIMESTAMP as load_dts,
    ul.record_source
FROM unique_locations ul;

-- SAT_PRODUCT_CATEGORY
WITH unique_products AS (
    SELECT 
        p.product_hk,
        c.category_hk,
        s.record_source
    FROM student47.stg_orders s
    JOIN student47.hub_product p ON p.sub_category = s.sub_category
    JOIN student47.hub_category c ON c.category = s.category
    GROUP BY p.product_hk, c.category_hk, s.record_source
)
INSERT INTO student47.sat_product_category (
    product_hk, hashdiff, category_hk, load_dts, record_source
)
SELECT 
    up.product_hk,
    MD5(up.category_hk) as hashdiff,
    up.category_hk,
    CURRENT_TIMESTAMP as load_dts,
    up.record_source
FROM unique_products up;

-- SAT_ORDER_LINE_METRICS
WITH unique_metrics AS (
    SELECT 
        lnk.order_line_hk,
        s.sales,
        s.quantity,
        s.discount,
        s.profit,
        s.record_source
    FROM student47.stg_orders s
    JOIN student47.hub_location loc ON loc.postal_code = s.postal_code AND loc.country = s.country
    JOIN student47.hub_product p ON p.sub_category = s.sub_category
    JOIN student47.hub_ship_mode sm ON sm.ship_mode = s.ship_mode
    JOIN student47.hub_segment sg ON sg.segment = s.segment
    JOIN student47.link_order_line lnk ON lnk.location_hk = loc.location_hk 
        AND lnk.product_hk = p.product_hk
        AND lnk.ship_mode_hk = sm.ship_mode_hk
        AND lnk.segment_hk = sg.segment_hk
    GROUP BY lnk.order_line_hk, s.sales, s.quantity, s.discount, s.profit, s.record_source
)
INSERT INTO student47.sat_order_line_metrics (
    order_line_hk, hashdiff, sales, quantity, discount, profit, load_dts, record_source
)
SELECT 
    um.order_line_hk,
    MD5(um.sales::TEXT || '|' || um.quantity::TEXT || '|' || um.discount::TEXT || '|' || um.profit::TEXT) as hashdiff,
    um.sales,
    um.quantity,
    um.discount,
    um.profit,
    CURRENT_TIMESTAMP as load_dts,
    um.record_source
FROM unique_metrics um;
