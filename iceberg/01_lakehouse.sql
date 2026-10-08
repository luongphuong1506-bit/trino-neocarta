-- Dữ liệu mẫu Iceberg (chạy qua Trino, dùng bash):  docker exec -i trino trino < iceberg/01_lakehouse.sql
-- Schema sales: bảng tạo mới + bảng CTAS copy từ Postgres + view

CREATE SCHEMA IF NOT EXISTS iceberg.sales;

-- Bảng có comment cho bảng và cột, partition theo ngày
CREATE TABLE IF NOT EXISTS iceberg.sales.web_events (
    event_id    BIGINT       COMMENT 'ID sự kiện',
    customer_id INTEGER      COMMENT 'Khoá khách hàng (tham chiếu postgres.raw.customers)',
    event_type  VARCHAR      COMMENT 'Loại sự kiện: view / add_to_cart / purchase',
    page_url    VARCHAR      COMMENT 'URL trang',
    device      ROW(os VARCHAR, browser VARCHAR) COMMENT 'Thiết bị (kiểu ROW)',
    tags        ARRAY(VARCHAR) COMMENT 'Tag gắn cho sự kiện (kiểu ARRAY)',
    event_ts    TIMESTAMP(6) COMMENT 'Thời điểm phát sinh',
    event_date  DATE         COMMENT 'Ngày (cột partition)'
)
COMMENT 'Clickstream từ website (bảng Iceberg)'
WITH (partitioning = ARRAY['event_date'], format = 'PARQUET');

DELETE FROM iceberg.sales.web_events;  -- cho phép chạy lại script mà không nhân đôi dữ liệu
INSERT INTO iceberg.sales.web_events VALUES
 (1, 1, 'view',        '/products/10', ROW('Windows','Chrome'),  ARRAY['campaign_a'], TIMESTAMP '2026-09-01 08:00:00', DATE '2026-09-01'),
 (2, 1, 'add_to_cart', '/cart',        ROW('Windows','Chrome'),  ARRAY['campaign_a'], TIMESTAMP '2026-09-01 08:05:00', DATE '2026-09-01'),
 (3, 1, 'purchase',    '/checkout',    ROW('Windows','Chrome'),  ARRAY[],             TIMESTAMP '2026-09-01 08:10:00', DATE '2026-09-01'),
 (4, 2, 'view',        '/products/12', ROW('iOS','Safari'),      ARRAY['organic'],    TIMESTAMP '2026-09-02 19:00:00', DATE '2026-09-02'),
 (5, 3, 'view',        '/products/10', ROW('Android','Chrome'),  ARRAY['campaign_b'], TIMESTAMP '2026-09-15 10:00:00', DATE '2026-09-15'),
 (6, 3, 'purchase',    '/checkout',    ROW('Android','Chrome'),  ARRAY['campaign_b'], TIMESTAMP '2026-09-15 10:20:00', DATE '2026-09-15'),
 (7, 4, 'view',        '/products/13', ROW('macOS','Firefox'),   ARRAY[],             TIMESTAMP '2026-09-20 21:00:00', DATE '2026-09-20');

-- Bảng CTAS: copy từ catalog postgres sang Iceberg (cross-catalog)
CREATE TABLE IF NOT EXISTS iceberg.sales.dim_customers
COMMENT 'Dimension khách hàng, copy từ postgres.raw.customers'
AS SELECT customer_id, full_name, city FROM postgres.raw.customers;

COMMENT ON COLUMN iceberg.sales.dim_customers.city IS 'Thành phố';

CREATE TABLE IF NOT EXISTS iceberg.sales.fact_orders
WITH (partitioning = ARRAY['month(order_date)'])
-- order_amount là NUMERIC không có precision bên Postgres -> Trino đọc thành kiểu "number", Iceberg không hỗ trợ nên phải CAST
AS SELECT o.order_id, o.customer_id, o.order_date, o.status, CAST(o.order_amount AS DECIMAL(12,2)) AS order_amount
FROM postgres.staging.stg_orders o;

-- View Iceberg
CREATE OR REPLACE VIEW iceberg.sales.v_customer_funnel
COMMENT 'Funnel theo khách hàng'
AS SELECT c.customer_id, c.city,
          count_if(e.event_type = 'view')     AS views,
          count_if(e.event_type = 'purchase') AS purchases
   FROM iceberg.sales.dim_customers c
   LEFT JOIN iceberg.sales.web_events e ON e.customer_id = c.customer_id
   GROUP BY c.customer_id, c.city;
