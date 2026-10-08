-- Dữ liệu mẫu nhiều tầng để test metadata + lineage
-- raw (bảng nguồn) -> staging (view) -> mart (view)

CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA mart;

-- ===== RAW =====
CREATE TABLE raw.customers (
    customer_id  INT PRIMARY KEY,
    full_name    VARCHAR(100) NOT NULL,
    email        VARCHAR(100),
    city         VARCHAR(50),
    created_at   TIMESTAMP DEFAULT now()
);
COMMENT ON TABLE raw.customers IS 'Khách hàng (nguồn CRM)';
COMMENT ON COLUMN raw.customers.email IS 'PII - email khách hàng';

CREATE TABLE raw.products (
    product_id   INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category     VARCHAR(50),
    price        NUMERIC(12,2)
);
COMMENT ON TABLE raw.products IS 'Danh mục sản phẩm';

CREATE TABLE raw.orders (
    order_id     INT PRIMARY KEY,
    customer_id  INT REFERENCES raw.customers(customer_id),
    order_date   DATE NOT NULL,
    status       VARCHAR(20)
);
COMMENT ON TABLE raw.orders IS 'Đơn hàng';

CREATE TABLE raw.order_items (
    order_id     INT REFERENCES raw.orders(order_id),
    product_id   INT REFERENCES raw.products(product_id),
    quantity     INT NOT NULL,
    unit_price   NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (order_id, product_id)
);
COMMENT ON TABLE raw.order_items IS 'Chi tiết đơn hàng';

INSERT INTO raw.customers (customer_id, full_name, email, city) VALUES
 (1, 'Nguyen Van A', 'a@example.com', 'Ha Noi'),
 (2, 'Tran Thi B',   'b@example.com', 'Ho Chi Minh'),
 (3, 'Le Van C',     'c@example.com', 'Da Nang'),
 (4, 'Pham Thi D',   'd@example.com', 'Ha Noi');

INSERT INTO raw.products VALUES
 (10, 'Laptop',   'Electronics', 1500.00),
 (11, 'Mouse',    'Electronics',   20.00),
 (12, 'Desk',     'Furniture',    300.00),
 (13, 'Chair',    'Furniture',    150.00);

INSERT INTO raw.orders VALUES
 (100, 1, '2026-09-01', 'completed'),
 (101, 2, '2026-09-02', 'completed'),
 (102, 1, '2026-09-10', 'cancelled'),
 (103, 3, '2026-09-15', 'completed'),
 (104, 4, '2026-09-20', 'pending');

INSERT INTO raw.order_items VALUES
 (100, 10, 1, 1500.00), (100, 11, 2, 20.00),
 (101, 12, 1,  300.00), (101, 13, 2, 150.00),
 (102, 11, 1,   20.00),
 (103, 10, 1, 1450.00), (103, 13, 1, 150.00),
 (104, 12, 2,  300.00);

-- ===== STAGING (view -> lineage từ raw) =====
CREATE VIEW staging.stg_orders AS
SELECT o.order_id,
       o.customer_id,
       o.order_date,
       o.status,
       SUM(oi.quantity * oi.unit_price) AS order_amount
FROM raw.orders o
JOIN raw.order_items oi ON oi.order_id = o.order_id
GROUP BY o.order_id, o.customer_id, o.order_date, o.status;

CREATE VIEW staging.stg_customers AS
SELECT customer_id, full_name, city
FROM raw.customers;

-- ===== MART (view -> lineage từ staging) =====
CREATE VIEW mart.customer_revenue AS
SELECT c.customer_id,
       c.full_name,
       c.city,
       COUNT(o.order_id)   AS total_orders,
       SUM(o.order_amount) AS total_revenue
FROM staging.stg_customers c
JOIN staging.stg_orders o ON o.customer_id = c.customer_id
WHERE o.status = 'completed'
GROUP BY c.customer_id, c.full_name, c.city;

CREATE VIEW mart.revenue_by_city AS
SELECT city, SUM(total_revenue) AS revenue
FROM mart.customer_revenue
GROUP BY city;
