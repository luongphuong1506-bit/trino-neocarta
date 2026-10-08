-- Test schema evolution - THAY ĐỔI SCHEMA (v1 -> v2)
-- Chạy bằng bash:  docker exec -i trino trino < iceberg/03_schema_evolution_v2.sql

-- 1. Thêm cột
ALTER TABLE iceberg.evo.customer_profile ADD COLUMN email VARCHAR COMMENT 'Email (thêm ở v2)';
-- 2. Đổi tên cột
ALTER TABLE iceberg.evo.customer_profile RENAME COLUMN name TO full_name;
-- 3. Xoá cột
ALTER TABLE iceberg.evo.customer_profile DROP COLUMN legacy_flag;
-- 4. Nới kiểu dữ liệu (Iceberg cho phép integer -> bigint)
ALTER TABLE iceberg.evo.customer_profile ALTER COLUMN score SET DATA TYPE BIGINT;
-- 5. Đổi comment cột và comment bảng
COMMENT ON COLUMN iceberg.evo.customer_profile.score IS 'Điểm (v2, kiểu bigint)';
COMMENT ON TABLE iceberg.evo.customer_profile IS 'Hồ sơ khách hàng (v2)';
-- 6. Đổi tên bảng
ALTER TABLE iceberg.evo.loyalty RENAME TO iceberg.evo.loyalty_v2;
-- 7. Xoá bảng
DROP TABLE iceberg.evo.tmp_import;
