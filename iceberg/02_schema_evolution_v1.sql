-- Test schema evolution - TRẠNG THÁI BAN ĐẦU (v1)
-- Chạy bằng bash:  docker exec -i trino trino < iceberg/02_schema_evolution_v1.sql
CREATE SCHEMA IF NOT EXISTS iceberg.evo;

DROP TABLE IF EXISTS iceberg.evo.customer_profile;
DROP TABLE IF EXISTS iceberg.evo.loyalty;
DROP TABLE IF EXISTS iceberg.evo.loyalty_v2;
DROP TABLE IF EXISTS iceberg.evo.tmp_import;

CREATE TABLE iceberg.evo.customer_profile (
    id          INTEGER COMMENT 'ID khách hàng',
    name        VARCHAR COMMENT 'Tên khách hàng',
    score       INTEGER COMMENT 'Điểm (v1)',
    legacy_flag BOOLEAN COMMENT 'Cờ cũ, sẽ bị xoá'
) COMMENT 'Hồ sơ khách hàng (v1)';
INSERT INTO iceberg.evo.customer_profile VALUES (1, 'A', 10, true), (2, 'B', 20, false);

CREATE TABLE iceberg.evo.loyalty (customer_id INTEGER, tier VARCHAR) COMMENT 'Hạng thành viên (sẽ bị đổi tên)';
INSERT INTO iceberg.evo.loyalty VALUES (1, 'gold');

CREATE TABLE iceberg.evo.tmp_import (x INTEGER) COMMENT 'Bảng tạm (sẽ bị xoá)';
INSERT INTO iceberg.evo.tmp_import VALUES (1);
