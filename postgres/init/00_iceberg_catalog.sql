-- Database riêng cho Iceberg JDBC catalog.
-- Trino không tự tạo bảng hệ thống của JDBC catalog nên tạo sẵn ở đây (schema V1, có iceberg_type để hỗ trợ view).
CREATE DATABASE iceberg_catalog OWNER demo;

\connect iceberg_catalog demo

CREATE TABLE iceberg_namespace_properties (
    catalog_name   VARCHAR(255) NOT NULL,
    namespace      VARCHAR(255) NOT NULL,
    property_key   VARCHAR(255),
    property_value VARCHAR(1000),
    PRIMARY KEY (catalog_name, namespace, property_key)
);

CREATE TABLE iceberg_tables (
    catalog_name               VARCHAR(255) NOT NULL,
    table_namespace            VARCHAR(255) NOT NULL,
    table_name                 VARCHAR(255) NOT NULL,
    metadata_location          VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    iceberg_type               VARCHAR(5),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);
