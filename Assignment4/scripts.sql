-- ============================================================
--   SHOPVERSE ETL PROJECT – DATABASE SCHEMA
--   Author: Nikhitha
-- ============================================================


-- 1. CREATE DATABASE

-- CREATE DATABASE dwh_shopverse;

-- Connect to DB:
-- \c dwh_shopverse;


-- 2. DROP TABLES (for resetting if needed)

DROP TABLE IF EXISTS fact_orders CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;

DROP TABLE IF EXISTS stg_orders CASCADE;
DROP TABLE IF EXISTS stg_products CASCADE;
DROP TABLE IF EXISTS stg_customers CASCADE;

-- 3. STAGING TABLES


-- 3.1 STAGING CUSTOMERS
CREATE TABLE stg_customers (
    customer_id      INT,
    first_name       VARCHAR(50),
    last_name        VARCHAR(50),
    email            VARCHAR(100),
    signup_date      DATE,
    country          VARCHAR(50)
);

-- 3.2 STAGING PRODUCTS
CREATE TABLE stg_products (
    product_id       INT,
    product_name     VARCHAR(100),
    category         VARCHAR(50),
    unit_price       NUMERIC(10,2)
);

-- 3.3 STAGING ORDERS
CREATE TABLE stg_orders (
    order_id         INT,
    order_timestamp  TIMESTAMP,
    customer_id      INT,
    product_id       INT,
    quantity         INT,
    total_amount     NUMERIC(10,2),
    currency         VARCHAR(10),
    status           VARCHAR(20)
);

-- 4. WAREHOUSE TABLES


-- 4.1 DIM CUSTOMERS
CREATE TABLE dim_customers (
    customer_id      INT PRIMARY KEY,
    first_name       VARCHAR(50),
    last_name        VARCHAR(50),
    email            VARCHAR(100),
    signup_date      DATE,
    country          VARCHAR(50)
);

-- 4.2 DIM PRODUCTS
CREATE TABLE dim_products (
    product_id       INT PRIMARY KEY,
    product_name     VARCHAR(100),
    category         VARCHAR(50),
    unit_price       NUMERIC(10,2)
);

-- 4.3 FACT ORDERS
CREATE TABLE fact_orders (
    order_id                INT PRIMARY KEY,
    order_timestamp         TIMESTAMP,
    customer_id             INT,
    product_id              INT,
    quantity                INT,
    total_amount            NUMERIC(10,2),
    currency_mismatch_flag  BOOLEAN,

    CONSTRAINT fk_customer FOREIGN KEY (customer_id)
        REFERENCES dim_customers(customer_id),

    CONSTRAINT fk_product FOREIGN KEY (product_id)
        REFERENCES dim_products(product_id)
);


-- 5. OPTIONAL: PERFORMANCE INDEXES


CREATE INDEX idx_fact_orders_customer ON fact_orders(customer_id);
CREATE INDEX idx_fact_orders_product  ON fact_orders(product_id);
CREATE INDEX idx_fact_orders_date     ON fact_orders(order_timestamp);


-- 6. TRUNCATE COMMANDS (for testing)


-- TRUNCATE TABLE stg_customers;
-- TRUNCATE TABLE stg_products;
-- TRUNCATE TABLE stg_orders;

-- TRUNCATE TABLE dim_customers;
-- TRUNCATE TABLE dim_products;
-- TRUNCATE TABLE fact_orders;
