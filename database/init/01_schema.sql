-- ============================================================
-- Sales Data Platform Schema
-- Production-Ready Relational Model
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- CUSTOMERS TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS customers (
    id BIGSERIAL PRIMARY KEY,
    customer_code VARCHAR(50) UNIQUE NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    country VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS idx_customers_country
ON customers(country);

-- ============================================================
-- PRODUCTS TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS products (
    id BIGSERIAL PRIMARY KEY,
    product_code VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS idx_products_category
ON products(category);

-- ============================================================
-- ORDERS TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    order_code VARCHAR(50) UNIQUE NOT NULL,
    customer_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    total_amount NUMERIC(14,2) NOT NULL CHECK (total_amount >= 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL,
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_orders_order_date
ON orders(order_date);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
ON orders(customer_id);

-- ============================================================
-- ORDER ITEMS TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS order_items (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    line_total NUMERIC(14,2) NOT NULL CHECK (line_total >= 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL,
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id)
        REFERENCES orders(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CONSTRAINT fk_order_items_product
        FOREIGN KEY (product_id)
        REFERENCES products(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id
ON order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_product_id
ON order_items(product_id);

-- ============================================================
-- FACT SALES TABLE (ANALYTICS-OPTIMIZED)
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_sales (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    customer_id BIGINT NOT NULL,
    customer_country VARCHAR(100),
    product_id BIGINT NOT NULL,
    product_category VARCHAR(100),
    quantity INTEGER NOT NULL,
    revenue NUMERIC(14,2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fact_sales_order
        FOREIGN KEY (order_id)
        REFERENCES orders(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_order_date
ON fact_sales(order_date);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_id
ON fact_sales(customer_id);

CREATE INDEX IF NOT EXISTS idx_fact_sales_product_id
ON fact_sales(product_id);