-- =======================================================================
-- Create and Setup Database: DataWarehouseAnalytics
-- =======================================================================

-- Drop database if it exists (use cautiously in production)
DROP DATABASE IF EXISTS DataWarehouseAnalytics;

-- Create the new database
CREATE DATABASE DataWarehouseAnalytics;

-- Use the new database
USE DataWarehouseAnalytics;

-- =======================================================================
-- Create Tables (no SCHEMA support like SQL Server, using flat naming)
-- =======================================================================

CREATE TABLE gold_dim_customers (
    customer_key INT,
    customer_id INT,
    customer_number VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    country VARCHAR(50),
    marital_status VARCHAR(50),
    gender VARCHAR(50),
    birthdate DATE,
    create_date DATE
);

CREATE TABLE gold_dim_products (
    product_key INT,
    product_id INT,
    product_number VARCHAR(50),
    product_name VARCHAR(50),
    category_id VARCHAR(50),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    maintenance VARCHAR(50),
    cost INT,
    product_line VARCHAR(50),
    start_date DATE
);

CREATE TABLE gold_fact_sales (
    order_number VARCHAR(50),
    product_key INT,
    customer_key INT,
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales_amount INT,
    quantity TINYINT,
    price INT
);

-- =======================================================================
-- Load CSV Data into Tables using LOAD DATA LOCAL INFILE
-- Assumes --local-infile=1 is enabled and file paths are accessible
-- =======================================================================

-- Truncate and load gold_dim_customers
TRUNCATE TABLE gold_dim_customers;

LOAD DATA LOCAL INFILE 'C:/sql/sql-data-analytics-project/datasets/csv-files/gold.dim_customers.csv'
INTO TABLE gold_dim_customers
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- Truncate and load gold_dim_products
TRUNCATE TABLE gold_dim_products;

LOAD DATA LOCAL INFILE 'C:/sql/sql-data-analytics-project/datasets/csv-files/gold.dim_products.csv'
INTO TABLE gold_dim_products
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- Truncate and load gold_fact_sales
TRUNCATE TABLE gold_fact_sales;

LOAD DATA LOCAL INFILE 'C:/sql/sql-data-analytics-project/datasets/csv-files/gold.fact_sales.csv'
INTO TABLE gold_fact_sales
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;
