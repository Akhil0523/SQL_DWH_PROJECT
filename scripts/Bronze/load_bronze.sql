/*
Script Purpose:
    This script performs the initial Bronze Layer loading in the Data Warehouse.
    It ingests raw data from ERP and CRM CSV files into the corresponding
    Bronze schema tables using `LOAD DATA LOCAL INFILE`. It includes:

    - Truncation of bronze tables to maintain idempotency
    - Timed execution for performance tracking
    - Message-based logs for monitoring progress

Data Sources:
    - CRM System: Customer Info, Product Info, Sales Details
    - ERP System: Location, Customer, Product Category

Pre-requisites:
    - Enable LOCAL INFILE in MySQL:
        SET GLOBAL local_infile = 1;

    - File paths must be accessible by the MySQL client with proper privileges

Parameters:
    None. This script does not take any input parameters.

Returns:
    Runtime logs and duration of each individual load operation.

Usage Instructions:
    1. Open MySQL Workbench or CLI
    2. Run:
        SET GLOBAL local_infile = 1;
    3. Then execute this script:
        SOURCE path_to_script/load_bronze_layer.sql;

===============================================================================
*/

SET GLOBAL local_infile = 1;
USE datawarehouse;
-- Set start time of full batch
SET @batch_start_time = NOW();
SELECT '================================================' AS msg;
SELECT 'Loading Bronze Layer' AS msg;

-- CRM: Customer Info
SET @start_time = NOW();
TRUNCATE TABLE bronze_crm_cust_info;
LOAD DATA LOCAL INFILE 'C:/Users/DELL/OneDrive/Desktop/datawarehoue_sql_project/source_crm/cust_info.csv'
INTO TABLE bronze_crm_cust_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
SET @end_time = NOW();
SELECT CONCAT('crm_cust_info Load Duration: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS msg;

-- CRM: Product Info
SET @start_time = NOW();
TRUNCATE TABLE bronze_crm_prd_info;
LOAD DATA LOCAL INFILE 'C:/Users/DELL/OneDrive/Desktop/datawarehoue_sql_project/source_crm/prd_info.csv'
INTO TABLE bronze_crm_prd_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
SET @end_time = NOW();
SELECT CONCAT('crm_prd_info Load Duration: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS msg;

-- CRM: Sales Details
SET @start_time = NOW();
TRUNCATE TABLE bronze_crm_sales_details;
LOAD DATA LOCAL INFILE 'C:/Users/DELL/OneDrive/Desktop/datawarehoue_sql_project/source_crm/sales_details.csv'
INTO TABLE bronze_crm_sales_details
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
SET @end_time = NOW();
SELECT CONCAT('crm_sales_details Load Duration: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS msg;

-- ERP: Location
SET @start_time = NOW();
TRUNCATE TABLE bronze_erp_loc_a101;
LOAD DATA LOCAL INFILE 'C:/Users/DELL/OneDrive/Desktop/datawarehoue_sql_project/source_erp/LOC_A101.csv'
INTO TABLE bronze_erp_loc_a101
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
SET @end_time = NOW();
SELECT CONCAT('erp_loc_a101 Load Duration: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS msg;

-- ERP: Customer
SET @start_time = NOW();
TRUNCATE TABLE bronze_erp_cust_az12;
LOAD DATA LOCAL INFILE 'C:/Users/DELL/OneDrive/Desktop/datawarehoue_sql_project/source_erp/CUST_AZ12.csv'
INTO TABLE bronze_erp_cust_az12
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
SET @end_time = NOW();
SELECT CONCAT('erp
_cust_az12 Load Duration: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS msg;

-- ERP: Product Category
SET @start_time = NOW();
TRUNCATE TABLE bronze_erp_px_cat_g1v2;
LOAD DATA LOCAL INFILE 'C:/Users/DELL/OneDrive/Desktop/datawarehoue_sql_project/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze_erp_px_cat_g1v2
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;
SET @end_time = NOW();
SELECT CONCAT('erp_px_cat_g1v2 Load Duration: ', TIMESTAMPDIFF(SECOND, @start_time, @end_time), ' seconds') AS msg;

-- End of batch
SET @batch_end_time = NOW();
SELECT '==========================================' AS msg;
SELECT 'Loading Bronze Layer Completed' AS msg;
SELECT CONCAT('Total Load Duration: ', TIMESTAMPDIFF(SECOND, @batch_start_time, @batch_end_time), ' seconds') AS msg;
SELECT '==========================================' AS msg;
