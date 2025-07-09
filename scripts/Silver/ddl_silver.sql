/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver) [MySQL Version]
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
    Actions Performed:
        - Truncates Silver tables.
        - Inserts transformed and cleansed data from Bronze into Silver tables.
Parameters:
    None.
Usage Example:
    CALL silver_load_silver();
===============================================================================
*/

DELIMITER $$

CREATE PROCEDURE silver_load_silver()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;
    DECLARE exit handler for SQLEXCEPTION
    BEGIN
        -- Error Handling
        SELECT '==========================================' AS '';
        SELECT 'ERROR OCCURRED DURING LOADING SILVER LAYER' AS '';
        -- MySQL doesn't have ERROR_MESSAGE(), ERROR_NUMBER(), ERROR_STATE()
        SELECT 'Check the server error logs for details.' AS '';
        SELECT '==========================================' AS '';
        ROLLBACK;
    END;

    SET batch_start_time = NOW();
    SELECT '================================================' AS '';
    SELECT 'Loading Silver Layer' AS '';
    SELECT '================================================' AS '';

    SELECT '------------------------------------------------' AS '';
    SELECT 'Loading CRM Tables' AS '';
    SELECT '------------------------------------------------' AS '';

    -- ====================== CRM CUSTOMER INFO ======================
    SET start_time = NOW();
    SELECT '>> Truncating Table: silver.crm_cust_info' AS '';
    TRUNCATE TABLE silver.crm_cust_info;

    SELECT '>> Inserting Data Into: silver.crm_cust_info' AS '';
    INSERT INTO silver.crm_cust_info (
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_marital_status, 
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname, -- Remove leading/trailing spaces
        TRIM(cst_lastname) AS cst_lastname,   -- Remove leading/trailing spaces
        -- Normalize marital status (S/M -> Single/Married, else n/a)
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        -- Normalize gender (F/M -> Female/Male, else n/a)
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        -- Select only most recent record per customer (ROW_NUMBER -> MySQL window function)
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS '';
    SELECT '>> -------------' AS '';

    -- ====================== CRM PRODUCT INFO ======================
    SET start_time = NOW();
    SELECT '>> Truncating Table: silver.crm_prd_info' AS '';
    TRUNCATE TABLE silver.crm_prd_info;

    SELECT '>> Inserting Data Into: silver.crm_prd_info' AS '';
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID and replace '-' with '_'
        SUBSTRING(prd_key, 7) AS prd_key, -- Extract product key (MySQL SUBSTRING default: start at pos 7 to end)
        prd_nm,
        IFNULL(prd_cost, 0) AS prd_cost, -- Replace NULL product cost with 0
        -- Map product line codes to descriptive values
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        DATE(prd_start_dt) AS prd_start_dt,
        -- Calculate end date as one day before the next start date
        DATE(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY
        ) AS prd_end_dt
    FROM bronze.crm_prd_info;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS '';
    SELECT '>> -------------' AS '';

    -- ====================== CRM SALES DETAILS ======================
    SET start_time = NOW();
    SELECT '>> Truncating Table: silver.crm_sales_details' AS '';
    TRUNCATE TABLE silver.crm_sales_details;

    SELECT '>> Inserting Data Into: silver.crm_sales_details' AS '';
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        -- Convert order date: If value is 0 or not 8 chars, set NULL; else cast to DATE
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
            ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
        END AS sls_order_dt,
        -- Convert ship date
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
            ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
        END AS sls_ship_dt,
        -- Convert due date
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
            ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
        END AS sls_due_dt,
        -- Recalculate sales if original value is missing or incorrect
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        -- Derive price if original value is invalid (NULL or <=0)
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS '';
    SELECT '>> -------------' AS '';

    -- ====================== ERP CUSTOMER AZ12 ======================
    SET start_time = NOW();
    SELECT '>> Truncating Table: silver.erp_cust_az12' AS '';
    TRUNCATE TABLE silver.erp_cust_az12;

    SELECT '>> Inserting Data Into: silver.erp_cust_az12' AS '';
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        -- Remove 'NAS' prefix if present (using SUBSTRING and LIKE)
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
            ELSE cid
        END AS cid, 
        -- Set future birthdates to NULL
        CASE
            WHEN bdate > NOW() THEN NULL
            ELSE bdate
        END AS bdate,
        -- Normalize gender values and handle unknown cases
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS '';
    SELECT '>> -------------' AS '';

    -- ====================== ERP TABLES ======================
    SELECT '------------------------------------------------' AS '';
    SELECT 'Loading ERP Tables' AS '';
    SELECT '------------------------------------------------' AS '';

    -- ====================== ERP LOC A101 ======================
    SET start_time = NOW();
    SELECT '>> Truncating Table: silver.erp_loc_a101' AS '';
    TRUNCATE TABLE silver.erp_loc_a101;

    SELECT '>> Inserting Data Into: silver.erp_loc_a101' AS '';
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid, -- Remove '-' from cid
        -- Normalize country codes
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS '';
    SELECT '>> -------------' AS '';

    -- ====================== ERP PX CAT G1V2 ======================
    SET start_time = NOW();
    SELECT '>> Truncating Table: silver.erp_px_cat_g1v2' AS '';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    SELECT '>> Inserting Data Into: silver.erp_px_cat_g1v2' AS '';
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS '';
    SELECT '>> -------------' AS '';

    SET batch_end_time = NOW();
    SELECT '==========================================' AS '';
    SELECT 'Loading Silver Layer is Completed' AS '';
    SELECT CONCAT('   - Total Load Duration: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS '';
    SELECT '==========================================' AS '';
    COMMIT;

END $$
DELIMITER ;
