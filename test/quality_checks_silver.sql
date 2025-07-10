-- ============================================================================
--  Silver Layer Data Quality Checks 
-- ============================================================================
-- Purpose:
--   - Validate uniqueness, nulls, string formatting, consistency
--   - Ensure clean and standardized input to Gold Layer
-- ============================================================================
--  Run this after loading Silver Layer data
-- ============================================================================

-- =============================================================================
--  Check 1: silver_crm_cust_info
-- =============================================================================

--  Check for NULLs or Duplicate Primary Keys (cst_id)
SELECT 
    cst_id,
    COUNT(*) AS record_count
FROM silver_crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

--  Check for Unwanted Spaces in Customer Key
SELECT 
    cst_key 
FROM silver_crm_cust_info
WHERE cst_key != TRIM(cst_key);

--  Check for inconsistent values in Marital Status
SELECT DISTINCT 
    cst_marital_status 
FROM silver_crm_cust_info;

-- =============================================================================
--  Check 2: silver_crm_prd_info
-- =============================================================================

--  Check for NULLs or Duplicate Primary Keys (prd_id)
SELECT 
    prd_id,
    COUNT(*) AS record_count
FROM silver_crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--  Check for Unwanted Spaces in Product Name
SELECT 
    prd_nm 
FROM silver_crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--  Check for NULL or Negative Product Costs
SELECT 
    prd_cost 
FROM silver_crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

--  Check for inconsistent values in Product Line
SELECT DISTINCT 
    prd_line 
FROM silver_crm_prd_info;

--  Check for Start Dates After End Dates
SELECT 
    * 
FROM silver_crm_prd_info
WHERE prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt;

-- =============================================================================
--  Check 3: silver_crm_sales_details
-- =============================================================================

--  Validate Dates: Must be non-zero, 8-digit format, within expected range
SELECT 
    sls_due_dt 
FROM silver_crm_sales_details
WHERE sls_due_dt IS NULL
   OR CHAR_LENGTH(sls_due_dt) != 8
   OR sls_due_dt NOT BETWEEN 19000101 AND 20500101;

--  Check Date Order: Order Date should be before Ship/Due Dates
SELECT 
    * 
FROM silver_crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

--  Check Consistency: sales = quantity * price, all positive and not null
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price
FROM silver_crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0;

-- =============================================================================
--  Check 4: silver_erp_cust_az12
-- =============================================================================

--  Check for Birthdates Outside Expected Range (1924 to Today)
SELECT DISTINCT 
    bdate 
FROM silver_erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > CURRENT_DATE();

--  Check for inconsistent values in Gender
SELECT DISTINCT 
    gen 
FROM silver_erp_cust_az12;

-- =============================================================================
--  Check 5: silver_erp_loc_a101
-- =============================================================================

--  Standardization of Country Names
SELECT DISTINCT 
    cntry 
FROM silver_erp_loc_a101
ORDER BY cntry;

-- =============================================================================
--  Check 6: silver_erp_px_cat_g1v2
-- =============================================================================

--  Check for Unwanted Spaces in Category/Subcategory/Maintenance
SELECT 
    * 
FROM silver_erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

--  Check for inconsistent Maintenance values
SELECT DISTINCT 
    maintenance 
FROM silver_erp_px_cat_g1v2;
