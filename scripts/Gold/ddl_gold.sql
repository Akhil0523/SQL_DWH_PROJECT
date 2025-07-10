-- ============================================
-- DDL Script: Create Gold Views 
-- Purpose:
--   - Create star schema views (fact + dimensions)
--   - Final clean, business-ready data model
-- ============================================

-- ============================================
-- Drop existing views if they exist
-- ============================================

DROP VIEW IF EXISTS gold_dim_customers;
DROP VIEW IF EXISTS gold_dim_products;
DROP VIEW IF EXISTS gold_fact_sales;

-- ============================================
-- Create gold_dim_customers View
-- ============================================
CREATE VIEW gold_dim_customers AS
SELECT
    -- Generate surrogate key using ROW_NUMBER
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,

    -- Direct mappings from source tables
    ci.cst_id               AS customer_id,
    ci.cst_key              AS customer_number,
    ci.cst_firstname        AS first_name,
    ci.cst_lastname         AS last_name,
    la.cntry                AS country,
    ci.cst_marital_status   AS marital_status,

    -- Prefer CRM gender if available, fallback to ERP
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END                     AS gender,

    ca.bdate                AS birthdate,
    ci.cst_create_date      AS create_date

FROM silver_crm_cust_info ci
LEFT JOIN silver_erp_cust_az12 ca 
    ON ci.cst_key = ca.cid
LEFT JOIN silver_erp_loc_a101 la 
    ON ci.cst_key = la.cid;

-- ============================================
-- Create gold_dim_products View
-- ============================================
CREATE VIEW gold_dim_products AS
SELECT
    -- Generate surrogate product key
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,

    -- Product details and category enrichment
    pn.prd_id           AS product_id,
    pn.prd_key          AS product_number,
    pn.prd_nm           AS product_name,
    pn.cat_id           AS category_id,
    pc.cat              AS category,
    pc.subcat           AS subcategory,
    pc.maintenance      AS maintenance,
    pn.prd_cost         AS cost,
    pn.prd_line         AS product_line,
    pn.prd_start_dt     AS start_date

FROM silver_crm_prd_info pn
LEFT JOIN silver_erp_px_cat_g1v2 pc 
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Only active products

-- ============================================
-- Create gold_fact_sales View
-- ============================================
CREATE VIEW gold_fact_sales AS
SELECT
    -- Sales transaction details
    sd.sls_ord_num     AS order_number,

    -- Link to dimension keys via product_number and customer_id
    pr.product_key     AS product_key,
    cu.customer_key    AS customer_key,

    -- Dates and metrics
    sd.sls_order_dt    AS order_date,
    sd.sls_ship_dt     AS shipping_date,
    sd.sls_due_dt      AS due_date,
    sd.sls_sales       AS sales_amount,
    sd.sls_quantity    AS quantity,
    sd.sls_price       AS price

FROM silver_crm_sales_details sd
LEFT JOIN gold_dim_products pr 
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold_dim_customers cu 
    ON sd.sls_cust_id = cu.customer_id;
