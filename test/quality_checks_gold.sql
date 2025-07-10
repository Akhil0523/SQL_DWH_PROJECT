-- ============================================================================
-- Quality Checks for Gold Layer 
-- ============================================================================
-- Purpose:
--   - Validate uniqueness of surrogate keys
--   - Ensure fact table keys exist in dimensions
--   - Check for potential data quality issues before reporting
-- ============================================================================

-- ============================================================================
-- Check 1: Uniqueness of customer_key in gold_dim_customers
-- Expectation: No duplicate surrogate keys
-- ============================================================================
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold_dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ============================================================================
--  Check 2: Uniqueness of product_key in gold_dim_products
-- Expectation: No duplicate surrogate keys
-- ============================================================================
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold_dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ============================================================================
--  Check 3: Referential Integrity in gold_fact_sales
-- Expectation: All product_key and customer_key must exist in dimensions
--              Records with NULL joins indicate broken relationships
-- ============================================================================
SELECT 
    f.order_number,
    f.product_key,
    f.customer_key,
    p.product_key   AS matched_product_key,
    c.customer_key  AS matched_customer_key
FROM gold_fact_sales f
LEFT JOIN gold_dim_customers c 
    ON f.customer_key = c.customer_key
LEFT JOIN gold_dim_products p 
    ON f.product_key = p.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL;
