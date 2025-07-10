/*
===============================================================================
Performance Analysis (Year-over-Year, Month-over-Month)
===============================================================================
Purpose:
    - To measure the performance of products over time.
    - For benchmarking against average sales.
    - To track year-over-year trends.

SQL Functions Used:
    - LAG(): Access previous year's sales.
    - AVG() OVER(): Average sales for each product.
    - CASE: For conditional trend labeling.
===============================================================================
*/

WITH yearly_product_sales AS (
    SELECT
        YEAR(f.order_date) AS order_year,
        p.product_name,
        SUM(f.sales_amount) AS current_sales
    FROM gold_fact_sales f
    LEFT JOIN gold_dim_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY 
        YEAR(f.order_date),
        p.product_name
)

SELECT
    order_year,
    product_name,
    current_sales,
    
    -- Average sales for the product across years
    AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
    
    -- Difference from average
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,
    
    -- Above or below average label
    CASE 
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change,
    
    -- Previous year's sales
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS py_sales,
    
    -- Difference from previous year
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
    
    -- Growth trend
    CASE 
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
        ELSE 'No Change'
    END AS py_change

FROM yearly_product_sales
ORDER BY product_name, order_year;
