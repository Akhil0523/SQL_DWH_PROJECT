/*
===============================================================================
Change Over Time Analysis
===============================================================================
Purpose:
    - To track trends, growth, and changes in key metrics over time.
    - For time-series analysis and identifying seasonality.
    - To measure growth or decline over specific periods.

SQL Functions Used:
    - Date Functions: YEAR(), MONTH(), DATE_FORMAT()
    - Aggregate Functions: SUM(), COUNT()
===============================================================================
*/

-- Analyze sales performance over time (by year and month separately)
SELECT
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold_fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);

-- Simulating DATETRUNC(month, order_date) using DATE_FORMAT() as YYYY-MM
SELECT
    DATE_FORMAT(order_date, '%Y-%m') AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold_fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_FORMAT(order_date, '%Y-%m')
ORDER BY DATE_FORMAT(order_date, '%Y-%m');

-- Simulating FORMAT(order_date, 'yyyy-MMM') using DATE_FORMAT() as YYYY-MMM
SELECT
    DATE_FORMAT(order_date, '%Y-%b') AS order_month_text,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold_fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_FORMAT(order_date, '%Y-%b')
ORDER BY STR_TO_DATE(DATE_FORMAT(order_date, '%Y-%b'), '%Y-%b');
