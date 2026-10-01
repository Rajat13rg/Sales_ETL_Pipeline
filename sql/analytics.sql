-- ============================================================
-- SALES DATA ETL & ANALYTICS
-- SQL ANALYTICS QUERIES
-- ============================================================

USE sales_dw;


-- ============================================================
-- 1. BASIC DATASET OVERVIEW
-- ============================================================

-- Total customers
SELECT
    COUNT(*) AS total_customers
FROM dim_customer;


-- Total products
SELECT
    COUNT(*) AS total_products
FROM dim_product;


-- Total sales records
SELECT
    COUNT(*) AS total_sales_records
FROM fact_sales;


-- Total unique orders
SELECT
    COUNT(DISTINCT order_id) AS total_orders
FROM fact_sales;


-- ============================================================
-- 2. OVERALL SALES KPIs
-- ============================================================

-- Total revenue
SELECT
    SUM(revenue) AS total_revenue
FROM fact_sales;


-- Total units sold
SELECT
    SUM(quantity) AS total_units_sold
FROM fact_sales;


-- Average order value
SELECT
    ROUND(
        SUM(revenue) / COUNT(DISTINCT order_id),
        2
    ) AS average_order_value
FROM fact_sales;


-- Average selling price
SELECT
    ROUND(
        SUM(revenue) / SUM(quantity),
        2
    ) AS average_selling_price
FROM fact_sales;


-- ============================================================
-- 3. PRODUCT ANALYTICS
-- ============================================================

-- Revenue by product
SELECT
    p.product_name,
    SUM(f.quantity) AS units_sold,
    SUM(f.revenue) AS total_revenue
FROM fact_sales f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY
    p.product_key,
    p.product_name
ORDER BY
    total_revenue DESC;


-- Top 5 products by revenue
SELECT
    p.product_name,
    SUM(f.quantity) AS units_sold,
    SUM(f.revenue) AS total_revenue
FROM fact_sales f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY
    p.product_key,
    p.product_name
ORDER BY
    total_revenue DESC
LIMIT 5;


-- Products by quantity sold
SELECT
    p.product_name,
    SUM(f.quantity) AS total_units_sold
FROM fact_sales f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY
    p.product_key,
    p.product_name
ORDER BY
    total_units_sold DESC;


-- ============================================================
-- 4. CATEGORY ANALYTICS
-- ============================================================

-- Revenue by category
SELECT
    p.category,
    SUM(f.quantity) AS units_sold,
    SUM(f.revenue) AS total_revenue
FROM fact_sales f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY
    p.category
ORDER BY
    total_revenue DESC;


-- Category revenue percentage
SELECT
    p.category,
    SUM(f.revenue) AS category_revenue,

    ROUND(
        SUM(f.revenue) * 100 /
        (
            SELECT SUM(revenue)
            FROM fact_sales
        ),
        2
    ) AS revenue_percentage

FROM fact_sales f
JOIN dim_product p
    ON f.product_key = p.product_key

GROUP BY
    p.category

ORDER BY
    category_revenue DESC;


-- ============================================================
-- 5. CUSTOMER ANALYTICS
-- ============================================================

-- Revenue by customer
SELECT
    c.customer_name,
    c.city,
    c.state,
    SUM(f.revenue) AS total_revenue

FROM fact_sales f
JOIN dim_customer c
    ON f.customer_key = c.customer_key

GROUP BY
    c.customer_key,
    c.customer_name,
    c.city,
    c.state

ORDER BY
    total_revenue DESC;


-- Top 5 customers by revenue
SELECT
    c.customer_name,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.revenue) AS total_revenue

FROM fact_sales f
JOIN dim_customer c
    ON f.customer_key = c.customer_key

GROUP BY
    c.customer_key,
    c.customer_name

ORDER BY
    total_revenue DESC

LIMIT 5;


-- Customer order frequency
SELECT
    c.customer_name,
    COUNT(DISTINCT f.order_id) AS total_orders

FROM fact_sales f
JOIN dim_customer c
    ON f.customer_key = c.customer_key

GROUP BY
    c.customer_key,
    c.customer_name

ORDER BY
    total_orders DESC;


-- ============================================================
-- 6. MONTHLY SALES ANALYSIS
-- ============================================================

-- Monthly revenue
SELECT
    d.year,
    d.month,
    d.month_name,
    SUM(f.revenue) AS total_revenue

FROM fact_sales f
JOIN dim_date d
    ON f.date_key = d.date_key

GROUP BY
    d.year,
    d.month,
    d.month_name

ORDER BY
    d.year,
    d.month;


-- Monthly orders
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id) AS total_orders

FROM fact_sales f
JOIN dim_date d
    ON f.date_key = d.date_key

GROUP BY
    d.year,
    d.month,
    d.month_name

ORDER BY
    d.year,
    d.month;


-- Monthly units sold
SELECT
    d.year,
    d.month,
    d.month_name,
    SUM(f.quantity) AS total_units_sold

FROM fact_sales f
JOIN dim_date d
    ON f.date_key = d.date_key

GROUP BY
    d.year,
    d.month,
    d.month_name

ORDER BY
    d.year,
    d.month;


-- ============================================================
-- 7. PAYMENT METHOD ANALYSIS
-- ============================================================

-- Orders and revenue by payment method
SELECT
    payment_method,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(revenue) AS total_revenue

FROM fact_sales

GROUP BY
    payment_method

ORDER BY
    total_revenue DESC;


-- Payment method revenue percentage
SELECT
    payment_method,
    SUM(revenue) AS total_revenue,

    ROUND(
        SUM(revenue) * 100 /
        (
            SELECT SUM(revenue)
            FROM fact_sales
        ),
        2
    ) AS revenue_percentage

FROM fact_sales

GROUP BY
    payment_method

ORDER BY
    total_revenue DESC;


-- ============================================================
-- 8. DAILY SALES ANALYSIS
-- ============================================================

-- Daily revenue
SELECT
    d.full_date,
    SUM(f.revenue) AS daily_revenue

FROM fact_sales f
JOIN dim_date d
    ON f.date_key = d.date_key

GROUP BY
    d.full_date

ORDER BY
    d.full_date;


-- Highest revenue day
SELECT
    d.full_date,
    SUM(f.revenue) AS daily_revenue

FROM fact_sales f
JOIN dim_date d
    ON f.date_key = d.date_key

GROUP BY
    d.full_date

ORDER BY
    daily_revenue DESC

LIMIT 1;


-- ============================================================
-- 9. TOP 3 PRODUCTS PER CATEGORY
-- ============================================================

WITH product_sales AS (

    SELECT
        p.category,
        p.product_name,
        SUM(f.revenue) AS total_revenue

    FROM fact_sales f

    JOIN dim_product p
        ON f.product_key = p.product_key

    GROUP BY
        p.category,
        p.product_name
),

ranked_products AS (

    SELECT
        category,
        product_name,
        total_revenue,

        DENSE_RANK() OVER (
            PARTITION BY category
            ORDER BY total_revenue DESC
        ) AS product_rank

    FROM product_sales
)

SELECT
    category,
    product_name,
    total_revenue,
    product_rank

FROM ranked_products

WHERE product_rank <= 3

ORDER BY
    category,
    product_rank;


-- ============================================================
-- 10. CUSTOMER RANKING
-- ============================================================

SELECT
    c.customer_name,
    SUM(f.revenue) AS total_revenue,

    DENSE_RANK() OVER (
        ORDER BY SUM(f.revenue) DESC
    ) AS customer_rank

FROM fact_sales f

JOIN dim_customer c
    ON f.customer_key = c.customer_key

GROUP BY
    c.customer_key,
    c.customer_name

ORDER BY
    customer_rank;


-- ============================================================
-- 11. RUNNING REVENUE
-- ============================================================

SELECT
    d.full_date,

    SUM(f.revenue) AS daily_revenue,

    SUM(
        SUM(f.revenue)
    ) OVER (
        ORDER BY d.full_date
    ) AS running_revenue

FROM fact_sales f

JOIN dim_date d
    ON f.date_key = d.date_key

GROUP BY
    d.full_date

ORDER BY
    d.full_date;


-- ============================================================
-- 12. REVENUE BY STATE
-- ============================================================

SELECT
    c.state,
    SUM(f.revenue) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders

FROM fact_sales f

JOIN dim_customer c
    ON f.customer_key = c.customer_key

GROUP BY
    c.state

ORDER BY
    total_revenue DESC;


-- ============================================================
-- 13. REVENUE BY CITY
-- ============================================================

SELECT
    c.city,
    SUM(f.revenue) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders

FROM fact_sales f

JOIN dim_customer c
    ON f.customer_key = c.customer_key

GROUP BY
    c.city

ORDER BY
    total_revenue DESC;


-- ============================================================
-- 14. PRODUCT PERFORMANCE
-- ============================================================

SELECT
    p.product_name,
    p.category,
    p.price,

    SUM(f.quantity) AS units_sold,

    SUM(f.revenue) AS total_revenue,

    ROUND(
        SUM(f.revenue) / SUM(f.quantity),
        2
    ) AS average_selling_price

FROM fact_sales f

JOIN dim_product p
    ON f.product_key = p.product_key

GROUP BY
    p.product_key,
    p.product_name,
    p.category,
    p.price

ORDER BY
    total_revenue DESC;


-- ============================================================
-- 15. CUSTOMER VALUE
-- ============================================================

SELECT
    c.customer_name,

    COUNT(DISTINCT f.order_id) AS total_orders,

    SUM(f.quantity) AS total_units,

    SUM(f.revenue) AS total_revenue,

    ROUND(
        SUM(f.revenue) /
        COUNT(DISTINCT f.order_id),
        2
    ) AS customer_average_order_value

FROM fact_sales f

JOIN dim_customer c
    ON f.customer_key = c.customer_key

GROUP BY
    c.customer_key,
    c.customer_name

ORDER BY
    total_revenue DESC;


-- ============================================================
-- 16. MONTHLY REVENUE WITH RUNNING TOTAL
-- ============================================================

WITH monthly_sales AS (

    SELECT
        d.year,
        d.month,
        d.month_name,
        SUM(f.revenue) AS monthly_revenue

    FROM fact_sales f

    JOIN dim_date d
        ON f.date_key = d.date_key

    GROUP BY
        d.year,
        d.month,
        d.month_name
)

SELECT
    year,
    month,
    month_name,
    monthly_revenue,

    SUM(monthly_revenue) OVER (
        ORDER BY year, month
    ) AS running_revenue

FROM monthly_sales

ORDER BY
    year,
    month;


-- ============================================================
-- 17. MONTH-OVER-MONTH REVENUE
-- ============================================================

WITH monthly_sales AS (

    SELECT
        d.year,
        d.month,
        d.month_name,
        SUM(f.revenue) AS monthly_revenue

    FROM fact_sales f

    JOIN dim_date d
        ON f.date_key = d.date_key

    GROUP BY
        d.year,
        d.month,
        d.month_name
)

SELECT
    year,
    month,
    month_name,
    monthly_revenue,

    LAG(monthly_revenue) OVER (
        ORDER BY year, month
    ) AS previous_month_revenue,

    monthly_revenue -
    LAG(monthly_revenue) OVER (
        ORDER BY year, month
    ) AS revenue_change

FROM monthly_sales

ORDER BY
    year,
    month;


-- ============================================================
-- 18. ORDER LEVEL ANALYSIS
-- ============================================================

SELECT
    order_id,

    SUM(quantity) AS total_items,

    SUM(revenue) AS order_value,

    payment_method

FROM fact_sales

GROUP BY
    order_id,
    payment_method

ORDER BY
    order_value DESC;


-- ============================================================
-- 19. HIGH VALUE ORDERS
-- ============================================================

SELECT
    order_id,
    SUM(revenue) AS order_value

FROM fact_sales

GROUP BY
    order_id

HAVING
    SUM(revenue) > 10000

ORDER BY
    order_value DESC;


-- ============================================================
-- 20. DATA WAREHOUSE VALIDATION
-- ============================================================

-- Dimension counts
SELECT
    'dim_customer' AS table_name,
    COUNT(*) AS record_count
FROM dim_customer

UNION ALL

SELECT
    'dim_product',
    COUNT(*)
FROM dim_product

UNION ALL

SELECT
    'dim_date',
    COUNT(*)
FROM dim_date

UNION ALL

SELECT
    'fact_sales',
    COUNT(*)
FROM fact_sales;


-- Check fact records with missing customer dimension
SELECT
    COUNT(*) AS invalid_customer_keys

FROM fact_sales f

LEFT JOIN dim_customer c
    ON f.customer_key = c.customer_key

WHERE c.customer_key IS NULL;


-- Check fact records with missing product dimension
SELECT
    COUNT(*) AS invalid_product_keys

FROM fact_sales f

LEFT JOIN dim_product p
    ON f.product_key = p.product_key

WHERE p.product_key IS NULL;


-- Check fact records with missing date dimension
SELECT
    COUNT(*) AS invalid_date_keys

FROM fact_sales f

LEFT JOIN dim_date d
    ON f.date_key = d.date_key

WHERE d.date_key IS NULL;