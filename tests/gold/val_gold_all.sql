-- =============================================================================
-- GOLD LAYER DATA VALIDATION QUERIES
-- Data Warehouse Project - PostgreSQL
-- =============================================================================
-- Purpose: Validate gold layer views and their data integrity
-- Views Validated:
--   - gold.dim_customers
--   - gold.dim_products  
--   - gold.fact_sales
-- =============================================================================
-- =============================================================================
-- 1. OVERALL GOLD LAYER SUMMARY
-- Provides high-level overview of all gold layer views
-- =============================================================================
SELECT
    'Gold Layer Summary' AS validation_type,
    'dim_customers' AS view_name,
    COUNT(*) AS record_count,
    'Customer Dimension' AS view_type,
    CASE
        WHEN COUNT(*) > 0 THEN 'ACTIVE'
        ELSE 'NO DATA'
    END AS status
FROM
    gold.dim_customers
UNION ALL
SELECT
    'Gold Layer Summary' AS validation_type,
    'dim_products' AS view_name,
    COUNT(*) AS record_count,
    'Product Dimension' AS view_type,
    CASE
        WHEN COUNT(*) > 0 THEN 'ACTIVE'
        ELSE 'NO DATA'
    END AS status
FROM
    gold.dim_products
UNION ALL
SELECT
    'Gold Layer Summary' AS validation_type,
    'fact_sales' AS view_name,
    COUNT(*) AS record_count,
    'Sales Fact Table' AS view_type,
    CASE
        WHEN COUNT(*) > 0 THEN 'ACTIVE'
        ELSE 'NO DATA'
    END AS status
FROM
    gold.fact_sales
;


-- =============================================================================
-- 2. DIM_CUSTOMERS VALIDATION
-- =============================================================================
-- 2.1 Basic Record Count and Uniqueness
SELECT
    'dim_customers - Basic Validation' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT customer_key) AS unique_customer_keys,
    COUNT(DISTINCT customer_id) AS unique_customer_ids,
    COUNT(DISTINCT customer_number) AS unique_customer_numbers,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT customer_key) THEN 'PASS'
        ELSE 'FAIL - Duplicate customer_keys'
    END AS customer_key_uniqueness,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT customer_id) THEN 'PASS'
        ELSE 'REVIEW - Multiple records per customer_id'
    END AS customer_id_uniqueness
FROM
    gold.dim_customers
;


-- 2.2 Data Completeness Check
SELECT
    'dim_customers - Data Completeness' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(customer_key) AS non_null_customer_keys,
    COUNT(customer_id) AS non_null_customer_ids,
    COUNT(customer_number) AS non_null_customer_numbers,
    COUNT(first_name) AS non_null_first_names,
    COUNT(last_name) AS non_null_last_names,
    COUNT(country) AS non_null_countries,
    COUNT(marital_status) AS non_null_marital_status,
    COUNT(gender) AS non_null_genders,
    COUNT(birthdate) AS non_null_birthdates,
    COUNT(create_date) AS non_null_create_dates,
    ROUND(100.0 * COUNT(customer_id) / COUNT(*), 2) AS customer_id_completeness_pct,
    ROUND(100.0 * COUNT(first_name) / COUNT(*), 2) AS first_name_completeness_pct,
    ROUND(100.0 * COUNT(last_name) / COUNT(*), 2) AS last_name_completeness_pct
FROM
    gold.dim_customers
;


-- 2.3 Gender Logic Validation
SELECT
    'dim_customers - Gender Logic Validation' AS validation_type,
    gender,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM
    gold.dim_customers
GROUP BY
    gender
ORDER BY
    COUNT(*) DESC
;


-- 2.4 Join Success Rate Analysis
SELECT
    'dim_customers - Join Analysis' AS validation_type,
    COUNT(*) AS total_customers,
    COUNT(country) AS customers_with_location_data,
    COUNT(birthdate) AS customers_with_erp_data,
    ROUND(100.0 * COUNT(country) / COUNT(*), 2) AS location_join_success_rate,
    ROUND(100.0 * COUNT(birthdate) / COUNT(*), 2) AS erp_join_success_rate,
    COUNT(*) - COUNT(country) AS customers_missing_location,
    COUNT(*) - COUNT(birthdate) AS customers_missing_erp_data
FROM
    gold.dim_customers
;


-- 2.5 Sample Records
SELECT
    'dim_customers - Sample Records' AS note,
    customer_key,
    customer_id,
    customer_number,
    first_name,
    last_name,
    country,
    marital_status,
    gender,
    birthdate,
    create_date
FROM
    gold.dim_customers
ORDER BY
    customer_key
LIMIT
    5
;


-- 2.6 Orphaned Records Check (customers in silver but missing in gold)
SELECT
    'dim_customers - Missing Records Check' AS validation_type,
    COUNT(*) AS silver_customer_records,
    (
        SELECT
            COUNT(*)
        FROM
            gold.dim_customers
    ) AS gold_customer_records,
    COUNT(*) - (
        SELECT
            COUNT(*)
        FROM
            gold.dim_customers
    ) AS missing_in_gold,
    CASE
        WHEN COUNT(*) = (
            SELECT
                COUNT(*)
            FROM
                gold.dim_customers
        ) THEN 'PASS'
        ELSE 'REVIEW - Records missing in gold layer'
    END AS completeness_status
FROM
    silver.crm_cust_info
;


-- =============================================================================
-- 3. DIM_PRODUCTS VALIDATION
-- =============================================================================
-- 3.1 Basic Record Count and Uniqueness
SELECT
    'dim_products - Basic Validation' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT product_key) AS unique_product_keys,
    COUNT(DISTINCT product_id) AS unique_product_ids,
    COUNT(DISTINCT product_number) AS unique_product_numbers,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT product_key) THEN 'PASS'
        ELSE 'FAIL - Duplicate product_keys'
    END AS product_key_uniqueness,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT product_id) THEN 'PASS'
        ELSE 'REVIEW - Multiple records per product_id'
    END AS product_id_uniqueness
FROM
    gold.dim_products
;


-- 3.2 Active Products Filter Validation
SELECT
    'dim_products - Active Filter Validation' AS validation_type,
    total_silver_products,
    active_products_in_gold,
    historical_products_filtered,
    ROUND(
        100.0 * active_products_in_gold / total_silver_products,
        2
    ) AS active_product_percentage
FROM
    (
        SELECT
            COUNT(*) AS total_silver_products
        FROM
            silver.crm_prd_info
    ) silver
    CROSS JOIN (
        SELECT
            COUNT(*) AS active_products_in_gold
        FROM
            gold.dim_products
    ) gold
    CROSS JOIN (
        SELECT
            COUNT(*) AS historical_products_filtered
        FROM
            silver.crm_prd_info
        WHERE
            prd_end_dt IS NOT NULL
    ) historical
;


-- 3.3 Data Completeness Check
SELECT
    'dim_products - Data Completeness' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(product_key) AS non_null_product_keys,
    COUNT(product_id) AS non_null_product_ids,
    COUNT(product_number) AS non_null_product_numbers,
    COUNT(product_name) AS non_null_product_names,
    COUNT(category_id) AS non_null_category_ids,
    COUNT(category) AS non_null_categories,
    COUNT(subcategory) AS non_null_subcategories,
    COUNT(maintenance) AS non_null_maintenance,
    COUNT(COST) AS non_null_costs,
    COUNT(product_line) AS non_null_product_lines,
    COUNT(product_start_date) AS non_null_start_dates,
    ROUND(100.0 * COUNT(category) / COUNT(*), 2) AS category_join_success_rate
FROM
    gold.dim_products
;


-- 3.4 Category Distribution Analysis
SELECT
    'dim_products - Category Distribution' AS validation_type,
    category,
    COUNT(*) AS product_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage,
    COUNT(DISTINCT subcategory) AS unique_subcategories
FROM
    gold.dim_products
WHERE
    category IS NOT NULL
GROUP BY
    category
ORDER BY
    COUNT(*) DESC
LIMIT
    10
;


-- 3.5 Sample Records
SELECT
    'dim_products - Sample Records' AS note,
    product_key,
    product_id,
    product_number,
    product_name,
    category_id,
    category,
    subcategory,
    maintenance,
    COST,
    product_line,
    product_start_date
FROM
    gold.dim_products
ORDER BY
    product_key
LIMIT
    5
;


-- 3.6 Category Join Success Analysis
SELECT
    'dim_products - Category Join Analysis' AS validation_type,
    COUNT(*) AS total_products,
    COUNT(category) AS products_with_category_data,
    COUNT(*) - COUNT(category) AS products_missing_category_data,
    ROUND(100.0 * COUNT(category) / COUNT(*), 2) AS category_join_success_rate,
    CASE
        WHEN COUNT(category) = COUNT(*) THEN 'PERFECT'
        WHEN ROUND(100.0 * COUNT(category) / COUNT(*), 2) >= 90 THEN 'GOOD'
        WHEN ROUND(100.0 * COUNT(category) / COUNT(*), 2) >= 70 THEN 'ACCEPTABLE'
        ELSE 'POOR'
    END AS join_quality_rating
FROM
    gold.dim_products
;


-- =============================================================================
-- 4. FACT_SALES VALIDATION
-- =============================================================================
-- 4.1 Basic Record Count and Structure
SELECT
    'fact_sales - Basic Validation' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT order_number) AS unique_orders,
    COUNT(DISTINCT product_key) AS unique_products_in_sales,
    COUNT(DISTINCT customer_key) AS unique_customers_in_sales,
    MIN(order_date) AS earliest_order_date,
    MAX(order_date) AS latest_order_date,
    COUNT(
        CASE
            WHEN product_key IS NULL THEN 1
        END
    ) AS sales_missing_product_key,
    COUNT(
        CASE
            WHEN customer_key IS NULL THEN 1
        END
    ) AS sales_missing_customer_key
FROM
    gold.fact_sales
;


-- 4.2 Data Completeness and Quality
SELECT
    'fact_sales - Data Completeness' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(order_number) AS non_null_order_numbers,
    COUNT(product_key) AS non_null_product_keys,
    COUNT(customer_key) AS non_null_customer_keys,
    COUNT(order_date) AS non_null_order_dates,
    COUNT(ship_date) AS non_null_ship_dates,
    COUNT(due_date) AS non_null_due_dates,
    COUNT(sales_amount) AS non_null_sales_amounts,
    COUNT(quantity) AS non_null_quantities,
    COUNT(price) AS non_null_prices,
    ROUND(100.0 * COUNT(product_key) / COUNT(*), 2) AS product_join_success_rate,
    ROUND(100.0 * COUNT(customer_key) / COUNT(*), 2) AS customer_join_success_rate
FROM
    gold.fact_sales
;


-- 4.3 Referential Integrity Validation
SELECT
    'fact_sales - Referential Integrity' AS validation_type,
    COUNT(*) AS total_sales_records,
    COUNT(DISTINCT fs.product_key) AS distinct_product_keys_in_sales,
    COUNT(DISTINCT fs.customer_key) AS distinct_customer_keys_in_sales,
    (
        SELECT
            COUNT(DISTINCT product_key)
        FROM
            gold.dim_products
    ) AS total_products_in_dim,
    (
        SELECT
            COUNT(DISTINCT customer_key)
        FROM
            gold.dim_customers
    ) AS total_customers_in_dim,
    COUNT(
        CASE
            WHEN dp.product_key IS NULL
            AND fs.product_key IS NOT NULL THEN 1
        END
    ) AS orphaned_product_keys,
    COUNT(
        CASE
            WHEN dc.customer_key IS NULL
            AND fs.customer_key IS NOT NULL THEN 1
        END
    ) AS orphaned_customer_keys
FROM
    gold.fact_sales fs
    LEFT JOIN gold.dim_products dp ON fs.product_key = dp.product_key
    LEFT JOIN gold.dim_customers dc ON fs.customer_key = dc.customer_key
;


-- 4.4 Sales Metrics Analysis
SELECT
    'fact_sales - Sales Metrics Analysis' AS validation_type,
    COUNT(*) AS total_transactions,
    ROUND(SUM(sales_amount), 2) AS total_sales_amount,
    ROUND(AVG(sales_amount), 2) AS avg_sales_amount,
    ROUND(MIN(sales_amount), 2) AS min_sales_amount,
    ROUND(MAX(sales_amount), 2) AS max_sales_amount,
    SUM(quantity) AS total_quantity_sold,
    ROUND(AVG(quantity), 2) AS avg_quantity_per_transaction,
    COUNT(
        CASE
            WHEN sales_amount <= 0 THEN 1
        END
    ) AS zero_or_negative_sales,
    COUNT(
        CASE
            WHEN quantity <= 0 THEN 1
        END
    ) AS zero_or_negative_quantity,
    COUNT(
        CASE
            WHEN price <= 0 THEN 1
        END
    ) AS zero_or_negative_price
FROM
    gold.fact_sales
;


-- 4.5 Date Integrity Analysis
SELECT
    'fact_sales - Date Integrity Analysis' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN order_date IS NOT NULL THEN 1
        END
    ) AS records_with_order_date,
    COUNT(
        CASE
            WHEN ship_date IS NOT NULL THEN 1
        END
    ) AS records_with_ship_date,
    COUNT(
        CASE
            WHEN due_date IS NOT NULL THEN 1
        END
    ) AS records_with_due_date,
    COUNT(
        CASE
            WHEN ship_date < order_date THEN 1
        END
    ) AS ship_before_order_anomalies,
    COUNT(
        CASE
            WHEN due_date < order_date THEN 1
        END
    ) AS due_before_order_anomalies,
    COUNT(
        CASE
            WHEN ship_date > due_date THEN 1
        END
    ) AS late_shipments,
    ROUND(
        100.0 * COUNT(
            CASE
                WHEN ship_date > due_date THEN 1
            END
        ) / COUNT(
            CASE
                WHEN ship_date IS NOT NULL
                AND due_date IS NOT NULL THEN 1
            END
        ),
        2
    ) AS late_shipment_percentage
FROM
    gold.fact_sales
;


-- 4.6 Sales Volume by Time Period
SELECT
    'fact_sales - Sales Volume by Month' AS validation_type,
    DATE_TRUNC('month', order_date) AS sales_month,
    COUNT(*) AS transaction_count,
    ROUND(SUM(sales_amount), 2) AS monthly_sales_amount,
    ROUND(AVG(sales_amount), 2) AS avg_transaction_amount
FROM
    gold.fact_sales
WHERE
    order_date IS NOT NULL
GROUP BY
    DATE_TRUNC('month', order_date)
ORDER BY
    sales_month DESC
LIMIT
    12
;


-- 4.7 Sample Records
SELECT
    'fact_sales - Sample Records' AS note,
    order_number,
    product_key,
    customer_key,
    order_date,
    ship_date,
    due_date,
    sales_amount,
    quantity,
    price
FROM
    gold.fact_sales
ORDER BY
    order_date DESC
LIMIT
    5
;


-- 4.8 Orphaned Sales Records Analysis
SELECT
    'fact_sales - Orphaned Records Analysis' AS validation_type,
    'Missing Product Dimension' AS orphan_type,
    COUNT(*) AS orphaned_count,
    ROUND(
        100.0 * COUNT(*) / (
            SELECT
                COUNT(*)
            FROM
                gold.fact_sales
        ),
        2
    ) AS orphaned_percentage
FROM
    gold.fact_sales fs
WHERE
    fs.product_key IS NOT NULL
    AND NOT EXISTS (
        SELECT
            1
        FROM
            gold.dim_products dp
        WHERE
            dp.product_key = fs.product_key
    )
UNION ALL
SELECT
    'fact_sales - Orphaned Records Analysis' AS validation_type,
    'Missing Customer Dimension' AS orphan_type,
    COUNT(*) AS orphaned_count,
    ROUND(
        100.0 * COUNT(*) / (
            SELECT
                COUNT(*)
            FROM
                gold.fact_sales
        ),
        2
    ) AS orphaned_percentage
FROM
    gold.fact_sales fs
WHERE
    fs.customer_key IS NOT NULL
    AND NOT EXISTS (
        SELECT
            1
        FROM
            gold.dim_customers dc
        WHERE
            dc.customer_key = fs.customer_key
    )
;


-- =============================================================================
-- 5. CROSS-VIEW RELATIONSHIP VALIDATION
-- =============================================================================
-- 5.1 Dimension Usage in Fact Table
SELECT
    'Cross-View - Dimension Usage' AS validation_type,
    (
        SELECT
            COUNT(DISTINCT customer_key)
        FROM
            gold.dim_customers
    ) AS total_customers_in_dim,
    (
        SELECT
            COUNT(DISTINCT customer_key)
        FROM
            gold.fact_sales
        WHERE
            customer_key IS NOT NULL
    ) AS customers_with_sales,
    (
        SELECT
            COUNT(DISTINCT product_key)
        FROM
            gold.dim_products
    ) AS total_products_in_dim,
    (
        SELECT
            COUNT(DISTINCT product_key)
        FROM
            gold.fact_sales
        WHERE
            product_key IS NOT NULL
    ) AS products_with_sales,
    ROUND(
        100.0 * (
            SELECT
                COUNT(DISTINCT customer_key)
            FROM
                gold.fact_sales
            WHERE
                customer_key IS NOT NULL
        ) / (
            SELECT
                COUNT(DISTINCT customer_key)
            FROM
                gold.dim_customers
        ),
        2
    ) AS customer_utilization_rate,
    ROUND(
        100.0 * (
            SELECT
                COUNT(DISTINCT product_key)
            FROM
                gold.fact_sales
            WHERE
                product_key IS NOT NULL
        ) / (
            SELECT
                COUNT(DISTINCT product_key)
            FROM
                gold.dim_products
        ),
        2
    ) AS product_utilization_rate
;


-- 5.2 Top Selling Products
SELECT
    'Cross-View - Top Selling Products' AS validation_type,
    dp.product_name,
    dp.category,
    COUNT(fs.order_number) AS transaction_count,
    ROUND(SUM(fs.sales_amount), 2) AS total_sales,
    ROUND(AVG(fs.sales_amount), 2) AS avg_sale_amount
FROM
    gold.fact_sales fs
    JOIN gold.dim_products dp ON fs.product_key = dp.product_key
GROUP BY
    dp.product_key,
    dp.product_name,
    dp.category
ORDER BY
    SUM(fs.sales_amount) DESC
LIMIT
    10
;


-- 5.3 Top Customers by Sales
SELECT
    'Cross-View - Top Customers by Sales' AS validation_type,
    dc.first_name,
    dc.last_name,
    dc.country,
    COUNT(fs.order_number) AS transaction_count,
    ROUND(SUM(fs.sales_amount), 2) AS total_purchases,
    ROUND(AVG(fs.sales_amount), 2) AS avg_purchase_amount
FROM
    gold.fact_sales fs
    JOIN gold.dim_customers dc ON fs.customer_key = dc.customer_key
GROUP BY
    dc.customer_key,
    dc.first_name,
    dc.last_name,
    dc.country
ORDER BY
    SUM(fs.sales_amount) DESC
LIMIT
    10
;


-- =============================================================================
-- 6. DATA LINEAGE AND TRANSFORMATION VALIDATION
-- =============================================================================
-- 6.1 Silver to Gold Record Count Comparison
SELECT
    'Data Lineage - Record Counts' AS validation_type,
    'Customer Dimension' AS data_flow,
    (
        SELECT
            COUNT(*)
        FROM
            silver.crm_cust_info
    ) AS silver_source_count,
    (
        SELECT
            COUNT(*)
        FROM
            gold.dim_customers
    ) AS gold_target_count,
    (
        SELECT
            COUNT(*)
        FROM
            silver.crm_cust_info
    ) - (
        SELECT
            COUNT(*)
        FROM
            gold.dim_customers
    ) AS record_difference,
    CASE
        WHEN (
            SELECT
                COUNT(*)
            FROM
                silver.crm_cust_info
        ) = (
            SELECT
                COUNT(*)
            FROM
                gold.dim_customers
        ) THEN 'PERFECT MATCH'
        ELSE 'DIFFERENCE DETECTED'
    END AS lineage_status
UNION ALL
SELECT
    'Data Lineage - Record Counts' AS validation_type,
    'Product Dimension' AS data_flow,
    (
        SELECT
            COUNT(*)
        FROM
            silver.crm_prd_info
        WHERE
            prd_end_dt IS NULL
    ) AS silver_source_count,
    (
        SELECT
            COUNT(*)
        FROM
            gold.dim_products
    ) AS gold_target_count,
    (
        SELECT
            COUNT(*)
        FROM
            silver.crm_prd_info
        WHERE
            prd_end_dt IS NULL
    ) - (
        SELECT
            COUNT(*)
        FROM
            gold.dim_products
    ) AS record_difference,
    CASE
        WHEN (
            SELECT
                COUNT(*)
            FROM
                silver.crm_prd_info
            WHERE
                prd_end_dt IS NULL
        ) = (
            SELECT
                COUNT(*)
            FROM
                gold.dim_products
        ) THEN 'PERFECT MATCH'
        ELSE 'DIFFERENCE DETECTED'
    END AS lineage_status
UNION ALL
SELECT
    'Data Lineage - Record Counts' AS validation_type,
    'Sales Fact' AS data_flow,
    (
        SELECT
            COUNT(*)
        FROM
            silver.crm_sales_details
    ) AS silver_source_count,
    (
        SELECT
            COUNT(*)
        FROM
            gold.fact_sales
    ) AS gold_target_count,
    (
        SELECT
            COUNT(*)
        FROM
            silver.crm_sales_details
    ) - (
        SELECT
            COUNT(*)
        FROM
            gold.fact_sales
    ) AS record_difference,
    CASE
        WHEN (
            SELECT
                COUNT(*)
            FROM
                silver.crm_sales_details
        ) = (
            SELECT
                COUNT(*)
            FROM
                gold.fact_sales
        ) THEN 'PERFECT MATCH'
        ELSE 'DIFFERENCE DETECTED'
    END AS lineage_status
;


-- 6.2 Row_Number() Sequence Validation
SELECT
    'Data Lineage - Row Number Validation' AS validation_type,
    'dim_customers' AS table_name,
    COUNT(*) AS total_records,
    MIN(customer_key) AS min_key,
    MAX(customer_key) AS max_key,
    CASE
        WHEN MIN(customer_key) = 1
        AND MAX(customer_key) = COUNT(*) THEN 'PASS - Sequential keys'
        ELSE 'REVIEW - Non-sequential keys'
    END AS sequence_validation
FROM
    gold.dim_customers
UNION ALL
SELECT
    'Data Lineage - Row Number Validation' AS validation_type,
    'dim_products' AS table_name,
    COUNT(*) AS total_records,
    MIN(product_key) AS min_key,
    MAX(product_key) AS max_key,
    CASE
        WHEN MIN(product_key) = 1
        AND MAX(product_key) = COUNT(*) THEN 'PASS - Sequential keys'
        ELSE 'REVIEW - Non-sequential keys'
    END AS sequence_validation
FROM
    gold.dim_products
;


-- =============================================================================
-- 7. BUSINESS LOGIC VALIDATION
-- =============================================================================
-- 7.1 Gender Transformation Logic Check
SELECT
    'Business Logic - Gender Transformation' AS validation_type,
    ci.cst_gndr AS crm_gender,
    ca.gen AS erp_gender,
    dc.gender AS final_gender,
    COUNT(*) AS record_count
FROM
    silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
    LEFT JOIN gold.dim_customers dc ON ci.cst_id = dc.customer_id
GROUP BY
    ci.cst_gndr,
    ca.gen,
    dc.gender
ORDER BY
    COUNT(*) DESC
LIMIT
    20
;


-- 7.2 Active Products Filter Logic Check
SELECT
    'Business Logic - Active Products Filter' AS validation_type,
    CASE
        WHEN prd_end_dt IS NULL THEN 'Active (Should be in Gold)'
        ELSE 'Historical (Should be filtered out)'
    END AS product_status,
    COUNT(*) AS silver_count,
    COUNT(
        CASE
            WHEN dp.product_id IS NOT NULL THEN 1
        END
    ) AS gold_count,
    COUNT(*) - COUNT(
        CASE
            WHEN dp.product_id IS NOT NULL THEN 1
        END
    ) AS filtered_count
FROM
    silver.crm_prd_info cp
    LEFT JOIN gold.dim_products dp ON cp.prd_id = dp.product_id
GROUP BY
    CASE
        WHEN prd_end_dt IS NULL THEN 'Active (Should be in Gold)'
        ELSE 'Historical (Should be filtered out)'
    END
;


-- =============================================================================
-- 8. COMPREHENSIVE GOLD LAYER VALIDATION SUMMARY
-- =============================================================================
WITH
    validation_summary AS (
        SELECT
            (
                SELECT
                    COUNT(*)
                FROM
                    gold.dim_customers
            ) AS customer_count,
            (
                SELECT
                    COUNT(*)
                FROM
                    gold.dim_products
            ) AS product_count,
            (
                SELECT
                    COUNT(*)
                FROM
                    gold.fact_sales
            ) AS sales_count,
            (
                SELECT
                    COUNT(*)
                FROM
                    gold.fact_sales
                WHERE
                    product_key IS NULL
            ) AS sales_missing_products,
            (
                SELECT
                    COUNT(*)
                FROM
                    gold.fact_sales
                WHERE
                    customer_key IS NULL
            ) AS sales_missing_customers,
            (
                SELECT
                    COUNT(DISTINCT customer_key)
                FROM
                    gold.dim_customers
            ) AS unique_customers,
            (
                SELECT
                    COUNT(DISTINCT product_key)
                FROM
                    gold.dim_products
            ) AS unique_products,
            (
                SELECT
                    ROUND(SUM(sales_amount), 2)
                FROM
                    gold.fact_sales
            ) AS total_sales_amount,
            (
                SELECT
                    MIN(order_date)
                FROM
                    gold.fact_sales
            ) AS earliest_sale,
            (
                SELECT
                    MAX(order_date)
                FROM
                    gold.fact_sales
            ) AS latest_sale
    )
SELECT
    'GOLD LAYER VALIDATION SUMMARY' AS report_type,
    customer_count,
    product_count,
    sales_count,
    unique_customers,
    unique_products,
    total_sales_amount,
    earliest_sale,
    latest_sale,
    sales_missing_products,
    sales_missing_customers,
    CASE
        WHEN customer_count > 0
        AND product_count > 0
        AND sales_count > 0
        AND sales_missing_products = 0
        AND sales_missing_customers = 0 THEN 'EXCELLENT - All views populated with full referential integrity'
        WHEN customer_count > 0
        AND product_count > 0
        AND sales_count > 0 THEN 'GOOD - All views populated with minor referential issues'
        WHEN customer_count = 0
        OR product_count = 0
        OR sales_count = 0 THEN 'CRITICAL - One or more views are empty'
        ELSE 'REVIEW REQUIRED'
    END AS overall_status,
    ROUND(
        100.0 * (
            sales_count - sales_missing_products - sales_missing_customers
        ) / NULLIF(sales_count, 0),
        2
    ) AS referential_integrity_score
FROM
    validation_summary
;


-- =============================================================================
-- 9. VIEW DEPENDENCY AND PERFORMANCE CHECK
-- =============================================================================
SELECT
    'View Dependencies' AS validation_type,
    schemaname,
    viewname,
    definition
FROM
    pg_views
WHERE
    schemaname = 'gold'
    AND viewname IN ('dim_customers', 'dim_products', 'fact_sales')
ORDER BY
    viewname
;


/*
Usage Instructions:
1. Run these queries after creating your gold layer views
2. Review all FAIL, CRITICAL, or REVIEW statuses
3. Investigate any referential integrity issues
4. Validate business logic transformations
5. Monitor data quality scores and completeness percentages
6. Use sample queries to manually verify data accuracy
 */
