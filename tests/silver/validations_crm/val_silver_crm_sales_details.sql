-- =============================================================================
-- DATA VALIDATION QUERIES FOR CRM_SALES_DETAILS SILVER LAYER TRANSFORMATION
-- =============================================================================
-- 1. RECORD COUNT VALIDATION
-- Ensure all records are preserved in transformation (1:1 mapping)
-- =============================================================================
SELECT
    'Record Count Validation' AS validation_type,
    bronze_count,
    silver_count,
    CASE
        WHEN bronze_count = silver_count THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    (
        SELECT
            COUNT(*) AS bronze_count
        FROM
            bronze.crm_sales_details
    ) bronze
    CROSS JOIN (
        SELECT
            COUNT(*) AS silver_count
        FROM
            silver.crm_sales_details
    ) silver
;


-- 2. DATE TRANSFORMATION VALIDATION - SLS_ORDER_DT
-- Verify date conversion logic: 0 values and invalid lengths converted to NULL
-- =============================================================================
SELECT
    'Order Date Transformation Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN b.sls_order_dt = 0 THEN 1
        END
    ) AS bronze_zero_dates,
    COUNT(
        CASE
            WHEN LENGTH(CAST(b.sls_order_dt AS VARCHAR)) != 8
            AND b.sls_order_dt != 0 THEN 1
        END
    ) AS bronze_invalid_length_dates,
    COUNT(
        CASE
            WHEN s.sls_order_dt IS NULL THEN 1
        END
    ) AS silver_null_dates,
    COUNT(
        CASE
            WHEN s.sls_order_dt IS NOT NULL THEN 1
        END
    ) AS silver_valid_dates,
    -- Validate that all invalid dates in bronze become NULL in silver
    CASE
        WHEN COUNT(
            CASE
                WHEN (
                    b.sls_order_dt = 0
                    OR LENGTH(CAST(b.sls_order_dt AS VARCHAR)) != 8
                )
                AND s.sls_order_dt IS NOT NULL THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.crm_sales_details b
    JOIN silver.crm_sales_details s ON b.sls_ord_num = s.sls_ord_num
    AND b.sls_prd_key = s.sls_prd_key
    AND b.sls_cust_id = s.sls_cust_id
;


-- Sample of date transformations for manual review
SELECT
    'Sample Date Transformations' AS note,
    b.sls_order_dt AS bronze_order_dt,
    LENGTH(CAST(b.sls_order_dt AS VARCHAR)) AS bronze_date_length,
    s.sls_order_dt AS silver_order_dt,
    CASE
        WHEN b.sls_order_dt = 0 THEN 'Zero Date'
        WHEN LENGTH(CAST(b.sls_order_dt AS VARCHAR)) != 8 THEN 'Invalid Length'
        ELSE 'Valid Format'
    END AS bronze_date_status
FROM
    bronze.crm_sales_details b
    JOIN silver.crm_sales_details s ON b.sls_ord_num = s.sls_ord_num
    AND b.sls_prd_key = s.sls_prd_key
    AND b.sls_cust_id = s.sls_cust_id
WHERE
    b.sls_order_dt = 0
    OR LENGTH(CAST(b.sls_order_dt AS VARCHAR)) != 8
    OR s.sls_order_dt IS NULL
LIMIT
    20
;


-- 3. DATE TRANSFORMATION VALIDATION - SLS_SHIP_DT
-- Verify ship date conversion logic
-- =============================================================================
SELECT
    'Ship Date Transformation Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN b.sls_ship_dt = 0 THEN 1
        END
    ) AS bronze_zero_dates,
    COUNT(
        CASE
            WHEN LENGTH(CAST(b.sls_ship_dt AS VARCHAR)) != 8
            AND b.sls_ship_dt != 0 THEN 1
        END
    ) AS bronze_invalid_length_dates,
    COUNT(
        CASE
            WHEN s.sls_ship_dt IS NULL THEN 1
        END
    ) AS silver_null_dates,
    COUNT(
        CASE
            WHEN s.sls_ship_dt IS NOT NULL THEN 1
        END
    ) AS silver_valid_dates,
    CASE
        WHEN COUNT(
            CASE
                WHEN (
                    b.sls_ship_dt = 0
                    OR LENGTH(CAST(b.sls_ship_dt AS VARCHAR)) != 8
                )
                AND s.sls_ship_dt IS NOT NULL THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.crm_sales_details b
    JOIN silver.crm_sales_details s ON b.sls_ord_num = s.sls_ord_num
    AND b.sls_prd_key = s.sls_prd_key
    AND b.sls_cust_id = s.sls_cust_id
;


-- 4. DATE TRANSFORMATION VALIDATION - SLS_DUE_DT
-- Verify due date conversion logic
-- =============================================================================
SELECT
    'Due Date Transformation Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN b.sls_due_dt = 0 THEN 1
        END
    ) AS bronze_zero_dates,
    COUNT(
        CASE
            WHEN LENGTH(CAST(b.sls_due_dt AS VARCHAR)) != 8
            AND b.sls_due_dt != 0 THEN 1
        END
    ) AS bronze_invalid_length_dates,
    COUNT(
        CASE
            WHEN s.sls_due_dt IS NULL THEN 1
        END
    ) AS silver_null_dates,
    COUNT(
        CASE
            WHEN s.sls_due_dt IS NOT NULL THEN 1
        END
    ) AS silver_valid_dates,
    CASE
        WHEN COUNT(
            CASE
                WHEN (
                    b.sls_due_dt = 0
                    OR LENGTH(CAST(b.sls_due_dt AS VARCHAR)) != 8
                )
                AND s.sls_due_dt IS NOT NULL THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.crm_sales_details b
    JOIN silver.crm_sales_details s ON b.sls_ord_num = s.sls_ord_num
    AND b.sls_prd_key = s.sls_prd_key
    AND b.sls_cust_id = s.sls_cust_id
;


-- 5. SALES AMOUNT TRANSFORMATION VALIDATION
-- Verify sales = quantity * price business rule implementation
-- =============================================================================
WITH
    validation_data AS (
        SELECT
            b.*,
            s.sls_sales AS silver_sales,
            s.sls_price AS silver_price,
            -- Expected silver sales based on transformation logic
            CASE
                WHEN ABS(b.sls_sales) IS NULL
                OR b.sls_sales != b.sls_quantity * ABS(b.sls_price) THEN NULLIF(b.sls_quantity, 0) * ABS(b.sls_price)
                ELSE ABS(b.sls_sales)
            END AS expected_silver_sales,
            -- Check if bronze sales follows business rule
            CASE
                WHEN b.sls_sales = b.sls_quantity * ABS(b.sls_price) THEN 'Follows Rule'
                WHEN b.sls_sales IS NULL THEN 'NULL Sales'
                ELSE 'Violates Rule'
            END AS bronze_sales_status
        FROM
            bronze.crm_sales_details b
            JOIN silver.crm_sales_details s ON b.sls_ord_num = s.sls_ord_num
            AND b.sls_prd_key = s.sls_prd_key
            AND b.sls_cust_id = s.sls_cust_id
    )
SELECT
    'Sales Amount Transformation Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN bronze_sales_status = 'Follows Rule' THEN 1
        END
    ) AS bronze_follows_rule,
    COUNT(
        CASE
            WHEN bronze_sales_status = 'Violates Rule' THEN 1
        END
    ) AS bronze_violates_rule,
    COUNT(
        CASE
            WHEN bronze_sales_status = 'NULL Sales' THEN 1
        END
    ) AS bronze_null_sales,
    COUNT(
        CASE
            WHEN silver_sales = expected_silver_sales THEN 1
        END
    ) AS correct_transformations,
    COUNT(
        CASE
            WHEN silver_sales != expected_silver_sales
            OR (
                silver_sales IS NULL
                AND expected_silver_sales IS NOT NULL
            )
            OR (
                silver_sales IS NOT NULL
                AND expected_silver_sales IS NULL
            ) THEN 1
        END
    ) AS incorrect_transformations,
    CASE
        WHEN COUNT(
            CASE
                WHEN silver_sales = expected_silver_sales THEN 1
            END
        ) = COUNT(*) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    validation_data
;


-- Sample of sales transformations for review
SELECT
    'Sample Sales Transformations' AS note,
    b.sls_ord_num,
    b.sls_quantity,
    b.sls_price AS bronze_price,
    b.sls_sales AS bronze_sales,
    s.sls_price AS silver_price,
    s.sls_sales AS silver_sales,
    b.sls_quantity * ABS(b.sls_price) AS calculated_sales,
    CASE
        WHEN b.sls_sales = b.sls_quantity * ABS(b.sls_price) THEN 'Rule Compliant'
        ELSE 'Rule Violation'
    END AS bronze_rule_status
FROM
    bronze.crm_sales_details b
    JOIN silver.crm_sales_details s ON b.sls_ord_num = s.sls_ord_num
    AND b.sls_prd_key = s.sls_prd_key
    AND b.sls_cust_id = s.sls_cust_id
WHERE
    b.sls_sales != b.sls_quantity * ABS(b.sls_price)
    OR b.sls_sales IS NULL
LIMIT
    20
;


-- 6. PRICE TRANSFORMATION VALIDATION
-- Verify price calculation and absolute value application
-- =============================================================================
WITH
    price_validation AS (
        SELECT
            b.*,
            s.sls_price AS silver_price,
            -- Expected silver price based on transformation logic
            CASE
                WHEN ABS(b.sls_price) IS NULL THEN ABS(b.sls_sales) / NULLIF(b.sls_quantity, 0)
                ELSE ABS(b.sls_price)
            END AS expected_silver_price
        FROM
            bronze.crm_sales_details b
            JOIN silver.crm_sales_details s ON b.sls_ord_num = s.sls_ord_num
            AND b.sls_prd_key = s.sls_prd_key
            AND b.sls_cust_id = s.sls_cust_id
    )
SELECT
    'Price Transformation Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN sls_price IS NULL THEN 1
        END
    ) AS bronze_null_prices,
    COUNT(
        CASE
            WHEN sls_price < 0 THEN 1
        END
    ) AS bronze_negative_prices,
    COUNT(
        CASE
            WHEN silver_price < 0 THEN 1
        END
    ) AS silver_negative_prices,
    COUNT(
        CASE
            WHEN ABS(silver_price - expected_silver_price) < 0.01
            OR (
                silver_price IS NULL
                AND expected_silver_price IS NULL
            ) THEN 1
        END
    ) AS correct_price_transformations,
    CASE
        WHEN COUNT(
            CASE
                WHEN silver_price < 0 THEN 1
            END
        ) = 0
        AND COUNT(
            CASE
                WHEN ABS(silver_price - expected_silver_price) < 0.01
                OR (
                    silver_price IS NULL
                    AND expected_silver_price IS NULL
                ) THEN 1
            END
        ) = COUNT(*) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    price_validation
;


-- 7. NEGATIVE VALUES HANDLING VALIDATION
-- Verify all negative sales and prices are converted to positive (ABS function)
-- =============================================================================
SELECT
    'Negative Values Handling Check' AS validation_type,
    COUNT(
        CASE
            WHEN b.sls_sales < 0 THEN 1
        END
    ) AS bronze_negative_sales,
    COUNT(
        CASE
            WHEN b.sls_price < 0 THEN 1
        END
    ) AS bronze_negative_prices,
    COUNT(
        CASE
            WHEN s.sls_sales < 0 THEN 1
        END
    ) AS silver_negative_sales,
    COUNT(
        CASE
            WHEN s.sls_price < 0 THEN 1
        END
    ) AS silver_negative_prices,
    CASE
        WHEN COUNT(
            CASE
                WHEN s.sls_sales < 0 THEN 1
            END
        ) = 0
        AND COUNT(
            CASE
                WHEN s.sls_price < 0 THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.crm_sales_details b
    JOIN silver.crm_sales_details s ON b.sls_ord_num = s.sls_ord_num
    AND b.sls_prd_key = s.sls_prd_key
    AND b.sls_cust_id = s.sls_cust_id
;


-- 8. NULL AND ZERO HANDLING VALIDATION
-- Verify NULLIF and division by zero handling
-- =============================================================================
SELECT
    'NULL and Zero Handling Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN b.sls_quantity = 0 THEN 1
        END
    ) AS bronze_zero_quantity,
    COUNT(
        CASE
            WHEN s.sls_quantity = 0 THEN 1
        END
    ) AS silver_zero_quantity,
    COUNT(
        CASE
            WHEN b.sls_sales IS NULL THEN 1
        END
    ) AS bronze_null_sales,
    COUNT(
        CASE
            WHEN b.sls_price IS NULL THEN 1
        END
    ) AS bronze_null_prices,
    COUNT(
        CASE
            WHEN s.sls_sales IS NULL THEN 1
        END
    ) AS silver_null_sales,
    COUNT(
        CASE
            WHEN s.sls_price IS NULL THEN 1
        END
    ) AS silver_null_prices,
    -- Check for division by zero scenarios
    COUNT(
        CASE
            WHEN b.sls_quantity = 0
            AND s.sls_price IS NOT NULL THEN 1
        END
    ) AS potential_division_errors,
    CASE
        WHEN COUNT(
            CASE
                WHEN b.sls_quantity = 0
                AND s.sls_price IS NOT NULL THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'REVIEW'
    END AS status
FROM
    bronze.crm_sales_details b
    JOIN silver.crm_sales_details s ON b.sls_ord_num = s.sls_ord_num
    AND b.sls_prd_key = s.sls_prd_key
    AND b.sls_cust_id = s.sls_cust_id
;


-- 9. DATA COMPLETENESS VALIDATION
-- Check for missing critical values
-- =============================================================================
SELECT
    'Data Completeness Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN sls_ord_num IS NULL THEN 1
        END
    ) AS null_order_numbers,
    COUNT(
        CASE
            WHEN sls_prd_key IS NULL THEN 1
        END
    ) AS null_product_keys,
    COUNT(
        CASE
            WHEN sls_cust_id IS NULL THEN 1
        END
    ) AS null_customer_ids,
    COUNT(
        CASE
            WHEN sls_quantity IS NULL THEN 1
        END
    ) AS null_quantities,
    COUNT(
        CASE
            WHEN sls_sales IS NULL THEN 1
        END
    ) AS null_sales,
    COUNT(
        CASE
            WHEN sls_price IS NULL THEN 1
        END
    ) AS null_prices,
    COUNT(
        CASE
            WHEN sls_order_dt IS NULL THEN 1
        END
    ) AS null_order_dates,
    COUNT(
        CASE
            WHEN sls_ship_dt IS NULL THEN 1
        END
    ) AS null_ship_dates,
    COUNT(
        CASE
            WHEN sls_due_dt IS NULL THEN 1
        END
    ) AS null_due_dates,
    CASE
        WHEN COUNT(
            CASE
                WHEN sls_ord_num IS NULL
                OR sls_prd_key IS NULL
                OR sls_cust_id IS NULL THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.crm_sales_details
;


-- 10. BUSINESS RULE COMPLIANCE VALIDATION
-- Verify sales = quantity * price rule is maintained in silver layer
-- =============================================================================
SELECT
    'Business Rule Compliance Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN ABS(sls_sales - (sls_quantity * sls_price)) < 0.01 THEN 1
        END
    ) AS rule_compliant_records,
    COUNT(
        CASE
            WHEN sls_sales IS NULL
            AND (
                sls_quantity IS NULL
                OR sls_price IS NULL
            ) THEN 1
        END
    ) AS acceptable_nulls,
    COUNT(
        CASE
            WHEN ABS(sls_sales - (sls_quantity * sls_price)) >= 0.01
            AND sls_sales IS NOT NULL
            AND sls_quantity IS NOT NULL
            AND sls_price IS NOT NULL THEN 1
        END
    ) AS rule_violations,
    ROUND(
        100.0 * COUNT(
            CASE
                WHEN ABS(sls_sales - (sls_quantity * sls_price)) < 0.01 THEN 1
            END
        ) / COUNT(
            CASE
                WHEN sls_sales IS NOT NULL
                AND sls_quantity IS NOT NULL
                AND sls_price IS NOT NULL THEN 1
            END
        ),
        2
    ) AS compliance_percentage,
    CASE
        WHEN COUNT(
            CASE
                WHEN ABS(sls_sales - (sls_quantity * sls_price)) >= 0.01
                AND sls_sales IS NOT NULL
                AND sls_quantity IS NOT NULL
                AND sls_price IS NOT NULL THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.crm_sales_details
;


-- Sample of business rule violations for review
SELECT
    'Business Rule Violations Sample' AS note,
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_quantity,
    sls_price,
    sls_sales,
    sls_quantity * sls_price AS calculated_sales,
    ABS(sls_sales - (sls_quantity * sls_price)) AS VARIANCE
FROM
    silver.crm_sales_details
WHERE
    ABS(sls_sales - (sls_quantity * sls_price)) >= 0.01
    AND sls_sales IS NOT NULL
    AND sls_quantity IS NOT NULL
    AND sls_price IS NOT NULL
LIMIT
    10
;


-- 11. DATE RANGE AND LOGICAL VALIDATION
-- Verify date relationships and reasonable ranges
-- =============================================================================
SELECT
    'Date Logic Validation' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN sls_order_dt IS NOT NULL THEN 1
        END
    ) AS records_with_order_date,
    COUNT(
        CASE
            WHEN sls_ship_dt IS NOT NULL THEN 1
        END
    ) AS records_with_ship_date,
    COUNT(
        CASE
            WHEN sls_due_dt IS NOT NULL THEN 1
        END
    ) AS records_with_due_date,
    COUNT(
        CASE
            WHEN sls_order_dt > CURRENT_DATE THEN 1
        END
    ) AS future_order_dates,
    COUNT(
        CASE
            WHEN sls_ship_dt < sls_order_dt THEN 1
        END
    ) AS ship_before_order,
    COUNT(
        CASE
            WHEN sls_due_dt < sls_order_dt THEN 1
        END
    ) AS due_before_order,
    MIN(sls_order_dt) AS earliest_order_date,
    MAX(sls_order_dt) AS latest_order_date,
    CASE
        WHEN COUNT(
            CASE
                WHEN sls_ship_dt < sls_order_dt THEN 1
            END
        ) = 0
        AND COUNT(
            CASE
                WHEN sls_due_dt < sls_order_dt THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'REVIEW'
    END AS status
FROM
    silver.crm_sales_details
;


-- 12. TRANSFORMATION COMPARISON SAMPLE
-- Side-by-side comparison of bronze vs silver for manual review
-- =============================================================================
SELECT
    'Sample Transformation Comparison' AS note,
    b.sls_ord_num,
    b.sls_prd_key,
    b.sls_order_dt AS bronze_order_dt,
    s.sls_order_dt AS silver_order_dt,
    b.sls_price AS bronze_price,
    s.sls_price AS silver_price,
    b.sls_sales AS bronze_sales,
    s.sls_sales AS silver_sales,
    b.sls_quantity,
    CASE
        WHEN b.sls_order_dt = 0
        OR LENGTH(CAST(b.sls_order_dt AS VARCHAR)) != 8 THEN 'Date Issue'
        WHEN b.sls_sales != b.sls_quantity * ABS(b.sls_price) THEN 'Sales Rule Issue'
        WHEN b.sls_price < 0
        OR b.sls_sales < 0 THEN 'Negative Values'
        ELSE 'Clean Record'
    END AS transformation_reason
FROM
    bronze.crm_sales_details b
    JOIN silver.crm_sales_details s ON b.sls_ord_num = s.sls_ord_num
    AND b.sls_prd_key = s.sls_prd_key
    AND b.sls_cust_id = s.sls_cust_id
ORDER BY
    CASE
        WHEN b.sls_order_dt = 0
        OR LENGTH(CAST(b.sls_order_dt AS VARCHAR)) != 8 THEN 1
        WHEN b.sls_sales != b.sls_quantity * ABS(b.sls_price) THEN 2
        WHEN b.sls_price < 0
        OR b.sls_sales < 0 THEN 3
        ELSE 4
    END,
    b.sls_ord_num
LIMIT
    25
;


-- 13. COMPREHENSIVE VALIDATION SUMMARY
-- Overall summary of transformation results
-- =============================================================================
SELECT
    'VALIDATION SUMMARY REPORT' AS report_type,
    COUNT(*) AS total_sales_records,
    COUNT(DISTINCT sls_ord_num) AS unique_orders,
    COUNT(DISTINCT sls_prd_key) AS unique_products,
    COUNT(DISTINCT sls_cust_id) AS unique_customers,
    MIN(sls_order_dt) AS earliest_order_date,
    MAX(sls_order_dt) AS latest_order_date,
    COUNT(
        CASE
            WHEN sls_order_dt IS NULL THEN 1
        END
    ) AS null_order_dates,
    COUNT(
        CASE
            WHEN sls_ship_dt IS NULL THEN 1
        END
    ) AS null_ship_dates,
    COUNT(
        CASE
            WHEN sls_due_dt IS NULL THEN 1
        END
    ) AS null_due_dates,
    ROUND(AVG(sls_sales), 2) AS avg_sales_amount,
    ROUND(AVG(sls_price), 2) AS avg_price,
    ROUND(AVG(sls_quantity), 2) AS avg_quantity,
    SUM(sls_sales) AS total_sales_value,
    COUNT(
        CASE
            WHEN sls_sales IS NULL THEN 1
        END
    ) AS null_sales_records,
    COUNT(
        CASE
            WHEN sls_price IS NULL THEN 1
        END
    ) AS null_price_records
FROM
    silver.crm_sales_details
;


-- 14. DATA QUALITY SCORE
-- Calculate overall data quality score for the transformation
-- =============================================================================
SELECT
    'Data Quality Score' AS metric_type,
    ROUND(
        100.0 * (
            COUNT(
                CASE
                    WHEN sls_ord_num IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_prd_key IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_cust_id IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_quantity IS NOT NULL
                    AND sls_quantity > 0 THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_sales IS NOT NULL
                    AND sls_sales > 0 THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_price IS NOT NULL
                    AND sls_price > 0 THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN ABS(sls_sales - (sls_quantity * sls_price)) < 0.01 THEN 1
                END
            )
        ) / (COUNT(*) * 7),
        2
    ) AS data_quality_percentage,
    CASE
        WHEN 100.0 * (
            COUNT(
                CASE
                    WHEN sls_ord_num IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_prd_key IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_cust_id IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_quantity IS NOT NULL
                    AND sls_quantity > 0 THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_sales IS NOT NULL
                    AND sls_sales > 0 THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_price IS NOT NULL
                    AND sls_price > 0 THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN ABS(sls_sales - (sls_quantity * sls_price)) < 0.01 THEN 1
                END
            )
        ) / (COUNT(*) * 7) >= 95 THEN 'EXCELLENT'
        WHEN 100.0 * (
            COUNT(
                CASE
                    WHEN sls_ord_num IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_prd_key IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_cust_id IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_quantity IS NOT NULL
                    AND sls_quantity > 0 THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_sales IS NOT NULL
                    AND sls_sales > 0 THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_price IS NOT NULL
                    AND sls_price > 0 THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN ABS(sls_sales - (sls_quantity * sls_price)) < 0.01 THEN 1
                END
            )
        ) / (COUNT(*) * 7) >= 85 THEN 'GOOD'
        WHEN 100.0 * (
            COUNT(
                CASE
                    WHEN sls_ord_num IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_prd_key IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_cust_id IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_quantity IS NOT NULL
                    AND sls_quantity > 0 THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_sales IS NOT NULL
                    AND sls_sales > 0 THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN sls_price IS NOT NULL
                    AND sls_price > 0 THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN ABS(sls_sales - (sls_quantity * sls_price)) < 0.01 THEN 1
                END
            )
        ) / (COUNT(*) * 7) >= 70 THEN 'ACCEPTABLE'
        ELSE 'NEEDS IMPROVEMENT'
    END AS quality_rating
FROM
    silver.crm_sales_details
;