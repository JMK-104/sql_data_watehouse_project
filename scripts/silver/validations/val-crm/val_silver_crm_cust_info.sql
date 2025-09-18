-- =============================================================================
-- DATA VALIDATION QUERIES FOR CUSTOMER SILVER LAYER TRANSFORMATION
-- =============================================================================
-- 1. RECORD COUNT AND DEDUPLICATION VALIDATION
-- Ensure duplicates are removed and NULL cst_id records are filtered out
-- =============================================================================
SELECT
    'Record Count and Deduplication Check' AS validation_type,
    bronze_total,
    bronze_non_null,
    bronze_unique_ids,
    silver_count,
    CASE
        WHEN silver_count = bronze_unique_ids THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    (
        SELECT
            COUNT(*) AS bronze_total,
            COUNT(
                CASE
                    WHEN cst_id IS NOT NULL THEN 1
                END
            ) AS bronze_non_null,
            COUNT(DISTINCT cst_id) AS bronze_unique_ids
        FROM
            bronze.crm_cust_info
        WHERE
            cst_id IS NOT NULL
    ) bronze
    CROSS JOIN (
        SELECT
            COUNT(*) AS silver_count
        FROM
            silver.crm_cust_info -- Adjust table name as needed
    ) silver
;


-- Verify no duplicate cst_id values in silver layer
SELECT
    'Duplicate cst_id Check in Silver' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT cst_id) AS unique_cst_ids,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT cst_id) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.crm_cust_info
;


-- 2. NULL CST_ID FILTERING VALIDATION
-- Verify all NULL cst_id records are filtered out
-- =============================================================================
SELECT
    'NULL cst_id Filtering Check' AS validation_type,
    COUNT(
        CASE
            WHEN cst_id IS NULL THEN 1
        END
    ) AS null_cst_id_count,
    CASE
        WHEN COUNT(
            CASE
                WHEN cst_id IS NULL THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.crm_cust_info
;


-- 3. LATEST RECORD SELECTION VALIDATION
-- Verify ROW_NUMBER logic keeps the latest record per cst_id
-- =============================================================================
WITH
    latest_check AS (
        SELECT
            cst_id,
            COUNT(*) AS bronze_records_per_id,
            MAX(cst_create_date) AS latest_create_date
        FROM
            bronze.crm_cust_info
        WHERE
            cst_id IS NOT NULL
        GROUP BY
            cst_id
        HAVING
            COUNT(*) > 1 -- Only check IDs that had duplicates
    ),
    silver_check AS (
        SELECT
            s.cst_id,
            s.cst_create_date
        FROM
            silver.crm_cust_info s
            INNER JOIN latest_check l ON s.cst_id = l.cst_id
    )
SELECT
    'Latest Record Selection Check' AS validation_type,
    COUNT(*) AS total_duplicate_groups_checked,
    COUNT(
        CASE
            WHEN sc.cst_create_date = lc.latest_create_date THEN 1
        END
    ) AS correct_latest_selections,
    CASE
        WHEN COUNT(
            CASE
                WHEN sc.cst_create_date = lc.latest_create_date THEN 1
            END
        ) = COUNT(*) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    latest_check lc
    JOIN silver_check sc ON lc.cst_id = sc.cst_id
;


-- 4. STRING TRIMMING VALIDATION
-- Verify white spaces are removed from string fields
-- =============================================================================
SELECT
    'String Trimming Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN cst_firstname = TRIM(cst_firstname) THEN 1
        END
    ) AS firstname_trimmed_correctly,
    COUNT(
        CASE
            WHEN cst_lastname = TRIM(cst_lastname) THEN 1
        END
    ) AS lastname_trimmed_correctly,
    COUNT(
        CASE
            WHEN cst_firstname LIKE ' %'
            OR cst_firstname LIKE '% ' THEN 1
        END
    ) AS firstname_with_spaces,
    COUNT(
        CASE
            WHEN cst_lastname LIKE ' %'
            OR cst_lastname LIKE '% ' THEN 1
        END
    ) AS lastname_with_spaces,
    CASE
        WHEN COUNT(
            CASE
                WHEN cst_firstname LIKE ' %'
                OR cst_firstname LIKE '% ' THEN 1
            END
        ) = 0
        AND COUNT(
            CASE
                WHEN cst_lastname LIKE ' %'
                OR cst_lastname LIKE '% ' THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.crm_cust_info
;


-- 5. MARITAL STATUS TRANSFORMATION VALIDATION
-- Verify single letter codes are correctly expanded
-- =============================================================================
SELECT
    'Marital Status Transformation Check' AS validation_type,
    bronze_value,
    expected_silver_value,
    actual_count,
    expected_count,
    CASE
        WHEN actual_count = expected_count THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    (
        SELECT
            UPPER(TRIM(b.cst_marital_status)) AS bronze_value,
            CASE UPPER(TRIM(b.cst_marital_status))
                WHEN 'M' THEN 'Married'
                WHEN 'S' THEN 'Single'
                ELSE 'N/A'
            END AS expected_silver_value,
            COUNT(*) AS expected_count,
            SUM(
                CASE
                    WHEN s.cst_marital_status = CASE UPPER(TRIM(b.cst_marital_status))
                        WHEN 'M' THEN 'Married'
                        WHEN 'S' THEN 'Single'
                        ELSE 'N/A'
                    END THEN 1
                    ELSE 0
                END
            ) AS actual_count
        FROM
            bronze.crm_cust_info b
            JOIN silver.crm_cust_info s ON b.cst_id = s.cst_id
            AND b.cst_create_date = s.cst_create_date -- Match on latest record
        WHERE
            b.cst_id IS NOT NULL
        GROUP BY
            UPPER(TRIM(b.cst_marital_status))
    ) validation
;


-- Distribution of marital status values for review
SELECT
    cst_marital_status,
    COUNT(*) AS COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM
    silver.crm_cust_info
GROUP BY
    cst_marital_status
ORDER BY
    COUNT DESC
;


-- 6. GENDER TRANSFORMATION VALIDATION
-- Verify single letter gender codes are correctly expanded
-- =============================================================================
SELECT
    'Gender Transformation Check' AS validation_type,
    bronze_value,
    expected_silver_value,
    actual_count,
    expected_count,
    CASE
        WHEN actual_count = expected_count THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    (
        SELECT
            UPPER(TRIM(b.cst_gndr)) AS bronze_value,
            CASE UPPER(TRIM(b.cst_gndr))
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE 'N/A'
            END AS expected_silver_value,
            COUNT(*) AS expected_count,
            SUM(
                CASE
                    WHEN s.cst_gndr = CASE UPPER(TRIM(b.cst_gndr))
                        WHEN 'F' THEN 'Female'
                        WHEN 'M' THEN 'Male'
                        ELSE 'N/A'
                    END THEN 1
                    ELSE 0
                END
            ) AS actual_count
        FROM
            bronze.crm_cust_info b
            JOIN silver.crm_cust_info s ON b.cst_id = s.cst_id
            AND b.cst_create_date = s.cst_create_date -- Match on latest record
        WHERE
            b.cst_id IS NOT NULL
        GROUP BY
            UPPER(TRIM(b.cst_gndr))
    ) validation
;


-- Distribution of gender values for review
SELECT
    cst_gndr,
    COUNT(*) AS COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM
    silver.crm_cust_info
GROUP BY
    cst_gndr
ORDER BY
    COUNT DESC
;


-- 7. DATA COMPLETENESS VALIDATION
-- Check for missing or empty critical values
-- =============================================================================
SELECT
    'Data Completeness Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN cst_id IS NULL THEN 1
        END
    ) AS null_cst_id,
    COUNT(
        CASE
            WHEN cst_key IS NULL
            OR cst_key = '' THEN 1
        END
    ) AS null_empty_cst_key,
    COUNT(
        CASE
            WHEN cst_firstname IS NULL
            OR cst_firstname = '' THEN 1
        END
    ) AS null_empty_firstname,
    COUNT(
        CASE
            WHEN cst_lastname IS NULL
            OR cst_lastname = '' THEN 1
        END
    ) AS null_empty_lastname,
    COUNT(
        CASE
            WHEN cst_create_date IS NULL THEN 1
        END
    ) AS null_create_date,
    CASE
        WHEN COUNT(
            CASE
                WHEN cst_id IS NULL THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.crm_cust_info
;


-- 8. DUPLICATE DETECTION IN BRONZE SOURCE
-- Identify which cst_ids had duplicates in bronze layer
-- =============================================================================
SELECT
    'Bronze Layer Duplicate Analysis' AS validation_type,
    total_bronze_records,
    unique_cst_ids,
    duplicate_cst_ids,
    ROUND((duplicate_cst_ids * 100.0 / unique_cst_ids), 2) AS duplicate_percentage
FROM
    (
        SELECT
            COUNT(*) AS total_bronze_records,
            COUNT(DISTINCT cst_id) AS unique_cst_ids,
            COUNT(*) - COUNT(DISTINCT cst_id) AS duplicate_records,
            SUM(
                CASE
                    WHEN dup_count > 1 THEN 1
                    ELSE 0
                END
            ) AS duplicate_cst_ids
        FROM
            (
                SELECT
                    cst_id,
                    COUNT(*) AS dup_count
                FROM
                    bronze.crm_cust_info
                WHERE
                    cst_id IS NOT NULL
                GROUP BY
                    cst_id
            ) dup_analysis
    ) summary
;


-- Sample of customers that had duplicates in bronze
SELECT
    'Sample Duplicate Records' AS note,
    cst_id,
    COUNT(*) AS duplicate_count,
    MIN(cst_create_date) AS earliest_date,
    MAX(cst_create_date) AS latest_date
FROM
    bronze.crm_cust_info
WHERE
    cst_id IS NOT NULL
GROUP BY
    cst_id
HAVING
    COUNT(*) > 1
ORDER BY
    duplicate_count DESC,
    cst_id
LIMIT
    10
;


-- 9. TRANSFORMATION ACCURACY SAMPLE
-- Side-by-side comparison of bronze vs silver for manual review
-- =============================================================================
WITH
    bronze_latest AS (
        SELECT
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date,
            ROW_NUMBER() OVER (
                PARTITION BY
                    cst_id
                ORDER BY
                    cst_create_date DESC
            ) AS rn
        FROM
            bronze.crm_cust_info
        WHERE
            cst_id IS NOT NULL
    )
SELECT
    'Sample Transformation Comparison' AS note,
    s.cst_id,
    b.cst_firstname AS bronze_firstname,
    s.cst_firstname AS silver_firstname,
    b.cst_lastname AS bronze_lastname,
    s.cst_lastname AS silver_lastname,
    b.cst_marital_status AS bronze_marital,
    s.cst_marital_status AS silver_marital,
    b.cst_gndr AS bronze_gender,
    s.cst_gndr AS silver_gender,
    s.cst_create_date
FROM
    silver.crm_cust_info s
    JOIN bronze_latest b ON s.cst_id = b.cst_id
    AND b.rn = 1
ORDER BY
    s.cst_id
LIMIT
    20
;


-- 10. DATE CONSISTENCY VALIDATION
-- Verify create dates are preserved correctly
-- =============================================================================
SELECT
    'Date Consistency Check' AS validation_type,
    COUNT(*) AS total_records,
    MIN(cst_create_date) AS earliest_create_date,
    MAX(cst_create_date) AS latest_create_date,
    COUNT(
        CASE
            WHEN cst_create_date IS NULL THEN 1
        END
    ) AS null_dates,
    COUNT(
        CASE
            WHEN cst_create_date > CURRENT_DATE THEN 1
        END
    ) AS future_dates,
    CASE
        WHEN COUNT(
            CASE
                WHEN cst_create_date IS NULL THEN 1
            END
        ) = 0
        AND COUNT(
            CASE
                WHEN cst_create_date > CURRENT_DATE THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'REVIEW'
    END AS status
FROM
    silver.crm_cust_info
;


-- 11. COMPREHENSIVE VALIDATION SUMMARY
-- Overall summary of all validation checks
-- =============================================================================
SELECT
    'VALIDATION SUMMARY REPORT' AS report_type,
    COUNT(*) AS total_customers_processed,
    COUNT(DISTINCT cst_key) AS unique_customer_keys,
    MIN(cst_create_date) AS earliest_customer_date,
    MAX(cst_create_date) AS latest_customer_date,
    COUNT(
        CASE
            WHEN cst_marital_status = 'Married' THEN 1
        END
    ) AS married_customers,
    COUNT(
        CASE
            WHEN cst_marital_status = 'Single' THEN 1
        END
    ) AS single_customers,
    COUNT(
        CASE
            WHEN cst_marital_status = 'N/A' THEN 1
        END
    ) AS unknown_marital_status,
    COUNT(
        CASE
            WHEN cst_gndr = 'Female' THEN 1
        END
    ) AS female_customers,
    COUNT(
        CASE
            WHEN cst_gndr = 'Male' THEN 1
        END
    ) AS male_customers,
    COUNT(
        CASE
            WHEN cst_gndr = 'N/A' THEN 1
        END
    ) AS unknown_gender,
    ROUND(AVG(LENGTH(cst_firstname)), 2) AS avg_firstname_length,
    ROUND(AVG(LENGTH(cst_lastname)), 2) AS avg_lastname_length
FROM
    silver.crm_cust_info
;


-- 12. DATA QUALITY SCORE
-- Calculate overall data quality score
-- =============================================================================
SELECT
    'Data Quality Score' AS metric_type,
    ROUND(
        (
            100.0 * (
                COUNT(
                    CASE
                        WHEN cst_firstname IS NOT NULL
                        AND cst_firstname != '' THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_lastname IS NOT NULL
                        AND cst_lastname != '' THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_marital_status IN ('Married', 'Single') THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_gndr IN ('Female', 'Male') THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_create_date IS NOT NULL THEN 1
                    END
                )
            )
        ) / (COUNT(*) * 5),
        2
    ) AS data_quality_percentage,
    CASE
        WHEN (
            100.0 * (
                COUNT(
                    CASE
                        WHEN cst_firstname IS NOT NULL
                        AND cst_firstname != '' THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_lastname IS NOT NULL
                        AND cst_lastname != '' THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_marital_status IN ('Married', 'Single') THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_gndr IN ('Female', 'Male') THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_create_date IS NOT NULL THEN 1
                    END
                )
            )
        ) / (COUNT(*) * 5) >= 95 THEN 'EXCELLENT'
        WHEN (
            100.0 * (
                COUNT(
                    CASE
                        WHEN cst_firstname IS NOT NULL
                        AND cst_firstname != '' THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_lastname IS NOT NULL
                        AND cst_lastname != '' THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_marital_status IN ('Married', 'Single') THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_gndr IN ('Female', 'Male') THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_create_date IS NOT NULL THEN 1
                    END
                )
            )
        ) / (COUNT(*) * 5) >= 85 THEN 'GOOD'
        WHEN (
            100.0 * (
                COUNT(
                    CASE
                        WHEN cst_firstname IS NOT NULL
                        AND cst_firstname != '' THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_lastname IS NOT NULL
                        AND cst_lastname != '' THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_marital_status IN ('Married', 'Single') THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_gndr IN ('Female', 'Male') THEN 1
                    END
                ) + COUNT(
                    CASE
                        WHEN cst_create_date IS NOT NULL THEN 1
                    END
                )
            )
        ) / (COUNT(*) * 5) >= 70 THEN 'ACCEPTABLE'
        ELSE 'NEEDS IMPROVEMENT'
    END AS quality_rating
FROM
    silver.crm_cust_info
;
