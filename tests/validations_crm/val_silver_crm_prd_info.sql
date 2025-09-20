------------------------------------
-- crm_prd_info
------------------------------------
-- =============================================================================
-- DATA VALIDATION QUERIES FOR SILVER LAYER TRANSFORMATION
-- =============================================================================
-- 1. RECORD COUNT VALIDATION
-- Ensure no records were lost or duplicated during transformation
-- =============================================================================
SELECT
    'Record Count Check' AS validation_type,
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
            bronze.crm_prd_info
    ) bronze
    CROSS JOIN (
        SELECT
            COUNT(*) AS silver_count
        FROM
            silver.crm_prd_info
    ) silver
;


-- 2. CAT_ID TRANSFORMATION VALIDATION
-- Verify cat_id is correctly extracted and transformed
-- =============================================================================
SELECT
    'cat_id Transformation Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN LENGTH(cat_id) = 5 THEN 1
        END
    ) AS correct_length_count,
    COUNT(
        CASE
            WHEN cat_id LIKE '%-%' THEN 1
        END
    ) AS contains_dash_count,
    CASE
        WHEN COUNT(
            CASE
                WHEN cat_id LIKE '%-%' THEN 1
            END
        ) = 0
        AND COUNT(
            CASE
                WHEN LENGTH(cat_id) = 5 THEN 1
            END
        ) = COUNT(*) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.crm_prd_info
;


-- Sample cat_id values for manual inspection
SELECT DISTINCT
    original_prd_key,
    cat_id,
    'Sample cat_id values' AS note
FROM
    (
        SELECT
            b.prd_key AS original_prd_key,
            s.cat_id,
            ROW_NUMBER() OVER (
                ORDER BY
                    s.prd_id
            ) AS rn
        FROM
            bronze.crm_prd_info b
            JOIN silver.crm_prd_info s ON b.prd_id = s.prd_id
    ) sample
WHERE
    rn <= 10
;


-- 3. PRD_KEY TRANSFORMATION VALIDATION
-- Verify new prd_key is correctly extracted (substring from position 7)
-- =============================================================================
SELECT
    'prd_key Transformation Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN s.prd_key = SUBSTRING(b.prd_key, 7, LENGTH(b.prd_key)) THEN 1
        END
    ) AS correct_transformation_count,
    CASE
        WHEN COUNT(
            CASE
                WHEN s.prd_key = SUBSTRING(b.prd_key, 7, LENGTH(b.prd_key)) THEN 1
            END
        ) = COUNT(*) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.crm_prd_info b
    JOIN silver.crm_prd_info s ON b.prd_id = s.prd_id
;


-- 4. PRD_COST NULL HANDLING VALIDATION
-- Verify NULL values in prd_cost are replaced with 0
-- =============================================================================
SELECT
    'prd_cost NULL Handling Check' AS validation_type,
    COUNT(
        CASE
            WHEN b.prd_cost IS NULL THEN 1
        END
    ) AS bronze_null_count,
    COUNT(
        CASE
            WHEN s.prd_cost IS NULL THEN 1
        END
    ) AS silver_null_count,
    COUNT(
        CASE
            WHEN b.prd_cost IS NULL
            AND s.prd_cost = 0 THEN 1
        END
    ) AS correctly_transformed_nulls,
    CASE
        WHEN COUNT(
            CASE
                WHEN s.prd_cost IS NULL THEN 1
            END
        ) = 0
        AND COUNT(
            CASE
                WHEN b.prd_cost IS NULL
                AND s.prd_cost = 0 THEN 1
            END
        ) = COUNT(
            CASE
                WHEN b.prd_cost IS NULL THEN 1
            END
        ) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.crm_prd_info b
    JOIN silver.crm_prd_info s ON b.prd_id = s.prd_id
;


-- 5. PRD_LINE CASE TRANSFORMATION VALIDATION
-- Verify abbreviated values are correctly expanded
-- =============================================================================
SELECT
    'prd_line Transformation Check' AS validation_type,
    bronze_value,
    expected_silver_value,
    actual_count,
    CASE
        WHEN actual_count = expected_count THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    (
        SELECT
            UPPER(TRIM(b.prd_line)) AS bronze_value,
            CASE UPPER(TRIM(b.prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END AS expected_silver_value,
            COUNT(*) AS expected_count,
            SUM(
                CASE
                    WHEN s.prd_line = CASE UPPER(TRIM(b.prd_line))
                        WHEN 'M' THEN 'Mountain'
                        WHEN 'R' THEN 'Road'
                        WHEN 'S' THEN 'Other Sales'
                        WHEN 'T' THEN 'Touring'
                        ELSE 'N/A'
                    END THEN 1
                    ELSE 0
                END
            ) AS actual_count
        FROM
            bronze.crm_prd_info b
            JOIN silver.crm_prd_info s ON b.prd_id = s.prd_id
        GROUP BY
            UPPER(TRIM(b.prd_line))
    ) validation
;


-- Distribution of prd_line values for review
SELECT
    prd_line,
    COUNT(*) AS COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM
    silver.crm_prd_info
GROUP BY
    prd_line
ORDER BY
    COUNT DESC
;


-- 6. PRD_END_DT LEAD FUNCTION VALIDATION
-- Verify end dates are correctly calculated using LEAD function
-- =============================================================================
WITH
    date_validation AS (
        SELECT
            prd_id,
            prd_key,
            prd_start_dt,
            prd_end_dt,
            LEAD(prd_start_dt, 1) OVER (
                PARTITION BY
                    prd_key
                ORDER BY
                    prd_start_dt
            ) AS next_start_dt,
            CASE
                WHEN LEAD(prd_start_dt, 1) OVER (
                    PARTITION BY
                        prd_key
                    ORDER BY
                        prd_start_dt
                ) IS NOT NULL THEN CAST(
                    LEAD(prd_start_dt, 1) OVER (
                        PARTITION BY
                            prd_key
                        ORDER BY
                            prd_start_dt
                    ) - INTERVAL '1 day' AS DATE
                )
                ELSE NULL
            END AS expected_end_dt
        FROM
            silver.crm_prd_info
    )
SELECT
    'prd_end_dt Calculation Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN prd_end_dt = expected_end_dt
            OR (
                prd_end_dt IS NULL
                AND expected_end_dt IS NULL
            ) THEN 1
        END
    ) AS correct_calculations,
    CASE
        WHEN COUNT(
            CASE
                WHEN prd_end_dt = expected_end_dt
                OR (
                    prd_end_dt IS NULL
                    AND expected_end_dt IS NULL
                ) THEN 1
            END
        ) = COUNT(*) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    date_validation
;


-- 7. DATA INTEGRITY CHECKS
-- Additional checks for data quality
-- =============================================================================
-- Check for duplicate prd_id values
SELECT
    'Duplicate prd_id Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT prd_id) AS unique_prd_ids,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT prd_id) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.crm_prd_info
;


-- Check for missing critical values
SELECT
    'Missing Critical Values Check' AS validation_type,
    COUNT(
        CASE
            WHEN prd_id IS NULL THEN 1
        END
    ) AS null_prd_id,
    COUNT(
        CASE
            WHEN prd_key IS NULL
            OR prd_key = '' THEN 1
        END
    ) AS null_empty_prd_key,
    COUNT(
        CASE
            WHEN cat_id IS NULL
            OR cat_id = '' THEN 1
        END
    ) AS null_empty_cat_id,
    COUNT(
        CASE
            WHEN prd_nm IS NULL
            OR prd_nm = '' THEN 1
        END
    ) AS null_empty_prd_nm,
    CASE
        WHEN COUNT(
            CASE
                WHEN prd_id IS NULL THEN 1
            END
        ) = 0
        AND COUNT(
            CASE
                WHEN prd_key IS NULL
                OR prd_key = '' THEN 1
            END
        ) = 0
        AND COUNT(
            CASE
                WHEN cat_id IS NULL
                OR cat_id = '' THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.crm_prd_info
;


-- 8. SAMPLE DATA COMPARISON
-- Side-by-side comparison of bronze vs silver for manual review
-- =============================================================================
SELECT
    'Sample Data Comparison' AS note,
    b.prd_id,
    b.prd_key AS bronze_prd_key,
    s.cat_id AS silver_cat_id,
    s.prd_key AS silver_prd_key,
    b.prd_cost AS bronze_prd_cost,
    s.prd_cost AS silver_prd_cost,
    b.prd_line AS bronze_prd_line,
    s.prd_line AS silver_prd_line,
    b.prd_start_dt,
    s.prd_end_dt AS silver_prd_end_dt
FROM
    bronze.crm_prd_info b
    JOIN silver.crm_prd_info s ON b.prd_id = s.prd_id
ORDER BY
    b.prd_id
LIMIT
    20
;


-- 9. SUMMARY VALIDATION REPORT
-- Overall summary of all validation checks
-- =============================================================================
SELECT
    'VALIDATION SUMMARY' AS report_type,
    COUNT(*) AS total_records_processed,
    MIN(prd_start_dt) AS earliest_start_date,
    MAX(prd_start_dt) AS latest_start_date,
    COUNT(DISTINCT prd_key) AS unique_products,
    COUNT(DISTINCT cat_id) AS unique_categories,
    AVG(prd_cost) AS avg_product_cost,
    COUNT(
        CASE
            WHEN prd_end_dt IS NULL THEN 1
        END
    ) AS records_without_end_date
FROM
    silver.crm_prd_info
;