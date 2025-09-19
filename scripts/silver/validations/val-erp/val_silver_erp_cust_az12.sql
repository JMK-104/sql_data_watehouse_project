-- =============================================================================
-- DATA VALIDATION QUERIES FOR ERP_CUST_AZ12 SILVER LAYER TRANSFORMATION
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
            bronze.erp_cust_az12
    ) bronze
    CROSS JOIN (
        SELECT
            COUNT(*) AS silver_count
        FROM
            silver.erp_cust_az12
    ) silver;

-- =============================================================================
-- 2. CUSTOMER ID (CID) TRANSFORMATION VALIDATION
-- Verify NAS prefix removal logic
-- =============================================================================
SELECT
    'Customer ID Transformation Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN b.cid LIKE 'NAS%' THEN 1
        END
    ) AS bronze_nas_prefixed,
    COUNT(
        CASE
            WHEN b.cid NOT LIKE 'NAS%' THEN 1
        END
    ) AS bronze_non_nas,
    COUNT(
        CASE
            WHEN s.cid LIKE 'NAS%' THEN 1
        END
    ) AS silver_nas_prefixed,
    -- Validate transformation logic
    COUNT(
        CASE
            WHEN b.cid LIKE 'NAS%' AND s.cid = SUBSTRING(b.cid, 4, LENGTH(b.cid)) THEN 1
            WHEN b.cid NOT LIKE 'NAS%' AND s.cid = b.cid THEN 1
        END
    ) AS correct_transformations,
    CASE
        WHEN COUNT(
            CASE
                WHEN b.cid LIKE 'NAS%' AND s.cid = SUBSTRING(b.cid, 4, LENGTH(b.cid)) THEN 1
                WHEN b.cid NOT LIKE 'NAS%' AND s.cid = b.cid THEN 1
            END
        ) = COUNT(*) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.erp_cust_az12 b
    JOIN silver.erp_cust_az12 s ON 
        CASE 
            WHEN b.cid LIKE 'NAS%' THEN SUBSTRING(b.cid, 4, LENGTH(b.cid))
            ELSE b.cid 
        END = s.cid;

-- Sample of CID transformations for manual review
SELECT
    'Sample CID Transformations' AS note,
    b.cid AS bronze_cid,
    s.cid AS silver_cid,
    CASE
        WHEN b.cid LIKE 'NAS%' THEN 'NAS Prefix Removed'
        ELSE 'No Change Required'
    END AS transformation_type,
    LENGTH(b.cid) AS bronze_cid_length,
    LENGTH(s.cid) AS silver_cid_length
FROM
    bronze.erp_cust_az12 b
    JOIN silver.erp_cust_az12 s ON 
        CASE 
            WHEN b.cid LIKE 'NAS%' THEN SUBSTRING(b.cid, 4, LENGTH(b.cid))
            ELSE b.cid 
        END = s.cid
WHERE
    b.cid LIKE 'NAS%'
LIMIT 20;

-- =============================================================================
-- 3. BIRTH DATE (BDATE) TRANSFORMATION VALIDATION
-- Verify future date conversion to NULL
-- =============================================================================
SELECT
    'Birth Date Transformation Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN b.bdate > CURRENT_DATE THEN 1
        END
    ) AS bronze_future_dates,
    COUNT(
        CASE
            WHEN b.bdate <= CURRENT_DATE THEN 1
        END
    ) AS bronze_valid_dates,
    COUNT(
        CASE
            WHEN b.bdate IS NULL THEN 1
        END
    ) AS bronze_null_dates,
    COUNT(
        CASE
            WHEN s.bdate IS NULL THEN 1
        END
    ) AS silver_null_dates,
    COUNT(
        CASE
            WHEN s.bdate IS NOT NULL THEN 1
        END
    ) AS silver_valid_dates,
    -- Validate that all future dates in bronze become NULL in silver
    COUNT(
        CASE
            WHEN b.bdate > CURRENT_DATE AND s.bdate IS NOT NULL THEN 1
        END
    ) AS future_dates_not_nullified,
    CASE
        WHEN COUNT(
            CASE
                WHEN b.bdate > CURRENT_DATE AND s.bdate IS NOT NULL THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.erp_cust_az12 b
    JOIN silver.erp_cust_az12 s ON 
        CASE 
            WHEN b.cid LIKE 'NAS%' THEN SUBSTRING(b.cid, 4, LENGTH(b.cid))
            ELSE b.cid 
        END = s.cid;

-- Sample of date transformations for manual review
SELECT
    'Sample Date Transformations' AS note,
    b.cid AS bronze_cid,
    b.bdate AS bronze_bdate,
    s.bdate AS silver_bdate,
    CURRENT_DATE AS current_date,
    CASE
        WHEN b.bdate > CURRENT_DATE THEN 'Future Date - Should be NULL'
        WHEN b.bdate IS NULL THEN 'Already NULL'
        ELSE 'Valid Date - No Change'
    END AS transformation_reason
FROM
    bronze.erp_cust_az12 b
    JOIN silver.erp_cust_az12 s ON 
        CASE 
            WHEN b.cid LIKE 'NAS%' THEN SUBSTRING(b.cid, 4, LENGTH(b.cid))
            ELSE b.cid 
        END = s.cid
WHERE
    b.bdate > CURRENT_DATE
    OR b.bdate IS NULL
    OR s.bdate IS NULL
LIMIT 20;

-- =============================================================================
-- 4. GENDER (GEN) TRANSFORMATION VALIDATION
-- Verify standardization of gender values
-- =============================================================================
SELECT
    'Gender Transformation Check' AS validation_type,
    COUNT(*) AS total_records,
    -- Bronze gender distribution
    COUNT(CASE WHEN LOWER(TRIM(b.gen)) = 'm' THEN 1 END) AS bronze_m_count,
    COUNT(CASE WHEN LOWER(TRIM(b.gen)) = 'male' THEN 1 END) AS bronze_male_count,
    COUNT(CASE WHEN LOWER(TRIM(b.gen)) = 'f' THEN 1 END) AS bronze_f_count,
    COUNT(CASE WHEN LOWER(TRIM(b.gen)) = 'female' THEN 1 END) AS bronze_female_count,
    COUNT(CASE WHEN LOWER(TRIM(b.gen)) NOT IN ('m', 'male', 'f', 'female') OR b.gen IS NULL THEN 1 END) AS bronze_other_null,
    -- Silver gender distribution
    COUNT(CASE WHEN s.gen = 'Male' THEN 1 END) AS silver_male_count,
    COUNT(CASE WHEN s.gen = 'Female' THEN 1 END) AS silver_female_count,
    COUNT(CASE WHEN s.gen = 'N/A' THEN 1 END) AS silver_na_count,
    -- Validation check
    CASE
        WHEN (COUNT(CASE WHEN LOWER(TRIM(b.gen)) IN ('m', 'male') THEN 1 END) = COUNT(CASE WHEN s.gen = 'Male' THEN 1 END))
        AND (COUNT(CASE WHEN LOWER(TRIM(b.gen)) IN ('f', 'female') THEN 1 END) = COUNT(CASE WHEN s.gen = 'Female' THEN 1 END))
        AND (COUNT(CASE WHEN LOWER(TRIM(b.gen)) NOT IN ('m', 'male', 'f', 'female') OR b.gen IS NULL THEN 1 END) = COUNT(CASE WHEN s.gen = 'N/A' THEN 1 END))
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.erp_cust_az12 b
    JOIN silver.erp_cust_az12 s ON 
        CASE 
            WHEN b.cid LIKE 'NAS%' THEN SUBSTRING(b.cid, 4, LENGTH(b.cid))
            ELSE b.cid 
        END = s.cid;

-- Detailed gender transformation validation
SELECT
    'Gender Transformation Details' AS validation_type,
    b.gen AS bronze_gender,
    LOWER(TRIM(b.gen)) AS bronze_normalized,
    s.gen AS silver_gender,
    COUNT(*) AS record_count,
    CASE
        WHEN LOWER(TRIM(b.gen)) IN ('m', 'male') AND s.gen = 'Male' THEN 'CORRECT'
        WHEN LOWER(TRIM(b.gen)) IN ('f', 'female') AND s.gen = 'Female' THEN 'CORRECT'
        WHEN (LOWER(TRIM(b.gen)) NOT IN ('m', 'male', 'f', 'female') OR b.gen IS NULL) AND s.gen = 'N/A' THEN 'CORRECT'
        ELSE 'INCORRECT'
    END AS transformation_status
FROM
    bronze.erp_cust_az12 b
    JOIN silver.erp_cust_az12 s ON 
        CASE 
            WHEN b.cid LIKE 'NAS%' THEN SUBSTRING(b.cid, 4, LENGTH(b.cid))
            ELSE b.cid 
        END = s.cid
GROUP BY
    b.gen, LOWER(TRIM(b.gen)), s.gen
ORDER BY
    record_count DESC;

-- =============================================================================
-- 5. DATA COMPLETENESS VALIDATION
-- Check for missing critical values after transformation
-- =============================================================================
SELECT
    'Data Completeness Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN s.cid IS NULL THEN 1 END) AS null_customer_ids,
    COUNT(CASE WHEN s.bdate IS NULL THEN 1 END) AS null_birth_dates,
    COUNT(CASE WHEN s.gen IS NULL THEN 1 END) AS null_genders,
    COUNT(CASE WHEN s.gen = 'N/A' THEN 1 END) AS na_genders,
    COUNT(CASE WHEN s.cid IS NOT NULL THEN 1 END) AS valid_customer_ids,
    COUNT(CASE WHEN s.bdate IS NOT NULL THEN 1 END) AS valid_birth_dates,
    COUNT(CASE WHEN s.gen IN ('Male', 'Female') THEN 1 END) AS valid_genders,
    CASE
        WHEN COUNT(CASE WHEN s.cid IS NULL THEN 1 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.erp_cust_az12 s;

-- =============================================================================
-- 6. DUPLICATE CUSTOMER ID CHECK
-- Ensure no duplicate customer IDs after transformation
-- =============================================================================
SELECT
    'Duplicate Customer ID Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT cid) AS unique_customer_ids,
    COUNT(*) - COUNT(DISTINCT cid) AS duplicate_count,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT cid) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.erp_cust_az12;

-- Show duplicate customer IDs if any exist
SELECT
    'Duplicate Customer IDs' AS note,
    cid,
    COUNT(*) AS occurrence_count
FROM
    silver.erp_cust_az12
GROUP BY
    cid
HAVING
    COUNT(*) > 1
ORDER BY
    occurrence_count DESC
LIMIT 10;

-- =============================================================================
-- 7. LOGICAL DATE VALIDATION
-- Verify birth dates are reasonable (not too old, not in future)
-- =============================================================================
SELECT
    'Birth Date Logic Validation' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN bdate IS NOT NULL THEN 1 END) AS records_with_birth_date,
    COUNT(CASE WHEN bdate > CURRENT_DATE THEN 1 END) AS future_birth_dates,
    COUNT(CASE WHEN bdate < CURRENT_DATE - INTERVAL '150 years' THEN 1 END) AS very_old_dates,
    COUNT(CASE WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, bdate)) BETWEEN 0 AND 120 THEN 1 END) AS reasonable_ages,
    MIN(bdate) AS earliest_birth_date,
    MAX(bdate) AS latest_birth_date,
    CASE
        WHEN COUNT(CASE WHEN bdate > CURRENT_DATE THEN 1 END) = 0
        AND COUNT(CASE WHEN bdate < CURRENT_DATE - INTERVAL '150 years' THEN 1 END) = 0
        THEN 'PASS'
        ELSE 'REVIEW'
    END AS status
FROM
    silver.erp_cust_az12;

-- =============================================================================
-- 8. TRANSFORMATION COMPARISON SAMPLE
-- Side-by-side comparison of bronze vs silver for manual review
-- =============================================================================
SELECT
    'Sample Transformation Comparison' AS note,
    b.cid AS bronze_cid,
    s.cid AS silver_cid,
    b.bdate AS bronze_bdate,
    s.bdate AS silver_bdate,
    b.gen AS bronze_gender,
    s.gen AS silver_gender,
    CASE
        WHEN b.cid LIKE 'NAS%' THEN 'CID: NAS Removed'
        ELSE ''
    END ||
    CASE
        WHEN b.bdate > CURRENT_DATE THEN ' | BDATE: Future Date Nullified'
        ELSE ''
    END ||
    CASE
        WHEN LOWER(TRIM(b.gen)) IN ('m', 'f') THEN ' | GEN: Single Letter Expanded'
        WHEN LOWER(TRIM(b.gen)) NOT IN ('m', 'male', 'f', 'female') OR b.gen IS NULL THEN ' | GEN: Invalid to N/A'
        ELSE ''
    END AS transformation_notes
FROM
    bronze.erp_cust_az12 b
    JOIN silver.erp_cust_az12 s ON 
        CASE 
            WHEN b.cid LIKE 'NAS%' THEN SUBSTRING(b.cid, 4, LENGTH(b.cid))
            ELSE b.cid 
        END = s.cid
ORDER BY
    CASE
        WHEN b.cid LIKE 'NAS%' THEN 1
        WHEN b.bdate > CURRENT_DATE THEN 2
        WHEN LOWER(TRIM(b.gen)) IN ('m', 'f') THEN 3
        ELSE 4
    END
LIMIT 25;

-- =============================================================================
-- 9. COMPREHENSIVE VALIDATION SUMMARY
-- Overall summary of transformation results
-- =============================================================================
SELECT
    'VALIDATION SUMMARY REPORT' AS report_type,
    COUNT(*) AS total_customer_records,
    COUNT(DISTINCT cid) AS unique_customers,
    MIN(bdate) AS earliest_birth_date,
    MAX(bdate) AS latest_birth_date,
    COUNT(CASE WHEN bdate IS NULL THEN 1 END) AS null_birth_dates,
    COUNT(CASE WHEN gen = 'Male' THEN 1 END) AS male_customers,
    COUNT(CASE WHEN gen = 'Female' THEN 1 END) AS female_customers,
    COUNT(CASE WHEN gen = 'N/A' THEN 1 END) AS unspecified_gender,
    CAST(AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, bdate))) AS INTEGER) AS avg_customer_age,
    COUNT(CASE WHEN LENGTH(cid) <= 3 THEN 1 END) AS short_customer_ids
FROM
    silver.erp_cust_az12;

-- =============================================================================
-- 10. DATA QUALITY SCORE
-- Calculate overall data quality score for the transformation
-- =============================================================================
SELECT
    'Data Quality Score' AS metric_type,
    ROUND(
        100.0 * (
            COUNT(CASE WHEN cid IS NOT NULL AND LENGTH(cid) > 0 THEN 1 END) +
            COUNT(CASE WHEN bdate IS NOT NULL AND bdate <= CURRENT_DATE THEN 1 END) +
            COUNT(CASE WHEN gen IN ('Male', 'Female', 'N/A') THEN 1 END)
        ) / (COUNT(*) * 3),
        2
    ) AS data_quality_percentage,
    CASE
        WHEN 100.0 * (
            COUNT(CASE WHEN cid IS NOT NULL AND LENGTH(cid) > 0 THEN 1 END) +
            COUNT(CASE WHEN bdate IS NOT NULL AND bdate <= CURRENT_DATE THEN 1 END) +
            COUNT(CASE WHEN gen IN ('Male', 'Female', 'N/A') THEN 1 END)
        ) / (COUNT(*) * 3) >= 95 THEN 'EXCELLENT'
        WHEN 100.0 * (
            COUNT(CASE WHEN cid IS NOT NULL AND LENGTH(cid) > 0 THEN 1 END) +
            COUNT(CASE WHEN bdate IS NOT NULL AND bdate <= CURRENT_DATE THEN 1 END) +
            COUNT(CASE WHEN gen IN ('Male', 'Female', 'N/A') THEN 1 END)
        ) / (COUNT(*) * 3) >= 85 THEN 'GOOD'
        WHEN 100.0 * (
            COUNT(CASE WHEN cid IS NOT NULL AND LENGTH(cid) > 0 THEN 1 END) +
            COUNT(CASE WHEN bdate IS NOT NULL AND bdate <= CURRENT_DATE THEN 1 END) +
            COUNT(CASE WHEN gen IN ('Male', 'Female', 'N/A') THEN 1 END)
        ) / (COUNT(*) * 3) >= 70 THEN 'ACCEPTABLE'
        ELSE 'NEEDS IMPROVEMENT'
    END AS quality_rating
FROM
    silver.erp_cust_az12;
