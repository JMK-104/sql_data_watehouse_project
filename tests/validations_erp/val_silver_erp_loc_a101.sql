-- =============================================================================
-- DATA VALIDATION QUERIES FOR ERP_LOC_A101 SILVER LAYER TRANSFORMATION
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
            bronze.erp_loc_a101
    ) bronze
    CROSS JOIN (
        SELECT
            COUNT(*) AS silver_count
        FROM
            silver.erp_loc_a101
    ) silver
;


-- 2. CID HYPHEN REMOVAL VALIDATION
-- Verify that hyphens are correctly removed from CID values
-- =============================================================================
SELECT
    'CID Hyphen Removal Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN b.cid LIKE '%-%' THEN 1
        END
    ) AS bronze_cid_with_hyphens,
    COUNT(
        CASE
            WHEN s.cid LIKE '%-%' THEN 1
        END
    ) AS silver_cid_with_hyphens,
    COUNT(
        CASE
            WHEN b.cid LIKE '%-%'
            AND s.cid = REPLACE(b.cid, '-', '') THEN 1
        END
    ) AS correct_hyphen_removals,
    COUNT(
        CASE
            WHEN b.cid NOT LIKE '%-%'
            AND s.cid = b.cid THEN 1
        END
    ) AS unchanged_cid_values,
    CASE
        WHEN COUNT(
            CASE
                WHEN s.cid LIKE '%-%' THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.erp_loc_a101 b
    JOIN silver.erp_loc_a101 s ON REPLACE(b.cid, '-', '') = s.cid
;


-- Sample of CID transformations for manual review
SELECT
    'Sample CID Transformations' AS note,
    b.cid AS bronze_cid,
    s.cid AS silver_cid,
    REPLACE(b.cid, '-', '') AS expected_cid,
    CASE
        WHEN b.cid LIKE '%-%' THEN 'Hyphen Removed'
        ELSE 'No Change Needed'
    END AS transformation_type,
    CASE
        WHEN s.cid = REPLACE(b.cid, '-', '') THEN 'Correct'
        ELSE 'Incorrect'
    END AS validation_result
FROM
    bronze.erp_loc_a101 b
    JOIN silver.erp_loc_a101 s ON REPLACE(b.cid, '-', '') = s.cid
WHERE
    b.cid LIKE '%-%'
    OR s.cid != REPLACE(b.cid, '-', '')
LIMIT
    20
;


-- 3. COUNTRY CODE TRANSFORMATION VALIDATION
-- Verify country code standardization and normalization
-- =============================================================================
SELECT
    'Country Transformation Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN b.cntry IS NULL
            OR TRIM(b.cntry) = '' THEN 1
        END
    ) AS bronze_null_empty_countries,
    COUNT(
        CASE
            WHEN TRIM(b.cntry) = 'DE' THEN 1
        END
    ) AS bronze_germany_codes,
    COUNT(
        CASE
            WHEN TRIM(b.cntry) IN ('US', 'USA') THEN 1
        END
    ) AS bronze_us_codes,
    COUNT(
        CASE
            WHEN s.cntry = 'N/A' THEN 1
        END
    ) AS silver_na_countries,
    COUNT(
        CASE
            WHEN s.cntry = 'Germany' THEN 1
        END
    ) AS silver_germany_countries,
    COUNT(
        CASE
            WHEN s.cntry = 'United States' THEN 1
        END
    ) AS silver_us_countries,
    -- Validate transformation logic
    CASE
        WHEN COUNT(
            CASE
                WHEN (
                    b.cntry IS NULL
                    OR TRIM(b.cntry) = ''
                )
                AND s.cntry != 'N/A' THEN 1
                WHEN TRIM(b.cntry) = 'DE'
                AND s.cntry != 'Germany' THEN 1
                WHEN TRIM(b.cntry) IN ('US', 'USA')
                AND s.cntry != 'United States' THEN 1
                WHEN TRIM(b.cntry) NOT IN ('', 'DE', 'US', 'USA')
                AND TRIM(b.cntry) IS NOT NULL
                AND s.cntry != TRIM(b.cntry) THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.erp_loc_a101 b
    JOIN silver.erp_loc_a101 s ON REPLACE(b.cid, '-', '') = s.cid
;


-- Sample of country transformations for manual review
SELECT
    'Sample Country Transformations' AS note,
    b.cid,
    b.cntry AS bronze_country,
    LENGTH(b.cntry) AS bronze_country_length,
    s.cntry AS silver_country,
    CASE
        WHEN b.cntry IS NULL
        OR TRIM(b.cntry) = '' THEN 'NULL/Empty → N/A'
        WHEN TRIM(b.cntry) = 'DE' THEN 'DE → Germany'
        WHEN TRIM(b.cntry) IN ('US', 'USA') THEN 'US/USA → United States'
        ELSE 'Trimmed Only'
    END AS transformation_type,
    CASE
        WHEN (
            b.cntry IS NULL
            OR TRIM(b.cntry) = ''
        )
        AND s.cntry = 'N/A' THEN 'Correct'
        WHEN TRIM(b.cntry) = 'DE'
        AND s.cntry = 'Germany' THEN 'Correct'
        WHEN TRIM(b.cntry) IN ('US', 'USA')
        AND s.cntry = 'United States' THEN 'Correct'
        WHEN TRIM(b.cntry) NOT IN ('', 'DE', 'US', 'USA')
        AND TRIM(b.cntry) IS NOT NULL
        AND s.cntry = TRIM(b.cntry) THEN 'Correct'
        ELSE 'Incorrect'
    END AS validation_result
FROM
    bronze.erp_loc_a101 b
    JOIN silver.erp_loc_a101 s ON REPLACE(b.cid, '-', '') = s.cid
ORDER BY
    CASE
        WHEN b.cntry IS NULL
        OR TRIM(b.cntry) = '' THEN 1
        WHEN TRIM(b.cntry) = 'DE' THEN 2
        WHEN TRIM(b.cntry) IN ('US', 'USA') THEN 3
        ELSE 4
    END,
    b.cid
LIMIT
    25
;


-- 4. WHITESPACE HANDLING VALIDATION
-- Verify that leading/trailing spaces are properly trimmed
-- =============================================================================
SELECT
    'Whitespace Handling Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN b.cntry != TRIM(b.cntry) THEN 1
        END
    ) AS bronze_countries_with_whitespace,
    COUNT(
        CASE
            WHEN s.cntry LIKE ' %'
            OR s.cntry LIKE '% ' THEN 1
        END
    ) AS silver_countries_with_whitespace,
    COUNT(
        CASE
            WHEN LENGTH(b.cntry) > LENGTH(TRIM(b.cntry)) THEN 1
        END
    ) AS bronze_records_needing_trim,
    CASE
        WHEN COUNT(
            CASE
                WHEN s.cntry LIKE ' %'
                OR s.cntry LIKE '% ' THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.erp_loc_a101 b
    JOIN silver.erp_loc_a101 s ON REPLACE(b.cid, '-', '') = s.cid
WHERE
    b.cntry IS NOT NULL
;


-- 5. NULL AND EMPTY VALUE HANDLING VALIDATION
-- Verify NULL and empty string handling
-- =============================================================================
SELECT
    'NULL and Empty Handling Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN b.cid IS NULL THEN 1
        END
    ) AS bronze_null_cids,
    COUNT(
        CASE
            WHEN s.cid IS NULL THEN 1
        END
    ) AS silver_null_cids,
    COUNT(
        CASE
            WHEN b.cntry IS NULL THEN 1
        END
    ) AS bronze_null_countries,
    COUNT(
        CASE
            WHEN TRIM(b.cntry) = '' THEN 1
        END
    ) AS bronze_empty_countries,
    COUNT(
        CASE
            WHEN s.cntry IS NULL THEN 1
        END
    ) AS silver_null_countries,
    COUNT(
        CASE
            WHEN s.cntry = 'N/A' THEN 1
        END
    ) AS silver_na_countries,
    COUNT(
        CASE
            WHEN (
                b.cntry IS NULL
                OR TRIM(b.cntry) = ''
            )
            AND s.cntry = 'N/A' THEN 1
        END
    ) AS correct_null_empty_handling,
    CASE
        WHEN COUNT(
            CASE
                WHEN s.cid IS NULL THEN 1
            END
        ) = 0
        AND COUNT(
            CASE
                WHEN s.cntry IS NULL THEN 1
            END
        ) = 0
        AND COUNT(
            CASE
                WHEN (
                    b.cntry IS NULL
                    OR TRIM(b.cntry) = ''
                )
                AND s.cntry != 'N/A' THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.erp_loc_a101 b
    JOIN silver.erp_loc_a101 s ON REPLACE(b.cid, '-', '') = s.cid
;


-- 6. DATA COMPLETENESS VALIDATION
-- Check overall data completeness in silver layer
-- =============================================================================
SELECT
    'Data Completeness Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN cid IS NOT NULL
            AND cid != '' THEN 1
        END
    ) AS complete_cids,
    COUNT(
        CASE
            WHEN cntry IS NOT NULL
            AND cntry != '' THEN 1
        END
    ) AS complete_countries,
    COUNT(
        CASE
            WHEN cid IS NULL
            OR cid = '' THEN 1
        END
    ) AS incomplete_cids,
    COUNT(
        CASE
            WHEN cntry IS NULL
            OR cntry = '' THEN 1
        END
    ) AS incomplete_countries,
    ROUND(
        100.0 * COUNT(
            CASE
                WHEN cid IS NOT NULL
                AND cid != '' THEN 1
            END
        ) / COUNT(*),
        2
    ) AS cid_completeness_percentage,
    ROUND(
        100.0 * COUNT(
            CASE
                WHEN cntry IS NOT NULL
                AND cntry != '' THEN 1
            END
        ) / COUNT(*),
        2
    ) AS country_completeness_percentage,
    CASE
        WHEN COUNT(
            CASE
                WHEN cid IS NULL
                OR cid = '' THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.erp_loc_a101
;


-- 7. UNIQUE CONSTRAINT VALIDATION
-- Verify CID uniqueness after transformation
-- =============================================================================
SELECT
    'CID Uniqueness Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT cid) AS unique_cids,
    COUNT(*) - COUNT(DISTINCT cid) AS duplicate_cids,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT cid) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.erp_loc_a101
;


-- Sample of duplicate CIDs if any exist
SELECT
    'Duplicate CID Analysis' AS note,
    cid,
    COUNT(*) AS occurrence_count,
    STRING_AGG(cntry, ', ') AS associated_countries
FROM
    silver.erp_loc_a101
GROUP BY
    cid
HAVING
    COUNT(*) > 1
ORDER BY
    COUNT(*) DESC
LIMIT
    10
;


-- 8. COUNTRY CODE DISTRIBUTION VALIDATION
-- Analyze the distribution of country values after transformation
-- =============================================================================
SELECT
    'Country Distribution Analysis' AS validation_type,
    cntry AS country_value,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM
    silver.erp_loc_a101
GROUP BY
    cntry
ORDER BY
    COUNT(*) DESC
;


-- 9. TRANSFORMATION COMPARISON SAMPLE
-- Side-by-side comparison of bronze vs silver for manual review
-- =============================================================================
SELECT
    'Sample Transformation Comparison' AS note,
    b.cid AS bronze_cid,
    s.cid AS silver_cid,
    b.cntry AS bronze_country,
    s.cntry AS silver_country,
    CASE
        WHEN b.cid LIKE '%-%' THEN 'CID Hyphen Removal'
        ELSE 'CID No Change'
    END AS cid_transformation,
    CASE
        WHEN b.cntry IS NULL
        OR TRIM(b.cntry) = '' THEN 'Country NULL/Empty → N/A'
        WHEN TRIM(b.cntry) = 'DE' THEN 'Country DE → Germany'
        WHEN TRIM(b.cntry) IN ('US', 'USA') THEN 'Country US/USA → United States'
        WHEN b.cntry != TRIM(b.cntry) THEN 'Country Trimmed'
        ELSE 'Country No Change'
    END AS country_transformation,
    CASE
        WHEN s.cid = REPLACE(b.cid, '-', '')
        AND (
            (
                b.cntry IS NULL
                OR TRIM(b.cntry) = ''
            )
            AND s.cntry = 'N/A'
            OR TRIM(b.cntry) = 'DE'
            AND s.cntry = 'Germany'
            OR TRIM(b.cntry) IN ('US', 'USA')
            AND s.cntry = 'United States'
            OR (
                TRIM(b.cntry) NOT IN ('', 'DE', 'US', 'USA')
                AND TRIM(b.cntry) IS NOT NULL
                AND s.cntry = TRIM(b.cntry)
            )
        ) THEN 'All Transformations Correct'
        ELSE 'Transformation Issue'
    END AS overall_validation
FROM
    bronze.erp_loc_a101 b
    JOIN silver.erp_loc_a101 s ON REPLACE(b.cid, '-', '') = s.cid
ORDER BY
    CASE
        WHEN b.cid LIKE '%-%' THEN 1
        WHEN b.cntry IS NULL
        OR TRIM(b.cntry) = '' THEN 2
        WHEN TRIM(b.cntry) = 'DE' THEN 3
        WHEN TRIM(b.cntry) IN ('US', 'USA') THEN 4
        WHEN b.cntry != TRIM(b.cntry) THEN 5
        ELSE 6
    END,
    b.cid
LIMIT
    30
;


-- 10. COMPREHENSIVE VALIDATION SUMMARY
-- Overall summary of transformation results
-- =============================================================================
SELECT
    'VALIDATION SUMMARY REPORT' AS report_type,
    COUNT(*) AS total_location_records,
    COUNT(DISTINCT cid) AS unique_location_ids,
    COUNT(DISTINCT cntry) AS unique_countries,
    COUNT(
        CASE
            WHEN cntry = 'N/A' THEN 1
        END
    ) AS records_with_na_country,
    COUNT(
        CASE
            WHEN cntry = 'Germany' THEN 1
        END
    ) AS germany_records,
    COUNT(
        CASE
            WHEN cntry = 'United States' THEN 1
        END
    ) AS us_records,
    COUNT(
        CASE
            WHEN cntry NOT IN ('N/A', 'Germany', 'United States') THEN 1
        END
    ) AS other_countries,
    COUNT(
        CASE
            WHEN cid IS NULL
            OR cid = '' THEN 1
        END
    ) AS incomplete_location_ids,
    COUNT(
        CASE
            WHEN cntry IS NULL
            OR cntry = '' THEN 1
        END
    ) AS incomplete_countries
FROM
    silver.erp_loc_a101
;


-- 11. DATA QUALITY SCORE
-- Calculate overall data quality score for the transformation
-- =============================================================================
SELECT
    'Data Quality Score' AS metric_type,
    ROUND(
        100.0 * (
            COUNT(
                CASE
                    WHEN cid IS NOT NULL
                    AND cid != '' THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN cntry IS NOT NULL
                    AND cntry != '' THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN cid NOT LIKE '%-%' THEN 1
                END
            )
        ) / (COUNT(*) * 3),
        2
    ) AS data_quality_percentage,
    CASE
        WHEN 100.0 * (
            COUNT(
                CASE
                    WHEN cid IS NOT NULL
                    AND cid != '' THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN cntry IS NOT NULL
                    AND cntry != '' THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN cid NOT LIKE '%-%' THEN 1
                END
            )
        ) / (COUNT(*) * 3) >= 95 THEN 'EXCELLENT'
        WHEN 100.0 * (
            COUNT(
                CASE
                    WHEN cid IS NOT NULL
                    AND cid != '' THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN cntry IS NOT NULL
                    AND cntry != '' THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN cid NOT LIKE '%-%' THEN 1
                END
            )
        ) / (COUNT(*) * 3) >= 85 THEN 'GOOD'
        WHEN 100.0 * (
            COUNT(
                CASE
                    WHEN cid IS NOT NULL
                    AND cid != '' THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN cntry IS NOT NULL
                    AND cntry != '' THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN cid NOT LIKE '%-%' THEN 1
                END
            )
        ) / (COUNT(*) * 3) >= 70 THEN 'ACCEPTABLE'
        ELSE 'NEEDS IMPROVEMENT'
    END AS quality_rating
FROM
    silver.erp_loc_a101
;