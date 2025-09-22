-- =============================================================================
-- DATA VALIDATION QUERIES FOR ERP_PX_CAT_G1V2 SILVER LAYER TRANSFORMATION
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
            bronze.erp_px_cat_g1v2
    ) bronze
    CROSS JOIN (
        SELECT
            COUNT(*) AS silver_count
        FROM
            silver.erp_px_cat_g1v2
    ) silver
;


-- 2. DATA INTEGRITY VALIDATION
-- Verify that all data is exactly preserved (no transformations applied)
-- =============================================================================
SELECT
    'Data Integrity Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN b.id = s.id
            AND (
                b.cat = s.cat
                OR (
                    b.cat IS NULL
                    AND s.cat IS NULL
                )
            )
            AND (
                b.subcat = s.subcat
                OR (
                    b.subcat IS NULL
                    AND s.subcat IS NULL
                )
            )
            AND (
                b.maintenance = s.maintenance
                OR (
                    b.maintenance IS NULL
                    AND s.maintenance IS NULL
                )
            ) THEN 1
        END
    ) AS matching_records,
    COUNT(
        CASE
            WHEN b.id != s.id
            OR (
                b.cat != s.cat
                AND NOT (
                    b.cat IS NULL
                    AND s.cat IS NULL
                )
            )
            OR (
                b.subcat != s.subcat
                AND NOT (
                    b.subcat IS NULL
                    AND s.subcat IS NULL
                )
            )
            OR (
                b.maintenance != s.maintenance
                AND NOT (
                    b.maintenance IS NULL
                    AND s.maintenance IS NULL
                )
            ) THEN 1
        END
    ) AS non_matching_records,
    CASE
        WHEN COUNT(
            CASE
                WHEN b.id = s.id
                AND (
                    b.cat = s.cat
                    OR (
                        b.cat IS NULL
                        AND s.cat IS NULL
                    )
                )
                AND (
                    b.subcat = s.subcat
                    OR (
                        b.subcat IS NULL
                        AND s.subcat IS NULL
                    )
                )
                AND (
                    b.maintenance = s.maintenance
                    OR (
                        b.maintenance IS NULL
                        AND s.maintenance IS NULL
                    )
                ) THEN 1
            END
        ) = COUNT(*) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    bronze.erp_px_cat_g1v2 b
    JOIN silver.erp_px_cat_g1v2 s ON b.id = s.id
;


-- Sample of any data mismatches for manual review
SELECT
    'Sample Data Mismatches' AS note,
    b.id,
    b.cat AS bronze_cat,
    s.cat AS silver_cat,
    b.subcat AS bronze_subcat,
    s.subcat AS silver_subcat,
    b.maintenance AS bronze_maintenance,
    s.maintenance AS silver_maintenance,
    CASE
        WHEN b.id != s.id THEN 'ID Mismatch'
        WHEN b.cat != s.cat
        AND NOT (
            b.cat IS NULL
            AND s.cat IS NULL
        ) THEN 'Category Mismatch'
        WHEN b.subcat != s.subcat
        AND NOT (
            b.subcat IS NULL
            AND s.subcat IS NULL
        ) THEN 'Subcategory Mismatch'
        WHEN b.maintenance != s.maintenance
        AND NOT (
            b.maintenance IS NULL
            AND s.maintenance IS NULL
        ) THEN 'Maintenance Mismatch'
        ELSE 'Unknown Issue'
    END AS mismatch_type
FROM
    bronze.erp_px_cat_g1v2 b
    JOIN silver.erp_px_cat_g1v2 s ON b.id = s.id
WHERE
    b.id != s.id
    OR (
        b.cat != s.cat
        AND NOT (
            b.cat IS NULL
            AND s.cat IS NULL
        )
    )
    OR (
        b.subcat != s.subcat
        AND NOT (
            b.subcat IS NULL
            AND s.subcat IS NULL
        )
    )
    OR (
        b.maintenance != s.maintenance
        AND NOT (
            b.maintenance IS NULL
            AND s.maintenance IS NULL
        )
    )
LIMIT
    20
;


-- 3. PRIMARY KEY VALIDATION
-- Verify ID uniqueness and completeness
-- =============================================================================
SELECT
    'Primary Key Validation' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT id) AS unique_ids,
    COUNT(
        CASE
            WHEN id IS NULL THEN 1
        END
    ) AS null_ids,
    COUNT(*) - COUNT(DISTINCT id) AS duplicate_ids,
    CASE
        WHEN COUNT(
            CASE
                WHEN id IS NULL THEN 1
            END
        ) = 0
        AND COUNT(*) = COUNT(DISTINCT id) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.erp_px_cat_g1v2
;


-- Sample of duplicate IDs if any exist
SELECT
    'Duplicate ID Analysis' AS note,
    id,
    COUNT(*) AS occurrence_count,
    STRING_AGG(DISTINCT cat, ', ') AS associated_categories,
    STRING_AGG(DISTINCT subcat, ', ') AS associated_subcategories
FROM
    silver.erp_px_cat_g1v2
WHERE
    id IS NOT NULL
GROUP BY
    id
HAVING
    COUNT(*) > 1
ORDER BY
    COUNT(*) DESC
LIMIT
    10
;


-- 4. DATA COMPLETENESS VALIDATION
-- Check for NULL values and data completeness
-- =============================================================================
SELECT
    'Data Completeness Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN id IS NOT NULL THEN 1
        END
    ) AS complete_ids,
    COUNT(
        CASE
            WHEN cat IS NOT NULL THEN 1
        END
    ) AS complete_categories,
    COUNT(
        CASE
            WHEN subcat IS NOT NULL THEN 1
        END
    ) AS complete_subcategories,
    COUNT(
        CASE
            WHEN maintenance IS NOT NULL THEN 1
        END
    ) AS complete_maintenance,
    COUNT(
        CASE
            WHEN id IS NULL THEN 1
        END
    ) AS null_ids,
    COUNT(
        CASE
            WHEN cat IS NULL THEN 1
        END
    ) AS null_categories,
    COUNT(
        CASE
            WHEN subcat IS NULL THEN 1
        END
    ) AS null_subcategories,
    COUNT(
        CASE
            WHEN maintenance IS NULL THEN 1
        END
    ) AS null_maintenance,
    ROUND(
        100.0 * COUNT(
            CASE
                WHEN id IS NOT NULL THEN 1
            END
        ) / COUNT(*),
        2
    ) AS id_completeness_percentage,
    ROUND(
        100.0 * COUNT(
            CASE
                WHEN cat IS NOT NULL THEN 1
            END
        ) / COUNT(*),
        2
    ) AS category_completeness_percentage,
    CASE
        WHEN COUNT(
            CASE
                WHEN id IS NULL THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM
    silver.erp_px_cat_g1v2
;


-- 5. CATEGORY DISTRIBUTION VALIDATION
-- Analyze the distribution of category values
-- =============================================================================
SELECT
    'Category Distribution Analysis' AS validation_type,
    cat AS category_value,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM
    silver.erp_px_cat_g1v2
WHERE
    cat IS NOT NULL
GROUP BY
    cat
ORDER BY
    COUNT(*) DESC
LIMIT
    20
;


-- 6. SUBCATEGORY DISTRIBUTION VALIDATION
-- Analyze the distribution of subcategory values
-- =============================================================================
SELECT
    'Subcategory Distribution Analysis' AS validation_type,
    subcat AS subcategory_value,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM
    silver.erp_px_cat_g1v2
WHERE
    subcat IS NOT NULL
GROUP BY
    subcat
ORDER BY
    COUNT(*) DESC
LIMIT
    20
;


-- 7. MAINTENANCE FLAG DISTRIBUTION VALIDATION
-- Analyze the distribution of maintenance values
-- =============================================================================
SELECT
    'Maintenance Flag Distribution Analysis' AS validation_type,
    maintenance AS maintenance_value,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM
    silver.erp_px_cat_g1v2
GROUP BY
    maintenance
ORDER BY
    COUNT(*) DESC
;


-- 8. CATEGORY-SUBCATEGORY RELATIONSHIP VALIDATION
-- Analyze category and subcategory combinations
-- =============================================================================
SELECT
    'Category-Subcategory Relationship Analysis' AS validation_type,
    cat AS category,
    subcat AS subcategory,
    COUNT(*) AS combination_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM
    silver.erp_px_cat_g1v2
WHERE
    cat IS NOT NULL
    OR subcat IS NOT NULL
GROUP BY
    cat,
    subcat
ORDER BY
    COUNT(*) DESC
LIMIT
    25
;


-- 9. DATA CONSISTENCY VALIDATION
-- Check for logical consistency in category hierarchies
-- =============================================================================
SELECT
    'Data Consistency Check' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(
        CASE
            WHEN cat IS NOT NULL
            AND subcat IS NULL THEN 1
        END
    ) AS category_without_subcategory,
    COUNT(
        CASE
            WHEN cat IS NULL
            AND subcat IS NOT NULL THEN 1
        END
    ) AS subcategory_without_category,
    COUNT(
        CASE
            WHEN cat IS NOT NULL
            AND subcat IS NOT NULL THEN 1
        END
    ) AS complete_category_hierarchy,
    COUNT(
        CASE
            WHEN cat IS NULL
            AND subcat IS NULL THEN 1
        END
    ) AS no_category_data,
    CASE
        WHEN COUNT(
            CASE
                WHEN cat IS NULL
                AND subcat IS NOT NULL THEN 1
            END
        ) = 0 THEN 'PASS'
        ELSE 'REVIEW'
    END AS status
FROM
    silver.erp_px_cat_g1v2
;


-- Sample of potential consistency issues
SELECT
    'Consistency Issues Sample' AS note,
    id,
    cat,
    subcat,
    maintenance,
    CASE
        WHEN cat IS NULL
        AND subcat IS NOT NULL THEN 'Subcategory without Category'
        WHEN cat IS NOT NULL
        AND subcat IS NULL THEN 'Category without Subcategory'
        WHEN cat IS NULL
        AND subcat IS NULL THEN 'No Category Data'
        ELSE 'Complete Hierarchy'
    END AS consistency_status
FROM
    silver.erp_px_cat_g1v2
WHERE
    (
        cat IS NULL
        AND subcat IS NOT NULL
    )
    OR (
        cat IS NULL
        AND subcat IS NULL
        AND id IS NOT NULL
    )
ORDER BY
    CASE
        WHEN cat IS NULL
        AND subcat IS NOT NULL THEN 1
        WHEN cat IS NULL
        AND subcat IS NULL THEN 2
        ELSE 3
    END,
    id
LIMIT
    20
;


-- 10. BRONZE TO SILVER COMPARISON VALIDATION
-- Direct comparison between bronze and silver layers
-- =============================================================================
SELECT
    'Bronze to Silver Comparison' AS validation_type,
    'Bronze Layer' AS layer,
    COUNT(*) AS total_records,
    COUNT(DISTINCT id) AS unique_ids,
    COUNT(
        CASE
            WHEN id IS NULL THEN 1
        END
    ) AS null_ids,
    COUNT(
        CASE
            WHEN cat IS NULL THEN 1
        END
    ) AS null_categories,
    COUNT(
        CASE
            WHEN subcat IS NULL THEN 1
        END
    ) AS null_subcategories,
    COUNT(
        CASE
            WHEN maintenance IS NULL THEN 1
        END
    ) AS null_maintenance
FROM
    bronze.erp_px_cat_g1v2
UNION ALL
SELECT
    'Bronze to Silver Comparison' AS validation_type,
    'Silver Layer' AS layer,
    COUNT(*) AS total_records,
    COUNT(DISTINCT id) AS unique_ids,
    COUNT(
        CASE
            WHEN id IS NULL THEN 1
        END
    ) AS null_ids,
    COUNT(
        CASE
            WHEN cat IS NULL THEN 1
        END
    ) AS null_categories,
    COUNT(
        CASE
            WHEN subcat IS NULL THEN 1
        END
    ) AS null_subcategories,
    COUNT(
        CASE
            WHEN maintenance IS NULL THEN 1
        END
    ) AS null_maintenance
FROM
    silver.erp_px_cat_g1v2
;


-- 11. TRANSFORMATION SAMPLE VERIFICATION
-- Sample records showing bronze vs silver (should be identical)
-- =============================================================================
SELECT
    'Sample Transformation Verification' AS note,
    b.id AS bronze_id,
    s.id AS silver_id,
    b.cat AS bronze_category,
    s.cat AS silver_category,
    b.subcat AS bronze_subcategory,
    s.subcat AS silver_subcategory,
    b.maintenance AS bronze_maintenance,
    s.maintenance AS silver_maintenance,
    CASE
        WHEN b.id = s.id
        AND (
            b.cat = s.cat
            OR (
                b.cat IS NULL
                AND s.cat IS NULL
            )
        )
        AND (
            b.subcat = s.subcat
            OR (
                b.subcat IS NULL
                AND s.subcat IS NULL
            )
        )
        AND (
            b.maintenance = s.maintenance
            OR (
                b.maintenance IS NULL
                AND s.maintenance IS NULL
            )
        ) THEN 'Identical (Correct)'
        ELSE 'Different (Issue)'
    END AS comparison_result
FROM
    bronze.erp_px_cat_g1v2 b
    JOIN silver.erp_px_cat_g1v2 s ON b.id = s.id
ORDER BY
    b.id
LIMIT
    25
;


-- 12. MISSING RECORDS VALIDATION
-- Check for records that exist in bronze but not in silver (and vice versa)
-- =============================================================================
SELECT
    'Missing Records Check - Bronze to Silver' AS validation_type,
    COUNT(*) AS bronze_records_missing_in_silver
FROM
    bronze.erp_px_cat_g1v2 b
    LEFT JOIN silver.erp_px_cat_g1v2 s ON b.id = s.id
WHERE
    s.id IS NULL
;


SELECT
    'Missing Records Check - Silver to Bronze' AS validation_type,
    COUNT(*) AS silver_records_missing_in_bronze
FROM
    silver.erp_px_cat_g1v2 s
    LEFT JOIN bronze.erp_px_cat_g1v2 b ON s.id = b.id
WHERE
    b.id IS NULL
;


-- Sample of missing records if any
SELECT
    'Sample Missing Records - Bronze to Silver' AS note,
    id,
    cat,
    subcat,
    maintenance,
    'Missing in Silver' AS status
FROM
    bronze.erp_px_cat_g1v2 b
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            silver.erp_px_cat_g1v2 s
        WHERE
            s.id = b.id
    )
LIMIT
    10
UNION ALL
SELECT
    'Sample Missing Records - Silver to Bronze' AS note,
    id,
    cat,
    subcat,
    maintenance,
    'Missing in Bronze' AS status
FROM
    silver.erp_px_cat_g1v2 s
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            bronze.erp_px_cat_g1v2 b
        WHERE
            b.id = s.id
    )
LIMIT
    10
;


-- 13. COMPREHENSIVE VALIDATION SUMMARY
-- Overall summary of transformation results
-- =============================================================================
SELECT
    'VALIDATION SUMMARY REPORT' AS report_type,
    COUNT(*) AS total_product_category_records,
    COUNT(DISTINCT id) AS unique_product_ids,
    COUNT(DISTINCT cat) AS unique_categories,
    COUNT(DISTINCT subcat) AS unique_subcategories,
    COUNT(DISTINCT maintenance) AS unique_maintenance_values,
    COUNT(
        CASE
            WHEN id IS NULL THEN 1
        END
    ) AS records_with_null_id,
    COUNT(
        CASE
            WHEN cat IS NULL THEN 1
        END
    ) AS records_with_null_category,
    COUNT(
        CASE
            WHEN subcat IS NULL THEN 1
        END
    ) AS records_with_null_subcategory,
    COUNT(
        CASE
            WHEN maintenance IS NULL THEN 1
        END
    ) AS records_with_null_maintenance,
    COUNT(
        CASE
            WHEN cat IS NOT NULL
            AND subcat IS NOT NULL THEN 1
        END
    ) AS complete_category_hierarchies
FROM
    silver.erp_px_cat_g1v2
;


-- 14. DATA QUALITY SCORE
-- Calculate overall data quality score for the pass-through transformation
-- =============================================================================
SELECT
    'Data Quality Score' AS metric_type,
    ROUND(
        100.0 * (
            COUNT(
                CASE
                    WHEN id IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN cat IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN subcat IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN maintenance IS NOT NULL THEN 1
                END
            )
        ) / (COUNT(*) * 4),
        2
    ) AS data_quality_percentage,
    CASE
        WHEN 100.0 * (
            COUNT(
                CASE
                    WHEN id IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN cat IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN subcat IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN maintenance IS NOT NULL THEN 1
                END
            )
        ) / (COUNT(*) * 4) >= 95 THEN 'EXCELLENT'
        WHEN 100.0 * (
            COUNT(
                CASE
                    WHEN id IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN cat IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN subcat IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN maintenance IS NOT NULL THEN 1
                END
            )
        ) / (COUNT(*) * 4) >= 85 THEN 'GOOD'
        WHEN 100.0 * (
            COUNT(
                CASE
                    WHEN id IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN cat IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN subcat IS NOT NULL THEN 1
                END
            ) + COUNT(
                CASE
                    WHEN maintenance IS NOT NULL THEN 1
                END
            )
        ) / (COUNT(*) * 4) >= 70 THEN 'ACCEPTABLE'
        ELSE 'NEEDS IMPROVEMENT'
    END AS quality_rating
FROM
    silver.erp_px_cat_g1v2
;