-- =============================================================================
-- BRONZE LAYER DATA VALIDATION QUERIES
-- Data Warehouse Project - PostgreSQL
-- =============================================================================
-- Purpose: Validate successful loading of CSV data into bronze layer tables
-- Tables Validated: 
--   - bronze.crm_cust_info
--   - bronze.crm_prd_info  
--   - bronze.crm_sales_details
--   - bronze.erp_CUST_AZ12
--   - bronze.erp_LOC_A101
--   - bronze.erp_PX_CAT_G1V2
-- =============================================================================
-- =============================================================================
-- 1. OVERALL LOAD SUMMARY
-- Provides high-level overview of all bronze tables
-- =============================================================================
SELECT
    'Bronze Layer Load Summary' AS validation_type,
    'CRM Customer Info' AS table_name,
    COUNT(*) AS record_count,
    MIN(
        CASE
            WHEN LENGTH(
                TRIM(COALESCE(CAST(COLUMNS.column_name AS TEXT), ''))
            ) > 0 THEN 'Data Present'
            ELSE 'Empty Data'
        END
    ) AS data_status
FROM
    bronze.crm_cust_info
    CROSS JOIN LATERAL (
        SELECT
            column_name
        FROM
            information_schema.columns
        WHERE
            table_schema = 'bronze'
            AND table_name = 'crm_cust_info'
        LIMIT
            1
    ) COLUMNS
UNION ALL
SELECT
    'Bronze Layer Load Summary' AS validation_type,
    'CRM Product Info' AS table_name,
    COUNT(*) AS record_count,
    CASE
        WHEN COUNT(*) > 0 THEN 'Data Present'
        ELSE 'No Data'
    END AS data_status
FROM
    bronze.crm_prd_info
UNION ALL
SELECT
    'Bronze Layer Load Summary' AS validation_type,
    'CRM Sales Details' AS table_name,
    COUNT(*) AS record_count,
    CASE
        WHEN COUNT(*) > 0 THEN 'Data Present'
        ELSE 'No Data'
    END AS data_status
FROM
    bronze.crm_sales_details
UNION ALL
SELECT
    'Bronze Layer Load Summary' AS validation_type,
    'ERP Customer AZ12' AS table_name,
    COUNT(*) AS record_count,
    CASE
        WHEN COUNT(*) > 0 THEN 'Data Present'
        ELSE 'No Data'
    END AS data_status
FROM
    bronze.erp_CUST_AZ12
UNION ALL
SELECT
    'Bronze Layer Load Summary' AS validation_type,
    'ERP Location A101' AS table_name,
    COUNT(*) AS record_count,
    CASE
        WHEN COUNT(*) > 0 THEN 'Data Present'
        ELSE 'No Data'
    END AS data_status
FROM
    bronze.erp_LOC_A101
UNION ALL
SELECT
    'Bronze Layer Load Summary' AS validation_type,
    'ERP Product Category G1V2' AS table_name,
    COUNT(*) AS record_count,
    CASE
        WHEN COUNT(*) > 0 THEN 'Data Present'
        ELSE 'No Data'
    END AS data_status
FROM
    bronze.erp_PX_CAT_G1V2
;


-- =============================================================================
-- 2. DETAILED TABLE VALIDATION - CRM CUSTOMER INFO
-- =============================================================================
SELECT
    'CRM Customer Info Validation' AS validation_type,
    COUNT(*) AS total_records,
    COUNT(*) FILTER (
        WHERE
            LENGTH(TRIM(COALESCE(CAST(* AS TEXT), ''))) > 0
    ) AS non_empty_records,
    COUNT(*) - COUNT(*) FILTER (
        WHERE
            LENGTH(TRIM(COALESCE(CAST(* AS TEXT), ''))) > 0
    ) AS potentially_empty_records,
    CASE
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL - No records loaded'
    END AS load_status,
    CASE
        WHEN COUNT(*) > 0 THEN ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE
                    LENGTH(TRIM(COALESCE(CAST(* AS TEXT), ''))) > 0
            ) / COUNT(*),
            2
        )
        ELSE 0
    END AS data_quality_percentage
FROM
    bronze.crm_cust_info
;


-- Sample records from CRM Customer Info
SELECT
    'CRM Customer Info Sample' AS note,
    *
FROM
    bronze.crm_cust_info
LIMIT
    5
;


-- =============================================================================
-- 3. DETAILED TABLE VALIDATION - CRM PRODUCT INFO
-- =============================================================================
SELECT
    'CRM Product Info Validation' AS validation_type,
    COUNT(*) AS total_records,
    CASE
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL - No records loaded'
    END AS load_status
FROM
    bronze.crm_prd_info
;


-- Sample records from CRM Product Info
SELECT
    'CRM Product Info Sample' AS note,
    *
FROM
    bronze.crm_prd_info
LIMIT
    5
;


-- =============================================================================
-- 4. DETAILED TABLE VALIDATION - CRM SALES DETAILS
-- =============================================================================
SELECT
    'CRM Sales Details Validation' AS validation_type,
    COUNT(*) AS total_records,
    CASE
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL - No records loaded'
    END AS load_status
FROM
    bronze.crm_sales_details
;


-- Sample records from CRM Sales Details
SELECT
    'CRM Sales Details Sample' AS note,
    *
FROM
    bronze.crm_sales_details
LIMIT
    5
;


-- =============================================================================
-- 5. DETAILED TABLE VALIDATION - ERP CUSTOMER AZ12
-- =============================================================================
SELECT
    'ERP Customer AZ12 Validation' AS validation_type,
    COUNT(*) AS total_records,
    CASE
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL - No records loaded'
    END AS load_status
FROM
    bronze.erp_CUST_AZ12
;


-- Sample records from ERP Customer AZ12
SELECT
    'ERP Customer AZ12 Sample' AS note,
    *
FROM
    bronze.erp_CUST_AZ12
LIMIT
    5
;


-- =============================================================================
-- 6. DETAILED TABLE VALIDATION - ERP LOCATION A101
-- =============================================================================
SELECT
    'ERP Location A101 Validation' AS validation_type,
    COUNT(*) AS total_records,
    CASE
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL - No records loaded'
    END AS load_status
FROM
    bronze.erp_LOC_A101
;


-- Sample records from ERP Location A101
SELECT
    'ERP Location A101 Sample' AS note,
    *
FROM
    bronze.erp_LOC_A101
LIMIT
    5
;


-- =============================================================================
-- 7. DETAILED TABLE VALIDATION - ERP PRODUCT CATEGORY G1V2
-- =============================================================================
SELECT
    'ERP Product Category G1V2 Validation' AS validation_type,
    COUNT(*) AS total_records,
    CASE
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL - No records loaded'
    END AS load_status
FROM
    bronze.erp_PX_CAT_G1V2
;


-- Sample records from ERP Product Category G1V2
SELECT
    'ERP Product Category G1V2 Sample' AS note,
    *
FROM
    bronze.erp_PX_CAT_G1V2
LIMIT
    5
;


-- =============================================================================
-- 8. DATA FRESHNESS VALIDATION
-- Check when tables were last populated (requires table with timestamps)
-- Note: This assumes your tables have timestamp columns or you can check pg_stat_user_tables
-- =============================================================================
SELECT
    'Data Freshness Check' AS validation_type,
    schemaname,
    tablename,
    n_tup_ins AS rows_inserted,
    n_tup_upd AS rows_updated,
    n_tup_del AS rows_deleted,
    n_live_tup AS current_row_count,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM
    pg_stat_user_tables
WHERE
    schemaname = 'bronze'
    AND tablename IN (
        'crm_cust_info',
        'crm_prd_info',
        'crm_sales_details',
        'erp_cust_az12',
        'erp_loc_a101',
        'erp_px_cat_g1v2'
    )
ORDER BY
    tablename
;


-- =============================================================================
-- 9. NULL VALUE ANALYSIS
-- Analyze NULL values across all bronze tables to identify data quality issues
-- =============================================================================
-- CRM Customer Info NULL Analysis
SELECT
    'CRM Customer Info NULL Analysis' AS analysis_type,
    COUNT(*) AS total_records,
    -- Add specific column NULL checks here based on your actual table schema
    'Check individual columns based on your schema' AS note
FROM
    bronze.crm_cust_info
;


-- Note: The following queries need to be customized based on your actual column names
-- Example template for NULL analysis:
/*
SELECT 
'CRM Customer Info NULL Analysis' AS analysis_type,
COUNT(*) AS total_records,
COUNT(*) - COUNT(customer_id) AS null_customer_ids,
COUNT(*) - COUNT(customer_name) AS null_customer_names,
COUNT(*) - COUNT(email) AS null_emails
FROM bronze.crm_cust_info;
 */
-- =============================================================================
-- 10. DUPLICATE RECORDS CHECK
-- Identify potential duplicate records in each table
-- =============================================================================
-- Template for duplicate detection (customize based on your key columns):
SELECT
    'Duplicate Records Check' AS validation_type,
    'Template - Customize based on your key columns' AS note,
    'Example: GROUP BY key_column HAVING COUNT(*) > 1' AS example_query
;


-- Example for tables with ID columns:
/*
SELECT 
'CRM Customer Duplicates' AS check_type,
customer_id,
COUNT(*) as duplicate_count
FROM bronze.crm_cust_info 
GROUP BY customer_id 
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;
 */
-- =============================================================================
-- 11. DATA TYPE VALIDATION
-- Check for data type consistency and format issues
-- =============================================================================
SELECT
    'Data Type Validation' AS validation_type,
    table_schema,
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM
    information_schema.columns
WHERE
    table_schema = 'bronze'
    AND table_name IN (
        'crm_cust_info',
        'crm_prd_info',
        'crm_sales_details',
        'erp_cust_az12',
        'erp_loc_a101',
        'erp_px_cat_g1v2'
    )
ORDER BY
    table_name,
    ordinal_position
;


-- =============================================================================
-- 12. REFERENTIAL INTEGRITY CHECKS
-- Basic checks for potential relationships between tables
-- =============================================================================
-- Count distinct values that might be foreign keys
-- This helps identify potential relationships for data validation
-- Example: Check if customer IDs from CRM tables might relate to ERP customer data
/*
SELECT 
'Potential Customer ID Overlap' AS relationship_check,
crm_distinct_customers,
erp_distinct_customers,
-- Add logic to check overlapping IDs if column names are known
FROM 
(SELECT COUNT(DISTINCT customer_id) as crm_distinct_customers FROM bronze.crm_cust_info) crm
CROSS JOIN 
(SELECT COUNT(DISTINCT customer_id) as erp_distinct_customers FROM bronze.erp_CUST_AZ12) erp;
 */
-- =============================================================================
-- 13. FILE LOAD COMPLETENESS CHECK
-- Verify all expected files were processed
-- =============================================================================
SELECT
    'Load Completeness Summary' AS validation_type,
    6 AS expected_tables,
    COUNT(*) AS loaded_tables,
    CASE
        WHEN COUNT(*) = 6 THEN 'COMPLETE'
        ELSE 'INCOMPLETE - Missing: ' || (6 - COUNT(*))::TEXT || ' tables'
    END AS load_completeness_status
FROM
    (
        SELECT
            'crm_cust_info' AS table_name
        WHERE
            EXISTS (
                SELECT
                    1
                FROM
                    bronze.crm_cust_info
                LIMIT
                    1
            )
        UNION ALL
        SELECT
            'crm_prd_info'
        WHERE
            EXISTS (
                SELECT
                    1
                FROM
                    bronze.crm_prd_info
                LIMIT
                    1
            )
        UNION ALL
        SELECT
            'crm_sales_details'
        WHERE
            EXISTS (
                SELECT
                    1
                FROM
                    bronze.crm_sales_details
                LIMIT
                    1
            )
        UNION ALL
        SELECT
            'erp_CUST_AZ12'
        WHERE
            EXISTS (
                SELECT
                    1
                FROM
                    bronze.erp_CUST_AZ12
                LIMIT
                    1
            )
        UNION ALL
        SELECT
            'erp_LOC_A101'
        WHERE
            EXISTS (
                SELECT
                    1
                FROM
                    bronze.erp_LOC_A101
                LIMIT
                    1
            )
        UNION ALL
        SELECT
            'erp_PX_CAT_G1V2'
        WHERE
            EXISTS (
                SELECT
                    1
                FROM
                    bronze.erp_PX_CAT_G1V2
                LIMIT
                    1
            )
    ) loaded_tables
;


-- =============================================================================
-- 14. TABLE SIZE AND STORAGE ANALYSIS
-- Monitor storage usage and table sizes
-- =============================================================================
SELECT
    'Storage Analysis' AS analysis_type,
    schemaname,
    tablename,
    PG_SIZE_PRETTY(
        PG_TOTAL_RELATION_SIZE(schemaname || '.' || tablename)
    ) AS total_size,
    PG_SIZE_PRETTY(PG_RELATION_SIZE(schemaname || '.' || tablename)) AS table_size,
    PG_SIZE_PRETTY(
        PG_TOTAL_RELATION_SIZE(schemaname || '.' || tablename) - PG_RELATION_SIZE(schemaname || '.' || tablename)
    ) AS index_size
FROM
    pg_tables
WHERE
    schemaname = 'bronze'
    AND tablename IN (
        'crm_cust_info',
        'crm_prd_info',
        'crm_sales_details',
        'erp_cust_az12',
        'erp_loc_a101',
        'erp_px_cat_g1v2'
    )
ORDER BY
    PG_TOTAL_RELATION_SIZE(schemaname || '.' || tablename) DESC
;


-- =============================================================================
-- 15. COMPREHENSIVE VALIDATION REPORT
-- Final summary report of bronze layer load validation
-- =============================================================================
WITH
    table_counts AS (
        SELECT
            'crm_cust_info' AS table_name,
            COUNT(*) AS record_count
        FROM
            bronze.crm_cust_info
        UNION ALL
        SELECT
            'crm_prd_info',
            COUNT(*)
        FROM
            bronze.crm_prd_info
        UNION ALL
        SELECT
            'crm_sales_details',
            COUNT(*)
        FROM
            bronze.crm_sales_details
        UNION ALL
        SELECT
            'erp_CUST_AZ12',
            COUNT(*)
        FROM
            bronze.erp_CUST_AZ12
        UNION ALL
        SELECT
            'erp_LOC_A101',
            COUNT(*)
        FROM
            bronze.erp_LOC_A101
        UNION ALL
        SELECT
            'erp_PX_CAT_G1V2',
            COUNT(*)
        FROM
            bronze.erp_PX_CAT_G1V2
    ),
    validation_summary AS (
        SELECT
            COUNT(*) AS total_tables_loaded,
            SUM(record_count) AS total_records_loaded,
            MIN(record_count) AS min_table_size,
            MAX(record_count) AS max_table_size,
            AVG(record_count) AS avg_table_size,
            COUNT(*) FILTER (
                WHERE
                    record_count = 0
            ) AS empty_tables,
            COUNT(*) FILTER (
                WHERE
                    record_count > 0
            ) AS populated_tables
        FROM
            table_counts
    )
SELECT
    'BRONZE LAYER VALIDATION REPORT' AS report_type,
    total_tables_loaded,
    total_records_loaded,
    populated_tables,
    empty_tables,
    ROUND(avg_table_size, 0) AS avg_records_per_table,
    min_table_size,
    max_table_size,
    CASE
        WHEN empty_tables = 0
        AND total_tables_loaded = 6 THEN 'SUCCESS - All tables loaded'
        WHEN empty_tables > 0 THEN 'WARNING - ' || empty_tables::TEXT || ' empty tables detected'
        WHEN total_tables_loaded < 6 THEN 'FAIL - Missing tables'
        ELSE 'UNKNOWN STATUS'
    END AS overall_load_status
FROM
    validation_summary
;


-- =============================================================================
-- NOTES FOR CUSTOMIZATION:
-- =============================================================================
/*
1. Column-specific validations need to be customized based on your actual table schemas
2. Add specific business logic validations based on your data requirements
3. Update NULL analysis queries with actual column names from your tables
4. Customize duplicate detection based on your business keys
5. Add date/timestamp validations if your data includes temporal fields
6. Consider adding range validations for numeric fields
7. Add pattern validations for formatted fields (emails, phone numbers, etc.)
8. Include cross-table relationship validations based on your data model

Usage Instructions:
- Run these queries after executing your bronze.load_bronze() procedure
- Review results to ensure all expected data has been loaded correctly
- Investigate any FAIL or WARNING statuses
- Use the sample queries to manually inspect data quality
- Monitor storage usage for capacity planning
 */
