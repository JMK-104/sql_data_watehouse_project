/*
Script Purpose:
The following script is designed to load transformed data from bronze layer tables into 
silver layer tables. Tables are cleaned and standardized while maintaining business logic.

Usage:
This stored procedure does not accept any input parameters or return any values.
To use this procedure:
CALL silver.load_silver();

WARNING:
Running this script will truncate silver tables and reload all their contents from bronze layer.
Please ensure that your bronze layer data is current before running this script.
 */
-- ==============================================
-- Stored Procedure: Load Silver Layer Data
-- Description: Transforms and loads data from bronze to silver layer for CRM and ERP systems
-- ==============================================
CREATE
OR REPLACE PROCEDURE silver.load_silver () LANGUAGE plpgsql AS $$
DECLARE
    v_row_count INTEGER;
    v_error_message TEXT;
    v_error_detail TEXT;
    v_error_hint TEXT;
    v_error_context TEXT;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_load_start TIMESTAMP;
    v_load_end TIMESTAMP;
BEGIN
    -- Initialize timing
    v_start_time := clock_timestamp();
    
    RAISE NOTICE '=======================================================';    
    RAISE NOTICE 'Starting Silver Layer Data Load at %', v_start_time;
    RAISE NOTICE '=======================================================';
    
    -- Main execution block
    BEGIN
        -- ==============================================
        -- Load CRM Silver Data with transformations
        -- ==============================================
        RAISE NOTICE '';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE 'Beginning CRM Silver Data Load';
        RAISE NOTICE '=======================================================';
        
        -- Load silver.crm_cust_info with transformations
        BEGIN
            v_load_start := clock_timestamp();
            TRUNCATE TABLE silver.crm_cust_info;
            RAISE NOTICE '>>> Truncating table: silver.crm_cust_info';
            RAISE NOTICE '>>> Transforming and inserting data into: silver.crm_cust_info';
            
            INSERT INTO silver.crm_cust_info (
                cst_id,
                cst_key,
                cst_firstname,
                cst_lastname,
                cst_marital_status,
                cst_gndr,
                cst_create_date
            )
            SELECT
                cst_id,
                cst_key,
                TRIM(cst_firstname) AS cst_firstname,
                TRIM(cst_lastname) AS cst_lastname,
                CASE (UPPER(TRIM(cst_marital_status)))
                    WHEN 'M' THEN 'Married'
                    WHEN 'S' THEN 'Single'
                    ELSE 'N/A'
                END AS cst_marital_status,
                CASE (UPPER(TRIM(cst_gndr)))
                    WHEN 'F' THEN 'Female'
                    WHEN 'M' THEN 'Male'
                    ELSE 'N/A'
                END AS cst_gndr,
                cst_create_date
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY cst_id
                        ORDER BY cst_create_date DESC
                    ) AS flag_latest
                FROM bronze.crm_cust_info
                WHERE cst_id IS NOT NULL
            ) AS sq
            WHERE flag_latest = 1;
            
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_load_end := clock_timestamp();
            RAISE NOTICE 'Table silver.crm_cust_info successfully loaded (% rows)', v_row_count;
            RAISE NOTICE 'Load completion time: %', v_load_end - v_load_start;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to load silver.crm_cust_info: %', SQLERRM;
                RAISE EXCEPTION 'Critical error in CRM customer info transformation: %', SQLERRM;
        END;
        
        -- Load silver.crm_prd_info with transformations
        BEGIN
            v_load_start := clock_timestamp();
            TRUNCATE TABLE silver.crm_prd_info;
            RAISE NOTICE '>>> Truncating table: silver.crm_prd_info';
            RAISE NOTICE '>>> Transforming and inserting data into: silver.crm_prd_info';
            
            INSERT INTO silver.crm_prd_info (
                prd_id,
                cat_id,
                prd_key,
                prd_nm,
                prd_cost,
                prd_line,
                prd_start_dt,
                prd_end_dt
            )
            SELECT
                prd_id,
                REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
                SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
                prd_nm,
                COALESCE(prd_cost, 0) AS prd_cost,
                CASE (UPPER(TRIM(prd_line)))
                    WHEN 'M' THEN 'Mountain'
                    WHEN 'R' THEN 'Road'
                    WHEN 'S' THEN 'Other Sales'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'N/A'
                END AS prd_line,
                prd_start_dt,
                CAST(
                    LEAD(prd_start_dt, 1) OVER (
                        PARTITION BY prd_key
                        ORDER BY prd_start_dt
                    ) - INTERVAL '1 day' AS DATE
                ) AS prd_end_dt
            FROM bronze.crm_prd_info;
            
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_load_end := clock_timestamp();
            RAISE NOTICE 'Table silver.crm_prd_info successfully loaded (% rows)', v_row_count;
            RAISE NOTICE 'Load completion time: %', v_load_end - v_load_start;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to load silver.crm_prd_info: %', SQLERRM;
                RAISE EXCEPTION 'Critical error in CRM product info transformation: %', SQLERRM;
        END;
        
        -- Load silver.crm_sales_details with transformations
        BEGIN
            v_load_start := clock_timestamp();
            TRUNCATE TABLE silver.crm_sales_details;
            RAISE NOTICE '>>> Truncating table: silver.crm_sales_details';
            RAISE NOTICE '>>> Transforming and inserting data into: silver.crm_sales_details';
            
            INSERT INTO silver.crm_sales_details (
                sls_ord_num,
                sls_prd_key,
                sls_cust_id,
                sls_order_dt,
                sls_ship_dt,
                sls_due_dt,
                sls_sales,
                sls_quantity,
                sls_price
            )
            SELECT
                sls_ord_num,
                sls_prd_key,
                sls_cust_id,
                CASE
                    WHEN sls_order_dt = 0
                    OR LENGTH(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
                    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
                END AS sls_order_dt,
                CASE
                    WHEN sls_ship_dt = 0
                    OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
                    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
                END AS sls_ship_dt,
                CASE
                    WHEN sls_due_dt = 0
                    OR LENGTH(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
                    ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
                END AS sls_due_dt,
                CASE
                    WHEN ABS(sls_sales) IS NULL
                    OR sls_sales != sls_quantity * ABS(sls_price) THEN NULLIF(sls_quantity, 0) * ABS(sls_price)
                    ELSE ABS(sls_sales)
                END AS sls_sales,
                sls_quantity,
                CASE
                    WHEN ABS(sls_price) IS NULL THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
                    ELSE ABS(sls_price)
                END AS sls_price
            FROM bronze.crm_sales_details;
            
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_load_end := clock_timestamp();
            RAISE NOTICE 'Table silver.crm_sales_details successfully loaded (% rows)', v_row_count;
            RAISE NOTICE 'Load completion time: %', v_load_end - v_load_start;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to load silver.crm_sales_details: %', SQLERRM;
                RAISE EXCEPTION 'Critical error in CRM sales details transformation: %', SQLERRM;
        END;
        
        RAISE NOTICE '';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE 'CRM Silver Data Successfully Loaded';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE '';
        
        -- ==============================================
        -- Load ERP Silver Data with transformations
        -- ==============================================
        RAISE NOTICE '=======================================================';
        RAISE NOTICE 'Beginning ERP Silver Data Load';
        RAISE NOTICE '=======================================================';
        
        -- Load silver.erp_cust_az12 with transformations
        BEGIN
            v_load_start := clock_timestamp();
            TRUNCATE TABLE silver.erp_cust_az12;
            RAISE NOTICE '>>> Truncating table: silver.erp_cust_az12';
            RAISE NOTICE '>>> Transforming and inserting data into: silver.erp_cust_az12';
            
            INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
            SELECT
                CASE
                    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
                    ELSE cid
                END AS cid,
                CASE
                    WHEN bdate > CURRENT_DATE THEN NULL
                    ELSE bdate
                END AS bdate,
                CASE
                    WHEN (LOWER(TRIM(gen))) IN ('m', 'male') THEN 'Male'
                    WHEN (LOWER(TRIM(gen))) IN ('f', 'female') THEN 'Female'
                    ELSE 'N/A'
                END AS gen
            FROM bronze.erp_cust_az12;
            
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_load_end := clock_timestamp();
            RAISE NOTICE 'Table silver.erp_cust_az12 successfully loaded (% rows)', v_row_count;
            RAISE NOTICE 'Load completion time: %', v_load_end - v_load_start;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to load silver.erp_cust_az12: %', SQLERRM;
                RAISE EXCEPTION 'Critical error in ERP customer transformation: %', SQLERRM;
        END;
        
        -- Load silver.erp_loc_a101 with transformations
        BEGIN
            v_load_start := clock_timestamp();
            TRUNCATE TABLE silver.erp_loc_a101;
            RAISE NOTICE '>>> Truncating table: silver.erp_loc_a101';
            RAISE NOTICE '>>> Transforming and inserting data into: silver.erp_loc_a101';
            
            INSERT INTO silver.erp_loc_a101 (cid, cntry)
            SELECT
                REPLACE(cid, '-', '') AS cid,
                CASE
                    WHEN TRIM(cntry) = ''
                    OR cntry IS NULL THEN 'N/A'
                    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                    ELSE TRIM(cntry)
                END AS cntry
            FROM bronze.erp_loc_a101;
            
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_load_end := clock_timestamp();
            RAISE NOTICE 'Table silver.erp_loc_a101 successfully loaded (% rows)', v_row_count;
            RAISE NOTICE 'Load completion time: %', v_load_end - v_load_start;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to load silver.erp_loc_a101: %', SQLERRM;
                RAISE EXCEPTION 'Critical error in ERP location transformation: %', SQLERRM;
        END;
        
        -- Load silver.erp_px_cat_g1v2 (pass-through transformation)
        BEGIN
            v_load_start := clock_timestamp();
            TRUNCATE TABLE silver.erp_px_cat_g1v2;
            RAISE NOTICE '>>> Truncating table: silver.erp_px_cat_g1v2';
            RAISE NOTICE '>>> Inserting data into: silver.erp_px_cat_g1v2 (pass-through)';
            
            INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
            SELECT
                id,
                cat,
                subcat,
                maintenance
            FROM bronze.erp_px_cat_g1v2;
            
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_load_end := clock_timestamp();
            RAISE NOTICE 'Table silver.erp_px_cat_g1v2 successfully loaded (% rows)', v_row_count;
            RAISE NOTICE 'Load completion time: %', v_load_end - v_load_start;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to load silver.erp_px_cat_g1v2: %', SQLERRM;
                RAISE EXCEPTION 'Critical error in ERP product category load: %', SQLERRM;
        END;

        -- Calculate execution time
        v_end_time := clock_timestamp();
        
        RAISE NOTICE '';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE 'ERP Silver Data Successfully Loaded';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE '';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE 'Silver layer data load completed successfully!';
        RAISE NOTICE 'Total execution time: %', v_end_time - v_start_time;
        RAISE NOTICE '=======================================================';

    EXCEPTION
        -- Global exception handler for any unhandled errors
        WHEN OTHERS THEN
            -- Get detailed error information
            GET STACKED DIAGNOSTICS
                v_error_message = MESSAGE_TEXT,
                v_error_detail = PG_EXCEPTION_DETAIL,
                v_error_hint = PG_EXCEPTION_HINT,
                v_error_context = PG_EXCEPTION_CONTEXT;
            
            v_end_time := clock_timestamp();
            
            RAISE NOTICE '=======================================================';
            RAISE NOTICE 'ERROR: Silver layer data load FAILED!';
            RAISE NOTICE 'Execution time before failure: %', v_end_time - v_start_time;
            RAISE NOTICE 'Error Message: %', v_error_message;
            RAISE NOTICE 'Error Detail: %', COALESCE(v_error_detail, 'No additional detail');
            RAISE NOTICE 'Error Hint: %', COALESCE(v_error_hint, 'No hint available');
            RAISE NOTICE 'Error Context: %', COALESCE(v_error_context, 'No context available');
            RAISE NOTICE '=======================================================';
            
            -- Re-raise the exception to ensure the calling code knows the procedure failed
            RAISE;
    END;
END;
$$
;
