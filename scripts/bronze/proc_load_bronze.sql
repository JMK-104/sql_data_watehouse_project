/*
Script Purpose:
    The following script is designed to load CSV data into database tables of the same name. 
    Tables are formatted in accordance with the source data.

Usage:
    This stored procedure does not accept any input parameters or return any values.
    To use this procedure:
        CALL bronze.load_bronze();
        -- or with custom path:
        CALL bronze.load_bronze('/your/custom/path');

WARNING:
    Running this script will delete tables and all their contents. Please ensure that your date
    is backed up before running this script.
*/

-- Process will be contained in one stored procedure

-- ==============================================
-- Stored Procedure: Load Bronze Layer Data
-- Description: Creates tables and loads CSV data for CRM and ERP source systems
-- ==============================================

CREATE OR REPLACE PROCEDURE bronze.load_bronze(
    p_base_path TEXT DEFAULT '/Users/justinkakuyo/Desktop/Dev/projects/SQL/sql-data-warehouse-project/datasets'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count INTEGER;
    v_crm_path TEXT;
    v_erp_path TEXT;
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
    
    -- Set up paths
    v_crm_path := p_base_path || '/source_crm/';
    v_erp_path := p_base_path || '/source_erp/';
    
    RAISE NOTICE '=======================================================';    
    RAISE NOTICE 'Starting Bronze Layer Data Load at %', v_start_time;
    RAISE NOTICE '=======================================================';
    
    -- Main execution block
    BEGIN
        -- ==============================================
        -- Load CRM Data with individual error handling
        -- ==============================================
        RAISE NOTICE '';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE 'Beginning CRM Data Load';
        RAISE NOTICE '=======================================================';
        
        -- Load crm_cust_info
        BEGIN
            v_load_start := clock_timestamp();
            TRUNCATE TABLE bronze.crm_cust_info;
            RAISE NOTICE '>>> Truncating table: bronze.crm_cust_info';
            RAISE NOTICE '>>> Inserting Data into table: bronze.crm_cust_info';
            EXECUTE 'COPY bronze.crm_cust_info FROM ''' || v_crm_path || 'cust_info.csv'' WITH CSV HEADER';
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_load_end := clock_timestamp();
            RAISE NOTICE 'Table bronze.crm_cust_info successfully loaded (% rows)', v_row_count;
            RAISE NOTICE 'Load completion time: %', v_load_end - v_load_start;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to load bronze.crm_cust_info: %', SQLERRM;
                RAISE EXCEPTION 'Critical error in CRM customer info load: %', SQLERRM;
        END;
        
        -- Load crm_prd_info
        BEGIN
            v_load_start := clock_timestamp();
            TRUNCATE TABLE bronze.crm_prd_info;
            RAISE NOTICE '>>> Truncating table: bronze.crm_prd_info';
            RAISE NOTICE '>>> Inserting Data into table: bronze.crm_prd_info';
            EXECUTE 'COPY bronze.crm_prd_info FROM ''' || v_crm_path || 'prd_info.csv'' WITH CSV HEADER';
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_load_end := clock_timestamp();
            RAISE NOTICE 'Table bronze.crm_prd_info successfully loaded (% rows)', v_row_count;
            RAISE NOTICE 'Load completion time: %', v_load_end - v_load_start;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to load bronze.crm_prd_info: %', SQLERRM;
                RAISE EXCEPTION 'Critical error in CRM product info load: %', SQLERRM;
        END;
        
        -- Load crm_sales_details
        BEGIN
            v_load_start := clock_timestamp();
            TRUNCATE TABLE bronze.crm_sales_details;
            RAISE NOTICE '>>> Truncating table: bronze.crm_sales_details';
            RAISE NOTICE '>>> Inserting Data into table: bronze.crm_sales_details';
            EXECUTE 'COPY bronze.crm_sales_details FROM ''' || v_crm_path || 'sales_details.csv'' WITH CSV HEADER';
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_load_end := clock_timestamp();
            RAISE NOTICE 'Table bronze.crm_sales_details successfully loaded (% rows)', v_row_count;
            RAISE NOTICE 'Load completion time: %', v_load_end - v_load_start;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to load bronze.crm_sales_details: %', SQLERRM;
                RAISE EXCEPTION 'Critical error in CRM sales details load: %', SQLERRM;
        END;
        
        RAISE NOTICE '';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE 'CRM Data Successfully Loaded';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE '';
        
        -- ==============================================
        -- Load ERP Data with individual error handling
        -- ==============================================
        RAISE NOTICE '=======================================================';
        RAISE NOTICE 'Beginning ERP Data Load';
        RAISE NOTICE '=======================================================';
        
        -- Load erp_CUST_AZ12
        BEGIN
            v_load_start := clock_timestamp();
            TRUNCATE TABLE bronze.erp_CUST_AZ12;
            RAISE NOTICE '>>> Truncating table: bronze.erp_CUST_AZ12';
            RAISE NOTICE '>>> Inserting Data into table: bronze.erp_CUST_AZ12';
            EXECUTE 'COPY bronze.erp_CUST_AZ12 FROM ''' || v_erp_path || 'CUST_AZ12.csv'' WITH CSV HEADER';
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_load_end := clock_timestamp();
            RAISE NOTICE 'Table bronze.erp_CUST_AZ12 successfully loaded (% rows)', v_row_count;
            RAISE NOTICE 'Load completion time: %', v_load_end - v_load_start;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to load bronze.erp_CUST_AZ12: %', SQLERRM;
                RAISE EXCEPTION 'Critical error in ERP customer load: %', SQLERRM;
        END;
        
        -- Load erp_LOC_A101
        BEGIN
            v_load_start := clock_timestamp();
            TRUNCATE TABLE bronze.erp_LOC_A101;
            RAISE NOTICE '>>> Truncating table: bronze.erp_LOC_A101';
            RAISE NOTICE '>>> Inserting Data into table: bronze.erp_LOC_A101';
            EXECUTE 'COPY bronze.erp_LOC_A101 FROM ''' || v_erp_path || 'LOC_A101.csv'' WITH CSV HEADER';
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_load_end := clock_timestamp();
            RAISE NOTICE 'Table bronze.erp_LOC_A101 successfully loaded (% rows)', v_row_count;
            RAISE NOTICE 'Load completion time: %', v_load_end - v_load_start;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to load bronze.erp_LOC_A101: %', SQLERRM;
                RAISE EXCEPTION 'Critical error in ERP location load: %', SQLERRM;
        END;
        
        -- Load erp_PX_CAT_G1V2  
        BEGIN
            v_load_start := clock_timestamp();
            TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
            RAISE NOTICE '>>> Truncating table: bronze.erp_PX_CAT_G1V2';
            RAISE NOTICE '>>> Inserting Data into table: bronze.erp_PX_CAT_G1V2';
            EXECUTE 'COPY bronze.erp_PX_CAT_G1V2 FROM ''' || v_erp_path || 'PX_CAT_G1V2.csv'' WITH CSV HEADER';
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_load_end := clock_timestamp();
            RAISE NOTICE 'Table bronze.erp_PX_CAT_G1V2 successfully loaded (% rows)', v_row_count;
            RAISE NOTICE 'Load completion time: %', v_load_end - v_load_start;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to load bronze.erp_PX_CAT_G1V2: %', SQLERRM;
                RAISE EXCEPTION 'Critical error in ERP product category load: %', SQLERRM;
        END;

        -- Calculate execution time
        v_end_time := clock_timestamp();
        
        RAISE NOTICE '';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE 'Bronze layer data load completed successfully!';
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
            RAISE NOTICE 'ERROR: Bronze layer data load FAILED!';
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
$$;
