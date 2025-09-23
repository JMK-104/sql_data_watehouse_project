/*
Script Purpose:
This stored procedure is designed to orchestrate the entire ETL pipeline:
1. Load raw data into Bronze layer
2. Transform and load data into Silver layer
3. Create Gold layer views for presentation

Usage:
CALL etl.load_dwh();  -- Uses default base path;
-- Optionally with a custom base path for Bronze layer
CALL etl.load_dwh('/your/custom/path');

WARNING:
Running this script will truncate and reload data in Bronze and Silver tables
and recreate all Gold views. Ensure you are aware of downstream impacts.
 */
-- ==============================================
-- Stored Procedure: Run Full ETL Pipeline
-- Description: Executes Bronze, Silver, and Gold procedures sequentially
-- ==============================================
CREATE
OR REPLACE PROCEDURE etl.load_dwh (
    p_base_path TEXT DEFAULT '/Users/justinkakuyo/Desktop/Dev/projects/SQL/sql-data-warehouse-project/datasets'
) LANGUAGE plpgsql AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time   TIMESTAMP;
    v_error_message TEXT;
    v_error_detail  TEXT;
    v_error_hint    TEXT;
    v_error_context TEXT;
BEGIN
    -- Initialize timing
    v_start_time := clock_timestamp();

    RAISE NOTICE '=======================================================';
    RAISE NOTICE 'Starting Full ETL Pipeline at %', v_start_time;
    RAISE NOTICE '=======================================================';

    BEGIN
        -- Step 1: Bronze Layer
        RAISE NOTICE '';
        RAISE NOTICE '>>> Running Bronze Layer Load...';
        CALL bronze.load_bronze(p_base_path);
        RAISE NOTICE '>>> Bronze Layer completed successfully.';

        -- Step 2: Silver Layer
        RAISE NOTICE '';
        RAISE NOTICE '>>> Running Silver Layer Load...';
        CALL silver.load_silver();
        RAISE NOTICE '>>> Silver Layer completed successfully.';

        -- Step 3: Gold Layer
        RAISE NOTICE '';
        RAISE NOTICE '>>> Running Gold Layer Load...';
        CALL gold.load_gold();
        RAISE NOTICE '>>> Gold Layer completed successfully.';

        -- Wrap up
        v_end_time := clock_timestamp();
        RAISE NOTICE '';
        RAISE NOTICE '=======================================================';
        RAISE NOTICE 'Full ETL Pipeline completed successfully!';
        RAISE NOTICE 'Total execution time: %', v_end_time - v_start_time;
        RAISE NOTICE '=======================================================';

    EXCEPTION
        WHEN OTHERS THEN
            -- Capture error details
            GET STACKED DIAGNOSTICS
                v_error_message = MESSAGE_TEXT,
                v_error_detail = PG_EXCEPTION_DETAIL,
                v_error_hint   = PG_EXCEPTION_HINT,
                v_error_context = PG_EXCEPTION_CONTEXT;

            v_end_time := clock_timestamp();

            RAISE NOTICE '=======================================================';
            RAISE NOTICE 'ERROR: Full ETL Pipeline FAILED!';
            RAISE NOTICE 'Execution time before failure: %', v_end_time - v_start_time;
            RAISE NOTICE 'Error Message: %', v_error_message;
            RAISE NOTICE 'Error Detail: %', COALESCE(v_error_detail, 'No additional detail');
            RAISE NOTICE 'Error Hint: %', COALESCE(v_error_hint, 'No hint available');
            RAISE NOTICE 'Error Context: %', COALESCE(v_error_context, 'No context available');
            RAISE NOTICE '=======================================================';

            -- Re-raise exception
            RAISE;
    END;
END;
$$
;