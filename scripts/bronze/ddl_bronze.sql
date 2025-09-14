/*
Script Purpose:
    The following script is designed to create database tables under the bronze schema.
    The existence of tables is checked first before new tables are created. 
    Tables are formatted in accordance with the source data.

WARNING:
    Running this script will delete tables and all their contents. Please ensure that your date
    is backed up before running this script.
*/


-- ==============================================
-- Create Tables for Source System crm
-- ==============================================

-- Create table for source file: 'cust_info.csv'
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR (50),
    cst_firstname VARCHAR (50),
    cst_lastname VARCHAR (50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

-- Create table for source file: 'prd_info.csv'
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR (50),
    prd_nm VARCHAR (100),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

-- Create table for source file: 'sales_details.csv'
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num VARCHAR (50),
    sls_prd_key VARCHAR (50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- ==============================================
-- Create Tables for Source System erp
-- ==============================================

-- Create table for source file: 'CUST_AZ12.csv'
DROP TABLE IF EXISTS bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12 (
    CID VARCHAR (50),
    BDATE DATE,
    GEN VARCHAR (25)
);

-- Create table for source file: 'LOC_A101.csv'
DROP TABLE IF EXISTS bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101 (
    CID VARCHAR (50),
    CNTRY VARCHAR (50)
);

-- Create table for source file: 'PX_CAT_G1V12.csv'
DROP TABLE IF EXISTS bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2 (
    ID VARCHAR (50),
    CAT VARCHAR (50),
    SUBCAT VARCHAR (50),
    MAINTENANCE VARCHAR (50)
);
