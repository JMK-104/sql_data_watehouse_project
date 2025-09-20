/*
Script Purpose:
    The following script is designed to create database tables under the silver schema.
    The existence of tables is checked first before new tables are created. 
    Tables are formatted for data warehouse silver layer processing.
WARNING:
    Running this script will delete tables and all their contents. Please ensure that your data
    is backed up before running this script.
*/
-- ==============================================
-- Create Tables for Source System crm
-- ==============================================
-- Create table for CRM customer information
DROP TABLE IF EXISTS silver.crm_cust_info
;
CREATE TABLE
    silver.crm_cust_info (
        cst_id INT,
        cst_key VARCHAR(50),
        cst_firstname VARCHAR(50),
        cst_lastname VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr VARCHAR(50),
        cst_create_date DATE,
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
;
-- Create table for CRM product information
DROP TABLE IF EXISTS silver.crm_prd_info
;
CREATE TABLE
    silver.crm_prd_info (
        prd_id INT,
        cat_id VARCHAR(50),
        prd_key VARCHAR(50),
        prd_nm VARCHAR(50),
        prd_cost INT,
        prd_line VARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt DATE,
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
;
-- Create table for CRM sales details
DROP TABLE IF EXISTS silver.crm_sales_details
;
CREATE TABLE
    silver.crm_sales_details (
        sls_ord_num VARCHAR(50),
        sls_prd_key VARCHAR(50),
        sls_cust_id INT,
        sls_order_dt DATE,
        sls_ship_dt DATE,
        sls_due_dt DATE,
        sls_sales INT,
        sls_quantity INT,
        sls_price INT,
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
;
-- ==============================================
-- Create Tables for Source System erp
-- ==============================================
-- Create table for ERP location data
DROP TABLE IF EXISTS silver.erp_loc_a101
;
CREATE TABLE
    silver.erp_loc_a101 (
        cid VARCHAR(50),
        cntry VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
;
-- Create table for ERP customer data
DROP TABLE IF EXISTS silver.erp_cust_az12
;
CREATE TABLE
    silver.erp_cust_az12 (
        cid VARCHAR(50),
        bdate DATE,
        gen VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
;
-- Create table for ERP product category data
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2
;
CREATE TABLE
    silver.erp_px_cat_g1v2 (
        id VARCHAR(50),
        cat VARCHAR(50),
        subcat VARCHAR(50),
        maintenance VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
;
