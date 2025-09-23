/*
===========================
Create Database and Schemas
===========================

Script Purpose:
This script creates a new database 'data_warehouse' after dropping an already existing one 
which might share the same name. If one exists already, it is dropped and recreated. Additionaly,
the script creates three new schemas within the database: 'bronze', 'silver', and 'gold'

WARNING:
Running this script will drop any existing database named 'data_warehouse' by default. All data in such a database
will be permanently deleted. It is highly recommended to choose a new name for database creation
to avoid data deletion and overwrite. Please proceed with caution and ensure your existing data is
backed up where necessary before running this script.
 */
-- Create new database called 'data_warehouse'
DROP DATABASE IF EXISTS data_warehouse
;


-- Use appropriate name if necessary
CREATE DATABASE data_warehouse
;


-- Use appropriate name if necessary
-- Multiple schemas needed: Bronze, Silver, Gold
-- Create bronze schema
CREATE SCHEMA bronze
;

-- Create silver schema
CREATE SCHEMA silver
;


-- Create gold schema
CREATE SCHEMA gold
;

-- Create etl schema for procedure
CREATE SCHEMA etl
;
