# Data Warehouse and Analytics Project

Hello, and welcome to the **Data Warehouse and Analytics Project** repo!
This project is designed to demonstrate modern data warehousing techniques and analytics solutions, delivering accurate and actionable insights. This project highlights industry best practices for data engineering and analytics.

## **Project Requirements**

---

### Building the Data Warehouse - Data Engineering

**Objective**

Develop a modern data warehouse using PostgreSQL to consolidate sales data, enabling analytical reporting and informed decision-making.

**Specifications**

- **Data Sources:** Import data from two source systems (ERP and CRM), provided as CSV files.
- **Data Quality:** Cleanse and resolve data quality issues prior to analysis
- **Integration:** Combine both sources into a single, user-friendly data model designed for analytical queries
- **Scope:** Focus on the latest dataset only; Historization is not required
- **Documentation:** Provide clear documentation of the data model to support both business stakeholders and analytics teams

---

### BI: Analytics and Reporting - Data Analytics

**Objective**

**Develop SQL-based analytics to deliver detailed insights into:**

- Customer Behaviour
- Product Performance
- Sales Trends

These insights empower stakeholders with key business metrics, enabling strategic decision-making.

---

### Data Warehouse Architecture

<img width="961" height="581" alt="data_architecture_design" src="https://github.com/user-attachments/assets/4b3b5596-1e8e-4784-b2aa-c948427b8a25" />

The architecture of this data warehouse follows the "Bronze, Silver, Gold" approach, consisting of a bronze layer that ingests the data, but does not perform any transformations. Next is the Silver layer, which cleans the Bronze layer data while maintaining the structure of the source systems. Finally, the Gold layer utilizes the Silver layer's data to create business-ready views, which are ready for analytics and other use cases.

---

### Data Integration Process

<img width="788" height="573" alt="data_integration drawio" src="https://github.com/user-attachments/assets/fc70150b-bd54-4e73-8c96-0ac6d1c3ef9b" />

The above model shows how data from the different sources will be integrated and connected.

---

### Gold Layer Data Model

<img width="752" height="584" alt="data_model drawio" src="https://github.com/user-attachments/assets/08e2b92c-2518-47cd-ade3-272186768c91" />

The Gold layer uses the "Star Schema", where the central fact table (fact_sales) is connected to separate dimension tables (dim_customers and dim_products).

---

### Usage

To use this Data Warehouse: 
- Initialize your database, creating the appropriate schemas. Run script 'init_database.sql' in the 'scripts' directory.
- To execute the full ETL process, call 'etl.load_dwh()'. This will run bronze, silver, and gold processes in succession with a single call **[Recommended]**.
- Alternatively, to run bronze, silver, and gold processes separately, call 'bronze.load_bronze()', 'silver.load_silver()', and 'gold.load_gold()' respectively.

---



## License

This project is licensed under the [MIT License]. You are free to use, modify, and share this project with proper attribution.

---

### Repository Structure

```text
sql_data_warehouse_project/
|-- datasets/
|   |-- source_crm/
|   |   |-- cust_info.csv
|   |   |-- prd_info.csv
|   |   |-- sales_details.csv
|   |-- source_erp/
|       |-- CUST_AZ12.csv
|       |-- LOC_A101.csv
|       |-- PX_CAT_G1V2.csv
|-- docs/
|   |-- data_architecture_design.png
|   |-- data_catalog.md
|   |-- data_flow_gold.drawio.png
|   |-- data_integration.drawio.png
|   |-- data_model.drawio.png
|-- scripts/
|   |-- bronze/
|   |   |-- ddl_bronze.sql
|   |   |-- proc_load_bronze.sql
|   |-- gold/
|   |   |-- proc_load_gold.sql
|   |-- silver/
|   |   |-- ddl_silver.sql
|   |   |-- proc_load_silver.sql
|   |-- init_database.sql
|   |-- proc_load_dwh.sql
|-- tests/
|   |-- bronze/
|   |   |-- val_bronze_load_bronze.sql 
|   |-- gold/
|   |   |-- val_gold_all.sql
|   |-- silver/
|   |   |-- validations_crm/
|   |   |   |-- val_silver_crm_cust_info.sql
|   |   |   |-- val_silver_crm_prd_info.sql
|   |   |   |-- val_silver_crm_sales_details.sql
|   |   |-- validations_erp/
|   |       |-- val_silver_erp_cust_az12.sql
|   |       |-- val_silver_erp_loc_a101.sql
|   |       |-- val_silver_erp_px_cat_g1v2.sql
|-- LICENSE
|-- README.md

