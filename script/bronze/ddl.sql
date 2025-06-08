-- Make sure the bronze database exists
CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Bronze layer for raw data ingestion';

-- Create Delta table bronze.crm_cust_info
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             STRING,
    cst_firstname       STRING,
    cst_lastname        STRING,
    cst_marital_status  STRING,
    cst_gndr            STRING,
    cst_create_date     DATE
) USING DELTA;

-- Create Delta table bronze.crm_prd_info
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      STRING,
    prd_nm       STRING,
    prd_cost     INT,
    prd_line     STRING,
    prd_start_dt TIMESTAMP,
    prd_end_dt   TIMESTAMP
) USING DELTA;

-- Create Delta table bronze.crm_sales_details
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  STRING,
    sls_prd_key  STRING,
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
) USING DELTA;

-- Create Delta table bronze.erp_loc_a101
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid    STRING,
    cntry  STRING
) USING DELTA;

-- Create Delta table bronze.erp_cust_az12
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid    STRING,
    bdate  DATE,
    gen    STRING
) USING DELTA;

-- Create Delta table bronze.erp_px_cat_g1v2
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           STRING,
    cat          STRING,
    subcat       STRING,
    maintenance  STRING
) USING DELTA;
