-- CRM Customer Info
DELETE FROM silver.crm_cust_info;

INSERT INTO silver.crm_cust_info
SELECT
  cst_id,
  cst_key,
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname) AS cst_lastname,
  CASE 
    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
    ELSE 'n/a'
  END AS cst_marital_status,
  CASE 
    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
    ELSE 'n/a'
  END AS cst_gndr,
  cst_create_date,
  current_timestamp() AS dwh_create_date
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
  FROM bronze.crm_cust_info
  WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;

-- CRM Product Info
DELETE FROM silver.crm_prd_info;

INSERT INTO silver.crm_prd_info
SELECT
  prd_id,
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
  prd_nm,
  COALESCE(prd_cost, 0) AS prd_cost,
  CASE 
    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    ELSE 'n/a'
  END AS prd_line,
  CAST(prd_start_dt AS DATE) AS prd_start_dt,
  CAST(
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY 
    AS DATE
  ) AS prd_end_dt,
  current_timestamp() AS dwh_create_date
FROM bronze.crm_prd_info;

-- CRM Sales Details
DELETE FROM silver.crm_sales_details;

INSERT INTO silver.crm_sales_details
SELECT
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  CASE 
    WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 THEN NULL
    ELSE TO_DATE(CAST(sls_order_dt AS STRING), 'yyyyMMdd')
  END AS sls_order_dt,
  CASE 
    WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 THEN NULL
    ELSE TO_DATE(CAST(sls_ship_dt AS STRING), 'yyyyMMdd')
  END AS sls_ship_dt,
  CASE 
    WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 THEN NULL
    ELSE TO_DATE(CAST(sls_due_dt AS STRING), 'yyyyMMdd')
  END AS sls_due_dt,
  CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
      THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
  END AS sls_sales,
  sls_quantity,
  CASE 
    WHEN sls_price IS NULL OR sls_price <= 0 
      THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
  END AS sls_price,
  current_timestamp() AS dwh_create_date
FROM bronze.crm_sales_details;

-- ERP Customer AZ12
DELETE FROM silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12
SELECT
  CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
    ELSE cid
  END AS cid,
  CASE
    WHEN bdate > current_date() THEN NULL
    ELSE bdate
  END AS bdate,
  CASE
    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
  END AS gen,
  current_timestamp() AS dwh_create_date
FROM bronze.erp_cust_az12;

-- ERP Location A101
DELETE FROM silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101
SELECT
  REPLACE(cid, '-', '') AS cid,
  CASE
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
  END AS cntry,
  current_timestamp() AS dwh_create_date
FROM bronze.erp_loc_a101;

-- ERP Product Category G1V2
DELETE FROM silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2
SELECT
  id,
  cat,
  subcat,
  maintenance,
  current_timestamp() AS dwh_create_date
FROM bronze.erp_px_cat_g1v2;

