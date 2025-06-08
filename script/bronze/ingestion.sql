from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

# CRM Customer Info Schema
crm_cust_info_schema = StructType([
    StructField("cst_id", IntegerType(), True),
    StructField("cst_key", StringType(), True),
    StructField("cst_firstname", StringType(), True),
    StructField("cst_lastname", StringType(), True),
    StructField("cst_marital_status", StringType(), True),
    StructField("cst_gndr", StringType(), True),
    StructField("cst_create_date", DateType(), True)
])

# Read CRM Customer Info CSV
crm_cust_info_df = spark.read.csv("/FileStore/bronze_data/crm_cust_info.csv", 
                                  header=True, 
                                  schema=crm_cust_info_schema)

# Overwrite data into Delta table
crm_cust_info_df.write.format("delta").mode("overwrite").saveAsTable("bronze.crm_cust_info")

# -------------------------------------------------

# CRM Product Info Schema
crm_prd_info_schema = StructType([
    StructField("prd_id", IntegerType(), True),
    StructField("prd_key", StringType(), True),
    StructField("prd_nm", StringType(), True),
    StructField("prd_cost", IntegerType(), True),
    StructField("prd_line", StringType(), True),
    StructField("prd_start_dt", TimestampType(), True),
    StructField("prd_end_dt", TimestampType(), True)
])

crm_prd_info_df = spark.read.csv("/FileStore/bronze_data/crm_prd_info.csv",
                                 header=True,
                                 schema=crm_prd_info_schema)

crm_prd_info_df.write.format("delta").mode("overwrite").saveAsTable("bronze.crm_prd_info")

# -------------------------------------------------

# CRM Sales Details Schema
crm_sales_details_schema = StructType([
    StructField("sls_ord_num", StringType(), True),
    StructField("sls_prd_key", StringType(), True),
    StructField("sls_cust_id", IntegerType(), True),
    StructField("sls_order_dt", IntegerType(), True),
    StructField("sls_ship_dt", IntegerType(), True),
    StructField("sls_due_dt", IntegerType(), True),
    StructField("sls_sales", IntegerType(), True),
    StructField("sls_quantity", IntegerType(), True),
    StructField("sls_price", IntegerType(), True)
])

crm_sales_details_df = spark.read.csv("/FileStore/bronze_data/crm_sales_details.csv",
                                      header=True,
                                      schema=crm_sales_details_schema)

crm_sales_details_df.write.format("delta").mode("overwrite").saveAsTable("bronze.crm_sales_details")

# -------------------------------------------------

# ERP Location Schema
erp_loc_a101_schema = StructType([
    StructField("cid", StringType(), True),
    StructField("cntry", StringType(), True)
])

erp_loc_a101_df = spark.read.csv("/FileStore/bronze_data/erp_loc_a101.csv",
                                 header=True,
                                 schema=erp_loc_a101_schema)

erp_loc_a101_df.write.format("delta").mode("overwrite").saveAsTable("bronze.erp_loc_a101")

# -------------------------------------------------

# ERP Customer Schema
erp_cust_az12_schema = StructType([
    StructField("cid", StringType(), True),
    StructField("bdate", DateType(), True),
    StructField("gen", StringType(), True)
])

erp_cust_az12_df = spark.read.csv("/FileStore/bronze_data/erp_cust_az12.csv",
                                  header=True,
                                  schema=erp_cust_az12_schema)

erp_cust_az12_df.write.format("delta").mode("overwrite").saveAsTable("bronze.erp_cust_az12")

# -------------------------------------------------

# ERP Product Category Schema
erp_px_cat_g1v2_schema = StructType([
    StructField("id", StringType(), True),
    StructField("cat", StringType(), True),
    StructField("subcat", StringType(), True),
    StructField("maintenance", StringType(), True)
])

erp_px_cat_g1v2_df = spark.read.csv("/FileStore/bronze_data/erp_px_cat_g1v2.csv",
                                    header=True,
                                    schema=erp_px_cat_g1v2_schema)

erp_px_cat_g1v2_df.write.format("delta").mode("overwrite").saveAsTable("bronze.erp_px_cat_g1v2")
