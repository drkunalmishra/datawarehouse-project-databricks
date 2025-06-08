-- Using the Databricks community version to build the lakehouse
-- Hence Can't use Schema or Catalog option 

-- Drop databases (schemas) if they exist
DROP DATABASE IF EXISTS bronze CASCADE;
DROP DATABASE IF EXISTS silver CASCADE;
DROP DATABASE IF EXISTS gold CASCADE;

-- Create databases (schemas)
CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Bronze Layer - Raw Data';
CREATE DATABASE IF NOT EXISTS silver COMMENT 'Silver Layer - Cleansed Data';
CREATE DATABASE IF NOT EXISTS gold COMMENT 'Gold Layer - Curated Business Data';
