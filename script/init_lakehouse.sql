-- Using the Databricks community version to build the lakehouse
-- USE CATALOG main;

-- Drop database if exists (CASCADE will remove all tables inside)
DROP DATABASE IF EXISTS DataWarehouse CASCADE;

-- Create database
CREATE DATABASE IF NOT EXISTS DataWarehouse
COMMENT 'This is the DataWarehouse database with bronze, silver, and gold schemas';

-- Create schemas (Databricks schemas = SQL Server schemas inside the database)
CREATE SCHEMA IF NOT EXISTS DataWarehouse.bronze;
CREATE SCHEMA IF NOT EXISTS DataWarehouse.silver;
CREATE SCHEMA IF NOT EXISTS DataWarehouse.gold;
