-- Snowflake DDL for Intermediate Vaccination Records Table

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS VACCINATION_DATA;

-- Create intermediate table for vaccination records
CREATE OR REPLACE TABLE VACCINATION_DATA.INTERMEDIATE_VACCINATION_RECORDS (
    -- Unique identifier for the customer
    CUST_I VARCHAR(50) NOT NULL,
    
    -- Customer's full name
    NAME VARCHAR(255) NOT NULL,
    
    -- Date the record was opened/created
    OPEN_DT DATE NOT NULL,
    
    -- Last consultation date (can be null)
    CONSUL_DT DATE,
    
    -- Vaccination ID or type
    VAC_ID VARCHAR(50),
    
    -- Doctor's name
    DR_NAME VARCHAR(255),
    
    -- State of residence
    STATE VARCHAR(100),
    
    -- County of residence
    COUNTRY VARCHAR(100),
    
    -- Date of Birth
    DOB DATE,
    
    -- Active status flag
    FLAG VARCHAR(10),
    
    -- Metadata columns for tracking
    LOAD_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE VARCHAR(255)
)
COMMENT = 'Intermediate table for validated vaccination records from multiple sources'
