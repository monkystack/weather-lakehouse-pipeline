# Weather Data Lakehouse Pipeline on Azure

## Overview
This project is built to practice and understand how a real-world data engineering pipeline works on Azure. It focuses on setting up an end-to-end lakehouse architecture using Data Factory and Databricks.

## Architecture
Weather API → Azure Data Factory → Azure Databricks → Delta Lake (Bronze, Silver, Gold) → Power BI

## Tech Stack
- Azure Data Factory
- Azure Databricks
- Delta Lake
- Unity Catalog
- Power BI

## Execution Environment
This project is designed to run on Azure Databricks.

- Uses dbutils for file system operations and file management  
- Secrets are managed via Databricks Secret Scopes (integrated with Azure Key Vault)  
- Originally developed in Databricks notebooks and refactored into Python scripts for better version control and project organization

## Pipeline Design

### Bronze Layer
- Ingest raw weather data from API
- Store data in Delta format

### Silver Layer
- Perform basic data cleaning
- Handle missing values and simple transformations

### Gold Layer
- Prepare aggregated data for reporting
- Optimize data for Power BI usage

## Output
- Analytics-ready dataset
- Simple Power BI dashboard

## Key Learnings
- Built incremental data ingestion pipeline using Data Factory
- Practiced Medallion Architecture in Databricks
- Explored data governance with Unity Catalog
