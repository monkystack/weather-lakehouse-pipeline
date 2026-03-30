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
