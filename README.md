# Linnworks Data Pipeline

![Architecture](img/arhci_img.jpg)  
*Figure: Linnworks Data Pipeline Architecture*

## Overview
This project outlines the end-to-end process of **ingesting, transforming, and delivering Linnworks data** for reporting and analytics in Power BI. The pipeline ensures that raw data from Linnworks is properly processed, transformed, and made available for dashboards and business insights.

---

## Data Flow

1. **Data Ingestion**
   - Linnworks data flows through **streams and APIs** into **staging tables**.
   - Tools used: **Airbyte** and **Google Apps Scripts**.

2. **Data Transformation**
   - **ETL scripts** and **SQL job agents** process and transform the raw data.
   - Transformation occurs in **GCP SQL**, standardizing and cleaning data for analytics.

3. **Data Delivery**
   - Transformed data is loaded into **BigQuery**.
   - Selected tables are exposed for **Power BI dashboards**, enabling reporting and analytics.

---

## Project Structure

- **docs/**: Architecture diagrams and documentation.  
- **schema/**: Base DDL SQL scripts.  
- **migrations/**: Incremental database changes (versioned).  
- **seed/**: Initial or sample data scripts.  
- **sql/**: ETL SQL scripts grouped by extraction, transformation, and loading stages.  
- **notebooks/**: Jupyter notebooks for data exploration and validation.  
- **src/**: Python ETL source code (Extract, Transform, Load).  
- **config/**: Configuration files and environment templates.  
- **scripts/**: Utility scripts (backup, restore, pipeline run, etc.).  
- **tests/**: Unit tests and SQL tests.  
- **ci/**: CI/CD configurations.  
- **logs/**: Execution logs.  

---

## How to Use

1. Ensure access to the GCP SQL instance and BigQuery datasets.  
2. Run the **ETL scripts** in the order specified to populate staging and transformed tables.  
3. Verify data availability in BigQuery for Power BI dashboards.  

---

## TO: DO

Standard Naming nomenclature

