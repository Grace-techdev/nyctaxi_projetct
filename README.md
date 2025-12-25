# 🚕 NYC Taxi Data Engineering Project (Azure Databricks)

## Overview
This project implements an end-to-end data engineering pipeline on Azure Databricks using NYC Taxi Trip data.

It is based on the Udemy course
**“Azure Databricks and Spark SQL (Python)”** by Malvik Vaghadia

https://www.udemy.com/course/azure-databricks-and-spark-sql-python/

While the original course focuses on processing Yellow Taxi data, this project builds on the same architectural patterns and extends them to an additional dataset (Green Taxi), creating a multi-source pipeline with shared downstream processing.

## Architecture & Data Flow
The pipeline follows a **Medallion Architecture**:
```
Landing → Bronze → Silver → Gold → Export
```
- Incremental monthly ingestion of NYC Yellow and Green Taxi data
- Parallel pipelines for multiple taxi types, with schema-aware processing
- Schema differences handled in the Silver layer using shared transformation logic
- Datasets merged downstream for analytics (Gold) and external delivery (Export)
- Orchestrated using a Databricks Jobs DAG with conditional downstream execution
- Partitioned JSON export to Azure Data Lake Storage Gen2 via Unity Catalog
    
A screenshot of the Databricks Job DAG is included in this repository.

## Technologies Used
- Databricks (Jobs, Workflows, Unity Catalog)
- Apache Spark (PySpark) 
- Delta Lake  
- Azure Data Lake Storage Gen2 
- GitHub
