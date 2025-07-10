# SQL_DWH_PROJECT
Building a moder Data Warehouse with MySQL,including data extraction,data transformations,data loading ,Building Data Architectures and approaches ,data modelling and data analytics .
# 🏢 MySQL-Based Data Warehouse & Analytics Project

Welcome to the **MySQL Data Warehouse & Analytics** repository. This project demonstrates how to build a **modern data warehouse** using **MySQL**, transforming raw operational data into actionable insights. It follows professional data engineering principles using a **layered Medallion Architecture**.

## 🧱 Project Architecture: Medallion Design

This warehouse follows a three-layer **Medallion Architecture**:

### 🔹 Bronze Layer (Raw Data)
- Direct import of raw ERP and CRM CSV files into MySQL.
- Data remains unchanged to preserve lineage.

### 🔸 Silver Layer (Staging / Refined)
- Cleansed and standardized data using MySQL queries.
- Duplicates removed, nulls handled, and data formats aligned.

### 🟡 Gold Layer (Analytical)
- Dimensional modeling using **Star Schema**.
- Fact and dimension tables ready for BI tools and SQL analytics.

---

## 🚀 Project Objectives

- Develop a scalable **MySQL-based data warehouse**.
- Integrate multiple data sources for unified analytics.
- Enable SQL-driven insights using clean, modeled data.
- Simulate real-world data pipelines and reporting scenarios.

---

This project is structured into three main phases:
1. Building the data warehouse
2. Performing advanced SQL-based analysis on the prepared dataset
3. Visualizing the analytical insights using Power BI

PHASE 1: **BUILDING THE DATAWAREHOUSE**

We adopted a data warehouse approach, as our data was structured and our primary goal was to build a robust foundation for reporting and business intelligence.Datawarehouse is basically a subject oriented, integrated, time variant and non-volatile cllection of data in support of management's decision making process.
![https://acuto.io/blog/data-warehouse-architecture-types/](documents/Datawarehouse_architecture.png)

We used the Medallion Architecture to structure our project’s data pipeline.
![https://www.oreilly.com/library/view/delta-lake-up/9781098139711/ch01.html](documents/Medallion_architecture.png)

STEP 1: We created 3 different schemas(namely, bronze, silver and gold).

STEP 2: Create DDL scripts for all CSV files in the CRM and ERP Systems in the bronze schema.

STEP 3: Load data into the tables of bronze schema. We wanted to do bulk inserts inside a store procedure but MySQL doesn't actually supports bulk insertion inside a stored procedure to avoid SQL injection, so we shifted to running the script instead to do the desired work.

STEP 4: In order to identify bottlenecks and optimize performance, we declared required variables to track duration of each ETL step.

STEP 5: We now moved to silver schema and created table accordingly with the same structure like that in the bronze schema.

STEP 6: Here, we added an extra metadata column called dwh_create_date in order to have an eye on when was the current data loaded.

STEP 7: Before moving tothe gold layer, we need to first check the quality of our data in our silver layer like 
1. Check for null values.
2. Check for unwanted spaces in string values.
3. Check for consistency of values in low cardinality columns,etc

STEP 8: In the gold layer, we modelled our data according to the star schema.

STEP 9: Perform quality checks on the gold layer data, since NULLS often come after joining tables, etc.

PHASE 2: **Performing advanced SQL-based analysis on the prepared dataset**


   
📫 Reach me on-
      LinkedIn:https://www.linkedin.com/in/akhileshdwivediworks/

📜 License
This project is licensed under the MIT License. You can freely use, modify, and distribute it with proper credit.
