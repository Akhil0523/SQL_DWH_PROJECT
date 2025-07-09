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

## 📁 Repository Structure
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── etl.drawio                      # Draw.io file shows all different techniquies and methods of ETL
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
└── requirements.txt                    # Dependencies and requirements for the project

## 🧰 Tools & Technologies Used

- **MySQL Server 8.x**  
- **MySQL Workbench** (for schema design and query execution)  
- **Draw.io** (for ERD and data flow diagrams)  
- **Notion** (for documentation and project planning)  
- **GitHub** (for version control and collaboration)

---

## 🧪 Sample Queries & Analytics

```sql
-- Total sales by product category
SELECT c.category_name, SUM(f.sales_amount) AS total_sales
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_category c ON p.category_id = c.category_id
GROUP BY c.category_name
ORDER BY total_sales DESC;

 Business Insights Generated
   -Top-performing products
   -Customer retention trends
   -Sales analysis by month and region
   -Conversion rates from CRM leads to ERP sales

✅ How to Run This Project
  -Install MySQL Server & Workbench
  -Clone the repository:
    git clone https://github.com/yourusername/mysql-data-warehouse.git
    cd mysql-data-warehouse
  -Import the CSVs from /datasets into Bronze tables using:
    LOAD DATA INFILE 'path_to_file.csv' 
    INTO TABLE bronze_erp 
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n' 
    IGNORE 1 ROWS;
  -Run scripts in the following order:
    bronze/*.sql
    silver/*.sql
    gold/*.sql
  -Test your queries using SQL from gold/analytics.sql.

🧾 Requirements
  MySQL Server 8.x
  MySQL Workbench
  CSV dataset files
  Draw.io (for opening .drawio diagrams)

📘 Documentation
  Available in the docs/ folder:
  data_architecture.drawio – Project architecture
  data_models.drawio – Star schema ERD
  data_flow.drawio – Data flow pipeline
  data_catalog.md – Field definitions and metadata
  naming-conventions.md – Standards for table/column names

📫 Reach me on-
      LinkedIn:https://www.linkedin.com/in/akhileshdwivediworks/

📜 License
This project is licensed under the MIT License. You can freely use, modify, and distribute it with proper credit.
