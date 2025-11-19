### Data Warehouse And Analyticts Project
Welcome to the Data Warehouse and Analytics Project repository! 🚀
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data
warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best
practices in data engineering and analytics.

---

### 🏗️ Data Architecture 
The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers: 
This project involves:
1.Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into Postgresql Database.

2.Silver Layer:  This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.

3.Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.

---

### 📖 Project Overview
1.Data Architecture: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
2.ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse.
3.Data Modeling: Developing fact and dimension tables optimized for analytical queries.
4.Analytics & Reporting: Creating SQL-based reports and dashboards for actionable insights.

---
###🗄 Schema Design & Folder Structure

PostgreSQL Schemas

bronze   → raw ingestion  
silver   → transformed, cleaned  
gold     → dimension & fact tables

---
### GitHub Repository Structure

📁 data-warehouse-project
 ├── 📁 datasets
 │     ├── crm.csv
 │     └── prd.csv
 ├── 📁 bronze
 ├── 📁 silver
 ├── 📁 gold
 ├── 📁 scripts
 │     └── load_procedures.sql
 ├── 📁 logs
 ├── 📁 docs
 └── README.md
 
---

### ⚙ ETL / ELT Process

Bronze Load
	•	Load raw CSVs using COPY or tool import
	•	TRUNCATE + INSERT pattern
	•	No cleaning

Silver Transform
	•	Run stored procedure silver.load_silver()
	•	Cleans & standardizes data
	•	Applies all business rules
	•	Creates consistent product keys
	•	Removes duplicates
	•	Builds end-date windows using LEAD()

Gold Build
	•	Build dim and fact tables
	•	Implement surrogate keys
	•	Join silver tables
	•	Create star schema for analytics
  
---

### 🧰 Technologies Used
	•	PostgreSQL
	•	SQL / PL/pgSQL
	•	pgAdmin / DBeaver
	•	Git & GitHub
	•	VS Code
	•	CSV Data Sources

---

### 👩‍💻 About Me

I am a Data Scientist who transforms raw data into reliable insights and meaningful solutions. I work with SQL, data pipelines, and machine learning to build clean datasets, strong analytical models, and structures that help businesses make better decisions.

---


