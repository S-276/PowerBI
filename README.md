# 🌟 Web Log BI Project - Star Schema & Sun Model  
This project implements a Business Intelligence system for web log analysis using SQL Server, Power BI, and future-ready scalability practices.

## 📄 Project Overview  
- **Star Schema:** SQL Server-based Fact & Dimension tables.  
- **Sun Model:** Visual design of relationships.  
- **Big Data Ready:** Partitioning, Indexing, and ETL.  
- **Visualization:** Power BI dashboards.

## 📂 Folder Structure  
📁 WebLog-BI-Project
├─ 🟢 star_schema_sun_model.sql # SQL for Star Schema
├─ 📊 WebLog_Analysis.pbix # Power BI Dashboard
├─ 📝 Part1_SunModel_Report.docx # Report for Submission
├─ 📷 sun_model_diagram.png # Sun Model Diagram
└─ 📘 README.md # Project Documentation

## 🚀 How to Run  
1. **SQL Server:**  
   - Restore `WebLogDW` database.  
   - Run `star_schema_sun_model.sql` to create schema.  
2. **Power BI:**  
   - Open `WebLog_Analysis.pbix`.  
   - Refresh connection.  

## 🤖 Future Scalability  
- **Partitioning:** Fact table by `RequestDate`.  
- **Indexing:** On foreign keys for faster querying.  
- **Incremental ETL:** Change Data Capture (CDC).  

---

💡 *For more details, refer to the full project documentation.*  
