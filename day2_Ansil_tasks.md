#  Task 1: Environment Setup & Ingestion (Completed)

1. Folder Structure

Navigated to the Lakehouse explorer.

Created a new directory under the Files section to act as the landing zone.

Status: Created Files/Landing/SourceData/.


3. Uploaded CSVs

   
Selected the destination folder.

Used the Upload button and successfully brought in the 4 source CSV files.


5. Table Promotion (SQL Endpoint)


Right-clicked each CSV file and selected Load to Tables.

Defined unique table names for all 4 files.

Result: Verified that the tables have automatically synced and now appear under the dbo schema in the SQL analytics endpoint.



# Load table into DataFrame
df = spark.read.table("dbo.your_table_name")


folder structure and 4 tables created


<img width="200" height="300" alt="image" src="https://github.com/user-attachments/assets/be279ed5-083b-4e38-9931-5a8a720b46c4" />

task 2 :


validated the existance of tables

<img width="300" height="200" alt="image" src="https://github.com/user-attachments/assets/899d0f90-f772-4577-a1d6-8182ad805008" />


task 3:


creating dataframe from table


the spark code for making df from tables

df_dept = spark.read.table("departments")

df_dept.show(5)

df_dept.printSchema()


<img width="300" height="200" alt="image" src="https://github.com/user-attachments/assets/3ea9c419-82b6-4644-aa41-c0f2b7538271" />



# Data Processing Pipeline: Departments Dataset

**Target Table:** `curated_departments` (Delta)  
**Purpose:** Clean, standardize, and enrich raw department data for analytics.

---

### Task 1 – Remove duplicate department records
**Purpose:** Ensure uniqueness by business key.
```python
df_dept = df_dept.dropDuplicates(["department_id"])
