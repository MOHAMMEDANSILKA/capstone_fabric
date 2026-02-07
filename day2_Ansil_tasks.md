Fabric Data Engineering: CSV Ingestion & Transformation
📂 Task 1: Setup & Ingestion
Folder Structure: In the Lakehouse Explorer, right-click Files to create a RawData folder.
Upload: Upload your 4 CSV files into the new directory using the Microsoft Fabric Portal.
Table Creation: Right-click each CSV > Load to Tables. This registers them as Delta tables.
SQL Discovery: Navigate to the SQL analytics endpoint; the tables automatically sync under the dbo schema.
🛠️ Task 2: Data Clean


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
