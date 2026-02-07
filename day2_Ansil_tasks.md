agenda 


Uploade CSVs in Fabric

Create tables from CSV

table appear under dbo (SQL endpoint)

 create DataFrames and clean data
 

 steps:
 
task 1: 


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
**Purpose:** Clean, standardize, and enrich raw department data for downstream analytics.

---

### Task 1: Remove Duplicate Department Records
**Purpose:** Ensure uniqueness by business key.
```python
df_dept = df_dept.dropDuplicates(["department_id"])
Use code with caution.

Task 2: Eliminate Records with Missing department_id
Purpose: Protect primary key integrity.
python
df_dept = df_dept.filter(col("department_id").isNotNull())
Use code with caution.

Task 3: Trim All String Columns
Purpose: Remove unnecessary leading/trailing whitespace.
python
df_dept = df_dept \
    .withColumn("department_name", trim(col("department_name"))) \
    .withColumn("location", trim(col("location")))
Use code with caution.

Task 4: Standardize Department Name (Uppercase)
Purpose: Consistent casing for reporting and joins.
python
df_dept = df_dept.withColumn("department_name", upper(col("department_name")))
Use code with caution.

Task 5: Standardize Location Formatting
Purpose: Apply title case for improved readability.
python
df_dept = df_dept.withColumn("location", initcap(col("location")))
Use code with caution.

Task 6: Cast manager_id to Integer
Purpose: Enforce correct data type for numeric IDs.
python
df_dept = df_dept.withColumn("manager_id", col("manager_id").cast("int"))
Use code with caution.

Task 7: Cast Budget to Double
Purpose: Support decimal precision for financial calculations.
python
df_dept = df_dept.withColumn("budget", col("budget").cast("double"))
Use code with caution.

Task 8: Replace Negative Budgets with NULL
Purpose: Handle invalid or impossible financial values.
python
df_dept = df_dept.withColumn(
    "budget",
    when(col("budget") < 0, None).otherwise(col("budget"))
)
Use code with caution.

Task 9: Remove Records with Missing Department Name
Purpose: Enforce required business attributes.
python
df_dept = df_dept.filter(col("department_name").isNotNull())
Use code with caution.

Task 10: Add Audit & Lineage Columns
Purpose: Track processing metadata for data governance.
python
df_dept = df_dept \
    .withColumn("created_date", current_timestamp()) \
    .withColumn("source_system", lit("CSV_LOAD"))
Use code with caution.

Final Write: Save as Curated Delta Table
python
df_dept.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("curated_departments")
