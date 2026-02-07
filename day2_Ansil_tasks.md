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


Data Processing Pipeline – Departments Dataset
Target Table: curated_departments (Delta)
Purpose: Clean, standardize and enrich raw department data for analytics
Task 1 – Remove duplicate department records
Purpose: Ensure uniqueness by business key
Code: df_dept = df_dept.dropDuplicates(["department_id"])
Task 2 – Eliminate records with missing department_id
Purpose: Protect primary key integrity
Code: df_dept = df_dept.filter(col("department_id").isNotNull())
Task 3 – Trim all string columns
Purpose: Remove leading/trailing whitespace
Code:
Pythondf_dept = df_dept \
    .withColumn("department_name", trim(col("department_name"))) \
    .withColumn("location", trim(col("location")))
Task 4 – Standardize department name (uppercase)
Purpose: Consistent casing for reporting & joins
Code: df_dept = df_dept.withColumn("department_name", upper(col("department_name")))
Task 5 – Standardize location formatting
Purpose: Apply title case for readability
Code: df_dept = df_dept.withColumn("location", initcap(col("location")))
Task 6 – Cast manager_id to integer
Purpose: Enforce correct data type
Code: df_dept = df_dept.withColumn("manager_id", col("manager_id").cast("int"))
Task 7 – Cast budget to double
Purpose: Support decimal precision
Code: df_dept = df_dept.withColumn("budget", col("budget").cast("double"))
Task 8 – Replace negative budgets with NULL
Purpose: Handle invalid financial values
Code:
Pythondf_dept = df_dept.withColumn(
    "budget",
    when(col("budget") < 0, None).otherwise(col("budget"))
)
Task 9 – Remove records with missing department name
Purpose: Enforce required business attribute
Code: df_dept = df_dept.filter(col("department_name").isNotNull())
Task 10 – Add audit / lineage columns
Purpose: Track processing metadata
Code:
Pythondf_dept = df_dept \
    .withColumn("created_date", current_timestamp()) \
    .withColumn("source_system", lit("CSV_LOAD"))
Final Write – Save as curated Delta table
Pythondf_dept.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("curated_departments")


