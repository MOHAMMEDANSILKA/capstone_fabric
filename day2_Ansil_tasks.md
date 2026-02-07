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


<img width="300" height="400" alt="image" src="https://github.com/user-attachments/assets/be279ed5-083b-4e38-9931-5a8a720b46c4" />

task 2 :


validated the existance of tables

<img width="300" height="400" alt="image" src="https://github.com/user-attachments/assets/899d0f90-f772-4577-a1d6-8182ad805008" />


task 3:


creating dataframe from table


the spark code for making df from tables

df_dept = spark.read.table("departments")

df_dept.show(5)

df_dept.printSchema()


<img width="300" height="400" alt="image" src="https://github.com/user-attachments/assets/3ea9c419-82b6-4644-aa41-c0f2b7538271" />



# Data Processing Pipeline: Departments Dataset

**Target Table:** `curated_departments` (Delta)  
**Purpose:** Clean, standardize, and enrich raw department data for analytics.

---

### Task 2 – Remove duplicate department records
**Purpose:** Ensure uniqueness by business key.
```python
df_dept = df_dept.dropDuplicates(["department_id"])

```
<img width="300" height="400" alt="image" src="https://github.com/user-attachments/assets/91656f89-c1c1-4376-a436-3c1eb9f0277f" />



### Task 3 – remove records with null primary key
**Purpose:** Ensure no null primary key .
```python
df_dept = df_dept.filter(col("department_id").isNotNull())


```
<img width="300" height="400" alt="image" src="https://github.com/user-attachments/assets/0684effc-f0fa-4dca-88d5-a47dfc738ee5" />


### Task 4 – Trim all string columns
**Purpose:** leading and lagging spaces will be removed .
```python
df_dept = (
    df_dept
    .withColumn("department_name", trim(col("department_name")))
    .withColumn("location", trim(col("location")))
)


```

<img width="300" height="400" alt="image" src="https://github.com/user-attachments/assets/da60676a-7285-43bd-b0c3-5b161da0ca28" />


### Task 5 – Standardize department name
**Purpose:** making the dep_anme to upper case.
```python
df_dept = df_dept.withColumn(
    "department_name", upper(col("department_name"))
)


```

<img width="300" height="400" alt="image" src="https://github.com/user-attachments/assets/38f194ed-72f2-4b76-b4eb-4d35934d73a9" />




### Task 6 – Standardize location values
**Purpose:** Initial Capitalization.
```python
df_dept = df_dept.withColumn(
    "location", initcap(col("location"))
)



```

<img width="300" height="400" alt="image" src="https://github.com/user-attachments/assets/3e0c255f-2950-49dc-8bb9-ef006a928404" />


### Task 7 – Cast manager_id to integer
**Purpose:** manager id converted to int.
```python
df_dept = df_dept.withColumn(
    "manager_id", col("manager_id").cast("int")
)



```

<img width="300" height="400" alt="image" src="https://github.com/user-attachments/assets/bc5a967d-a1ed-4d15-bdba-028cc45d7751" />


### Task 8 – Cast budget to double
**Purpose:** casting budget to double
```python
df_dept = df_dept.withColumn(
    "budget", col("budget").cast("double")
)




```



### Task 9 – Replace negative budgets with NULL
**Purpose:** replace negative bugget value to NULL
```python
df_dept = df_dept.withColumn(
    "budget",
    when(col("budget") < 0, None).otherwise(col("budget"))
)




```


### Task 10 – Remove rows with null department name
**Purpose:** Remove rows with null department name
```python
df_dept = df_dept.filter(col("department_name").isNotNull())


```


### Task 11 – Write Curated Table
**Purpose:** creating delta tables named curated_departments
```python
df_dept.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("curated_departments")

```

<img width="300" height="400" alt="image" src="https://github.com/user-attachments/assets/ed339cb7-25d7-421f-ad0d-a101c639aaeb" />



# Rest of the 3 tables also like this



# 2. FINANCE TRANSACTIONS – FINANCIAL VALIDATION

```python
df_fin = spark.read.table("finance_transactions")
df_fin = (
    df_fin
    .dropDuplicates(["transaction_id"])
    .filter(col("transaction_id").isNotNull())
    .withColumn("amount", col("amount").cast("double"))
    .filter((col("amount") > 0) & (col("amount").isNotNull()))
    .withColumn("department", upper(trim(col("department"))))
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("transaction_date", to_date(col("transaction_date")))
    .filter(col("transaction_date") <= current_date())
    .withColumn("risk_flag", when(col("amount") > 100000, "HIGH").otherwise("NORMAL"))
    .withColumn("created_date", current_timestamp())
)
df_fin.write.mode("overwrite").format("delta").saveAsTable("curated_finance_transactions")
```
# 3. HR EMPLOYEES – WORKFORCE DATA STANDARDIZATION
```python 

df_hr = spark.read.table("hr_employees")
df_hr = (
    df_hr
    .dropDuplicates(["employee_id"])
    .filter(col("employee_id").isNotNull())
    .withColumn("employee_name", initcap(trim(col("employee_name"))))
    .withColumn("department", upper(trim(col("department"))))
    .withColumn("salary", col("salary").cast("double"))
    .filter(col("salary") > 0)
    .withColumn("joining_date", to_date(col("joining_date")))
    .filter(col("joining_date") <= current_date())
    .withColumn("employment_status", upper(col("employment_status")))
    .withColumn("created_date", current_timestamp())
)
df_hr.write.mode("overwrite").format("delta").saveAsTable("curated_hr_employees")
```
# 4. PROCUREMENT ORDERS – VENDOR AND ORDER VALIDATION
``` python 

df_proc = spark.read.table("procurement_orders")
df_proc = (
    df_proc
    .dropDuplicates(["order_id"])
    .filter(col("order_id").isNotNull())
    .withColumn("vendor_name", upper(trim(col("vendor_name"))))
    .withColumn("order_amount", col("order_amount").cast("double"))
    .filter(col("order_amount") > 0)
    .withColumn("department", upper(trim(col("department"))))
    .withColumn("order_date", to_date(col("order_date")))
    .filter(col("order_date") <= current_date())
    .withColumn("order_status", upper(trim(col("order_status"))))
    .withColumn("high_value_flag", when(col("order_amount") > 50000, "Y").otherwise("N"))
    .withColumn("created_date", current_timestamp())
)
df_proc.write.mode("overwrite").format("delta").saveAsTable("curated_procurement_orders")
```


