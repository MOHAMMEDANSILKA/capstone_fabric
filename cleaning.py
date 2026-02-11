from pyspark.sql.functions import *

# =========================
# LOAD TABLES FROM DBO
# =========================
finance = spark.read.table("dbo.finance_transactions")
hr = spark.read.table("dbo.hr_employees")
proc = spark.read.table("dbo.procurement_orders")
dept = spark.read.table("dbo.departments")

# =========================
# COMMON CLEANING FUNCTION
# =========================
def clean_common(df, id_col):
    return (
        df.filter(col(id_col).isNotNull())     # Primary key check
          .dropDuplicates([id_col])            # Remove duplicates
    )

finance = clean_common(finance, "transaction_id")
hr = clean_common(hr, "employee_id")
proc = clean_common(proc, "procurement_id")
dept = clean_common(dept, "department_id")

# =========================
# DATA TYPE + NULL + INVALID FILTER
# =========================

finance = (
    finance
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("transaction_date", to_date("transaction_date"))
    .withColumn("expense_category", upper(trim(col("expense_category"))))
    .filter((col("amount") > 0) & (col("transaction_date") <= current_date()))
)

hr = (
    hr
    .withColumn("salary", col("salary").cast("double"))
    .withColumn("hire_date", to_date("hire_date"))
    .withColumn("job_title", upper(trim(col("job_title"))))
    .filter((col("salary") > 0) & (col("hire_date") <= current_date()))
)

proc = (
    proc
    .withColumn("order_amount", col("order_amount").cast("double"))
    .withColumn("order_date", to_date("order_date"))
    .withColumn("vendor_name", upper(trim(col("vendor_name"))))
    .withColumn("delivery_status", upper(trim(col("delivery_status"))))
    .filter((col("order_amount") > 0) & (col("order_date") <= current_date()))
)

dept = (
    dept
    .withColumn("budget", col("budget").cast("double"))
    .withColumn("department_name", upper(trim(col("department_name"))))
    .withColumn("location", upper(trim(col("location"))))
    .filter(col("budget") > 0)
)

# =========================
# FOREIGN KEY VALIDATION
# =========================
valid_depts = dept.select("department_id").distinct()

finance = finance.join(valid_depts, "department_id", "inner")
hr = hr.join(valid_depts, "department_id", "inner")
proc = proc.join(valid_depts, "department_id", "inner")

# =========================
# WRITE CURATED TABLES
# =========================
finance.write.mode("overwrite").saveAsTable("curated.finance_cleaned")
hr.write.mode("overwrite").saveAsTable("curated.hr_cleaned")
proc.write.mode("overwrite").saveAsTable("curated.procurement_cleaned")
dept.write.mode("overwrite").saveAsTable("curated.department_cleaned")

print("✅ Cleaning Completed")


#handling of inconsistent department names
from pyspark.sql import functions as F
inconsistent_map = {
    'Finance': ['FIN', 'Finance_Dept', 'FINANCE'],
    'HR': ['Human Resources', 'HR-Admin', 'hr'],
    'IT': ['Information Tech', 'IT_Support', 'Tech'],
    'Operations': ['OPS', 'Operations_Logistics'],
    'Sales': ['S&M', 'Sales_Dept']
}

dept_map = {}
for standard, variants in inconsistent_map.items():
    for v in variants:
        dept_map[v] = standard
