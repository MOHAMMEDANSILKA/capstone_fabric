import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set seed for reproducibility
np.random.seed(42)

# 1. DEPARTMENTS CSV (small file - departments don't change much)
print("Creating Departments CSV...")
departments = pd.DataFrame({
    'department_id': range(1, 21),
    'department_name': [f'Department_{i}' for i in range(1, 21)],
    'manager_id': np.random.choice(range(100, 500), 20, replace=False),
    'budget': np.round(np.random.uniform(100000, 5000000, 20), 2),
    'location': np.random.choice(['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix'], 20)
})

# 2. HR EMPLOYEES CSV (medium file - matching 2M transactions)
print("Creating HR Employees CSV...")
employee_count = 5000  # 5000 employees to match with 2M transactions
employees = pd.DataFrame({
    'employee_id': range(100, 100 + employee_count),
    'first_name': [f'FirstName_{i}' for i in range(employee_count)],
    'last_name': [f'LastName_{i}' for i in range(employee_count)],
    'department_id': np.random.choice(departments['department_id'], employee_count),
    'salary': np.round(np.random.uniform(40000, 150000, employee_count), 2),
    'hire_date': [datetime(2015, 1, 1) + timedelta(days=np.random.randint(0, 3000)) 
                  for _ in range(employee_count)],
    'email': [f'employee{i}@company.com' for i in range(100, 100 + employee_count)]
})

# 3. FINANCE TRANSACTIONS CSV (2 million rows)
print("Creating Finance Transactions CSV (2M rows)...")
n_transactions = 2000000

# Generate dates spanning 3 years
start_date = datetime(2020, 1, 1)
date_range = [start_date + timedelta(days=i) for i in range(3*365)]

transactions = pd.DataFrame({
    'transaction_id': range(1, n_transactions + 1),
    'date': np.random.choice(date_range, n_transactions),
    'employee_id': np.random.choice(employees['employee_id'], n_transactions),
    'department_id': np.random.choice(departments['department_id'], n_transactions),
    'amount': np.round(np.random.exponential(500, n_transactions), 2),
    'transaction_type': np.random.choice(['Expense', 'Revenue', 'Transfer', 'Adjustment'], 
                                        n_transactions, p=[0.6, 0.3, 0.05, 0.05]),
    'category': np.random.choice(['Travel', 'Office Supplies', 'Software', 'Hardware', 
                                  'Consulting', 'Marketing', 'Utilities', 'Rent'], n_transactions),
    'vendor_id': np.random.choice(range(1000, 5000), n_transactions),
    'payment_method': np.random.choice(['Credit Card', 'Bank Transfer', 'Check', 'Cash'], 
                                       n_transactions),
    'status': np.random.choice(['Completed', 'Pending', 'Rejected', 'Approved'], 
                              n_transactions, p=[0.7, 0.1, 0.1, 0.1]),
    'description': [f'Transaction for {cat}' for cat in np.random.choice(
        ['Project A', 'Project B', 'Project C', 'General Operations'], n_transactions)]
})

# 4. PROCUREMENT ORDERS CSV (matching finance transactions)
print("Creating Procurement Orders CSV...")
n_orders = 500000  # Half a million procurement orders

procurement = pd.DataFrame({
    'order_id': range(1, n_orders + 1),
    'transaction_id': np.random.choice(transactions['transaction_id'], n_orders, replace=False),
    'supplier_id': np.random.choice(range(100, 1000), n_orders),
    'order_date': np.random.choice(date_range, n_orders),
    'delivery_date': [(d + timedelta(days=np.random.randint(1, 30))) for d in np.random.choice(date_range, n_orders)],
    'item_category': np.random.choice(['Electronics', 'Furniture', 'Raw Materials', 
                                       'Office Supplies', 'Services'], n_orders),
    'quantity': np.random.randint(1, 100, n_orders),
    'unit_price': np.round(np.random.uniform(10, 1000, n_orders), 2),
    'total_amount': None,  # Will calculate below
    'order_status': np.random.choice(['Delivered', 'Pending', 'Cancelled', 'Shipped'], 
                                     n_orders, p=[0.6, 0.2, 0.1, 0.1])
})

# Calculate total amount
procurement['total_amount'] = procurement['quantity'] * procurement['unit_price']

# Save to CSV files
print("Saving CSV files...")

# Save departments
departments.to_csv('departments.csv', index=False)

# Save employees
employees.to_csv('hr_employees.csv', index=False)

# Save finance transactions in chunks (to manage memory)
chunk_size = 500000
for i in range(0, n_transactions, chunk_size):
    chunk = transactions.iloc[i:i+chunk_size]
    if i == 0:
        chunk.to_csv('finance_transactions.csv', index=False)
    else:
        chunk.to_csv('finance_transactions.csv', mode='a', header=False, index=False)

# Save procurement
procurement.to_csv('procurement_orders.csv', index=False)

print("Files created successfully!")
print(f"1. departments.csv: {len(departments)} rows")
print(f"2. hr_employees.csv: {len(employees)} rows")
print(f"3. finance_transactions.csv: {len(transactions):,} rows")
print(f"4. procurement_orders.csv: {len(procurement):,} rows")
