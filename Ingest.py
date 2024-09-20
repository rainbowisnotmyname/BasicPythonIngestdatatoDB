import pandas as pd
import pyodbc

print(pd.__version__)
print(pyodbc.drivers())

# Step 1: Extract Data from CSV
def extract_data(file_path):
    data = pd.read_csv(file_path)
    return data

# Step 2: Transform Data (Optional: no transformations in this case)
def transform_data(data):
    return data  # No changes in this example

# Step 3: Load Data into SQL Server
def load_data_to_sql(transformed_data, server, database, table):
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for index, row in transformed_data.iterrows():
        insert_query = f"""
        INSERT INTO {table} (EmpID, FirstName, LastName, Position, Salary, JoinDate)
        VALUES (?, ?, ?, ?, ?, ?)"""
        cursor.execute(insert_query, row['EmpID'], row['FirstName'], row['LastName'], row['Position'], row['Salary'], row['JoinDate'])
    
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # Parameters
    file_path = "employees.csv"
    server = "DESKTOP-70V4JCI\SQLEXPRESS"
    database = "AllUser"
    table = "dbo.Employees"

    # Run ETL
    data = extract_data(file_path)
    transformed_data = transform_data(data)
    load_data_to_sql(transformed_data, server, database, table)

    print("ETL process completed.")
