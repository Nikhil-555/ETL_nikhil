import os
import redshift_connector
from dotenv import load_dotenv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from function.fetch_date import *

load_dotenv()


# Connect to Redshift
conn = redshift_connector.connect(
    host=os.environ.get('host'),
    database=os.environ.get('db_name'),      
    user=os.environ.get('user'),                
    password=os.environ.get('passs'),         
    port=5439
)

cursor = conn.cursor()

IAM_ROLE = os.environ.get('IAM_role')
etl_batch_date = fetch_date_value(conn1)
print("Date Value:", etl_batch_date)

#query to copy data 
query = f"""
COPY nikhil_db.devstage.employees (EMPLOYEENUMBER, LASTNAME, FIRSTNAME, EXTENSION, EMAIL, OFFICECODE, REPORTSTO, JOBTITLE, CREATE_TIMESTAMP, UPDATE_TIMESTAMP)
FROM 's3://etl-python-bucket/employees/{etl_batch_date}/employees.csv' 
IAM_ROLE '{IAM_ROLE}'
FORMAT AS CSV DELIMITER ',' DATEFORMAT 'auto' QUOTE '"' IGNOREHEADER 1 REGION AS 'eu-north-1'
"""

# Execute the COPY command
try:
    cursor.execute('truncate nikhil_db.devstage.employees;')
    cursor.execute(query)
    conn.commit()
    print("Data loaded successfully from S3 to Redshift.")
except Exception as e:
    print(f"Error loading data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()

