import os
import sys
import redshift_connector


# Connect to Redshift
conn = redshift_connector.connect(
    host='default-workgroup.783764614140.eu-north-1.redshift-serverless.amazonaws.com',
    database='nikhil_db',      
    user='admin',                
    password='Killua111',         
    port=5439
)

cursor = conn.cursor()

def fetch_date_value(conn1):
    cursor = conn1.cursor()
    query = """
    SELECT etl_batch_no, etl_batch_date from metadata.batch_control
    """
    cursor.execute(query)
    result = cursor.fetchone()

    if result:
        return result[1]  # Returns the batch_control_date as YYYYMMDD
    else:
        raise ValueError("No date value found in batch_control table")

IAM_ROLE = 'arn:aws:iam::783764614140:role/service-role/AmazonRedshift-CommandsAccessRole-20241104T131412'
etl_batch_date = fetch_date_value(conn)
print("Date Value:", etl_batch_date)

#query to copy data 
query = f"""
COPY nikhil_db.devstage.offices (OFFICECODE, CITY, PHONE, ADDRESSLINE1, ADDRESSLINE2, STATE, COUNTRY, POSTALCODE, TERRITORY, CREATE_TIMESTAMP, UPDATE_TIMESTAMP)
FROM 's3://etl-python-bucket/offices/{etl_batch_date}/offices.csv' 
IAM_ROLE '{IAM_ROLE}'
FORMAT AS CSV DELIMITER ',' DATEFORMAT 'auto' QUOTE '"' IGNOREHEADER 1 REGION AS 'eu-north-1'
"""

# Execute the COPY command
try:
    cursor.execute('truncate nikhil_db.devstage.offices;')
    cursor.execute(query)
    conn.commit()
    print("Data loaded successfully from S3 to Redshift.")
except Exception as e:
    print(f"Error loading data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()

