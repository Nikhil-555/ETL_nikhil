import os
import oracledb
import boto3
import botocore
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize Oracle Client
oracledb.init_oracle_client(lib_dir=os.getenv('ORACLE_INSTANT_CLIENT_PATH'))  # Ensure this variable is in your .env file

# Oracle Connection Function
def connect_to_oracle():
    dsn = os.environ.get('ORACLE_DSN')
    user = os.environ.get('ORACLE_USER')
    password = os.environ.get('ORACLE_PASSWORD')
    return oracledb.connect(user=user, password=password, dsn=dsn)

# Set Current Schema to h24nikhil
def set_current_schema(conn):
    cursor = conn.cursor()
    print("Setting current schema to h24nikhil")
    cursor.execute("ALTER SESSION SET CURRENT_SCHEMA = h24nikhil")

# Create a database link if it doesn't exist
def create_database_link_if_not_exists(conn):
    cursor = conn.cursor()

    # Fetch the database link name and password from environment variables
    db_link_name = os.getenv('database_link_name')
    db_link_password = os.getenv('database_link_password')
    
    # Ensure the environment variables are loaded correctly
    if not db_link_name or not db_link_password:
        raise ValueError("Database link name or password not found in environment variables.")
    
    # Try to create the database link
    try:
        print("Creating database link 'nikhil_dblink_classicmodels'")
        create_query = f"""
        CREATE PUBLIC DATABASE LINK nikhil_dblink_classicmodels
        CONNECT TO {db_link_name} IDENTIFIED BY {db_link_password}
        USING 'XE'
        """
        cursor.execute(create_query)
        print("Database link created successfully.")
    except oracledb.DatabaseError as e:
        error_message = str(e)
        if "ORA-02011: duplicate database link name" in error_message:
            print("Database link 'nikhil_dblink_classicmodels' already exists, skipping creation.")
        else:
            raise  # Re-raise if it's a different error


# Fetch Date Value from Oracle Table in h24nikhil Schema
def fetch_date_value(conn):
    cursor = conn.cursor()
    # Fetch the row number from environment variables
    row_num = os.getenv('row_num')

    # Ensure the row_num is loaded correctly
    if not row_num:
        raise ValueError("Row number not found in environment variables.")

    # Use a subquery to assign row numbers and select the second row
    query = f"""
    SELECT folder_date
    FROM (
        SELECT folder_date, ROWNUM AS rn
        FROM (
            SELECT folder_date
            FROM batch_control
            ORDER BY folder_date 
        )
        WHERE ROWNUM <= {row_num} -- Select first n rows after ordering
    )
    WHERE rn = {row_num}  -- Select the specified row
    """
    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        return result[0][0]  # Assuming the date value is in the first column
    else:
        raise ValueError(f"No date value found in row {row_num} of the batch_control table")


# Extract Data from Oracle
def extract_data(conn, table_name, columns):
    cursor = conn.cursor()
    query = f"SELECT {', '.join(columns)} FROM {table_name}@nikhil_dblink_classicmodels"
    cursor.execute(query)
    return cursor.fetchall()

# Transform Data to CSV
def transform_data(data):
    csv_data = [",".join(str(value) for value in row) for row in data]
    return "\n".join(csv_data)

# Load CSV Data to S3
def load_to_s3(csv_data, table_name, date_value):
    s3 = boto3.client('s3')
    bucket_name = os.environ.get('S3_BUCKET')
    folder_name = f"{table_name}/{date_value}"
    file_name = f"{table_name}.csv"
    key = f"{folder_name}/{file_name}"

    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        print(f"File {file_name} already exists. Adding timestamp to the new file.")
        timestamp = datetime.now().strftime('%Y-%m-%d')
        file_name = f"{table_name}_{timestamp}.csv"
        key = f"{folder_name}/{file_name}"
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"No existing file found with the name {file_name}. Proceeding without timestamp.")
        else:
            raise

    s3.put_object(Body=csv_data, Bucket=bucket_name, Key=key)
    print(f"File {file_name} uploaded successfully.")

# Main Function
def main(table_name, columns):
    with connect_to_oracle() as conn:
        # Set current schema to h24nikhil
        set_current_schema(conn)

        # Manage the database link (drop if exists and create new one)
        create_database_link_if_not_exists(conn)

        # Fetch the date value from the h24nikhil schema
        date_value = fetch_date_value(conn)
        
        # Extract and process data
        data = extract_data(conn, table_name, columns)
        csv_data = transform_data(data)
        
        # Upload data to S3 with the fetched date value
        load_to_s3(csv_data, table_name, date_value)

table_name = "customers"
columns = ["customerNumber", "customerName", "contactLastName", "contactFirstName", "phone", "addressLine1", "city", "country"]
main(table_name, columns)
