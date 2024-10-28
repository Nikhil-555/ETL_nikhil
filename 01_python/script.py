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

# Extract Data from Oracle
def extract_data(conn, schema_name, table_name, columns):
    cursor = conn.cursor()
    query = f"SELECT {', '.join(columns)} FROM {schema_name}.{table_name}"
    cursor.execute(query)
    return cursor.fetchall()

# Transform Data to CSV
def transform_data(data):
    csv_data = [",".join(str(value) for value in row) for row in data]
    return "\n".join(csv_data)

# Load CSV Data to S3
def load_to_s3(csv_data, schema_name, table_name):
    s3 = boto3.client('s3')
    bucket_name = os.environ.get('S3_BUCKET')
    folder_name = f"{schema_name}"
    file_name = f"{table_name}.csv"
    key = f"{folder_name}/{table_name}/{file_name}"

    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        print(f"File {file_name} already exists. Adding timestamp to the new file.")
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_name = f"{table_name}_{timestamp}.csv"
        key = f"{folder_name}/{table_name}/{file_name}"
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"No existing file found with the name {file_name}. Proceeding without timestamp.")
        else:
            raise

    s3.put_object(Body=csv_data, Bucket=bucket_name, Key=key)
    print(f"File {file_name} uploaded successfully.")

# Main Function
def main(schema_name, table_name, columns):
    with connect_to_oracle() as conn:
        data = extract_data(conn, schema_name, table_name, columns)
        csv_data = transform_data(data)
        load_to_s3(csv_data, schema_name, table_name)
