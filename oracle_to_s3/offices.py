import csv
import os
from datetime import datetime
from dotenv import load_dotenv
import oracledb
import boto3
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

# Initialize S3 client and parameters
s3_client = boto3.client('s3')
bucket_name = os.getenv('S3_BUCKET_NAME')
region = os.getenv('AWS_REGION')

# Ensure the bucket exists
def create_bucket_if_not_exists(bucket_name, region=None):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Bucket '{bucket_name}' does not exist. Creating it...")
            if region:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            else:
                s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Error checking bucket: {e}")
            raise

# Connect to Oracle database and retrieve batch information
un = os.getenv('ORACLE_USERNAME')
userpwd = os.getenv('ORACLE_PASSWORD')
connect_string = os.getenv('ORACLE_DSN')
oracledb.init_oracle_client(lib_dir=os.getenv('d'))

def get_batch_control_info():
    with oracledb.connect(user=un, password=userpwd, dsn=connect_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT etl_batch_no, etl_batch_date FROM h24parva.batch_control")
            return cursor.fetchall()

def export_table(table_name, columns, batch_no, batch_date):
    # Format date path for S3
    date_path = batch_date.strftime('%Y-%m-%d')
    s3_path = f"{table_name.lower()}/{date_path}/{table_name}.csv"
    
    # Use UPDATE_TIMESTAMP for incremental loading if available
    # if 'UPDATE_TIMESTAMP' in columns:
    sql_query = f"""
        SELECT {', '.join(columns)}
        FROM {table_name}@parva_dblink
        WHERE UPDATE_TIMESTAMP > TO_TIMESTAMP('{batch_date.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
    """
    # else:
    #     # Perform a full load if no timestamp column for incremental filtering
    #     sql_query = f"""
    #         SELECT {', '.join(columns)}
    #         FROM {table_name}@parva_dblink
    #     """
    
    file_path = f"{table_name}.csv"
    
    try:
        # Connect to Oracle database
        connection = oracledb.connect(user=un, password=userpwd, dsn=connect_string)
        with connection.cursor() as cursor:
            # Execute query
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            
            # Write data to CSV if there are rows
            if rows:
                with open(file_path, "w", encoding='utf-8') as outputfile:
                    writer = csv.writer(outputfile, lineterminator="\n")
                    writer.writerow(columns)
                    writer.writerows(rows)
                print(f"{table_name}.csv created with {len(rows)} rows.")

                # Upload CSV to S3
                s3_client.upload_file(file_path, bucket_name, s3_path)
                print(f"Uploaded {table_name}.csv to S3 at {s3_path}")
            else:
                print(f"No data to export for table '{table_name}' on batch date '{batch_date}'. Skipping file creation.")
        
    finally:
        # Clean up local file
        if os.path.exists(file_path):
            os.remove(file_path)
        if 'connection' in locals():
            connection.close()



# Example usage for tables
batch_info = get_batch_control_info()

# Define table columns
tables = {
    "OFFICES": ['OFFICECODE', 'CITY', 'PHONE', 'ADDRESSLINE1', 'ADDRESSLINE2', 'STATE', 'COUNTRY', 'POSTALCODE', 'TERRITORY','CREATE_TIMESTAMP','UPDATE_TIMESTAMP'],
    # Add more tables here as needed
}

for batch_no, batch_date in batch_info:
    for table_name, columns in tables.items():
        print(f"Processing table '{table_name}' for batch number {batch_no} on date {batch_date}")
        export_table(table_name, columns, batch_no, batch_date)
