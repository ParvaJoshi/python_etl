import csv
import os
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import oracledb
import boto3
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

# Initialize S3 client and parameters (IAM Role credentials automatically used)
s3_client = boto3.client('s3', region_name=os.getenv('AWS_REGION'))  # No need to pass AWS credentials here
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


un = os.getenv('ORACLE_USERNAME')
userpwd = os.getenv('ORACLE_PASSWORD')
connect_string = os.getenv('ORACLE_DSN')
oracledb.init_oracle_client(lib_dir=os.getenv('d'))

# Connect to Redshift and retrieve batch information
def get_batch_control_info():
    # Redshift connection parameters from .env
    redshift_host = os.getenv('REDSHIFT_HOST')
    redshift_port = os.getenv('REDSHIFT_PORT')
    redshift_db = os.getenv('REDSHIFT_DB')
    redshift_user = os.getenv('REDSHIFT_USER')
    redshift_password = os.getenv('REDSHIFT_PASSWORD')
    
    # Connect to Redshift
    conn = psycopg2.connect(    
        host=redshift_host,
        port=redshift_port,
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password
    )
    
    # Query to retrieve batch control info from Redshift
    query = """
        SELECT etl_batch_no, etl_batch_date 
        FROM etl_metadata.batch_control 
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        
    conn.close()
    return result

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
    'PRODUCTS': ['PRODUCTCODE', 'PRODUCTNAME', 'PRODUCTLINE', 'PRODUCTSCALE', 'PRODUCTVENDOR', 'QUANTITYINSTOCK', 'BUYPRICE', 'MSRP','CREATE_TIMESTAMP','UPDATE_TIMESTAMP'],
    # Add more tables here as needed
}

for batch_no, batch_date in batch_info:
    for table_name, columns in tables.items():
        print(f"Processing table '{table_name}' for batch number {batch_no} on date {batch_date}")
        export_table(table_name, columns, batch_no, batch_date)

