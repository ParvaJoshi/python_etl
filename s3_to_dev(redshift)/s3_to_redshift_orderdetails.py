import redshift_connector
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_env_variables():
    """Retrieve environment variables for AWS and Redshift credentials."""
    aws_credentials = {
        'region': os.getenv('AWS_REGION'),
        'bucket_name': os.getenv('S3_BUCKET_NAME')
    }
    
    redshift_credentials = {
        'user': os.getenv('REDSHIFT_USER'),
        'password': os.getenv('REDSHIFT_PASSWORD'),
        'host': os.getenv('REDSHIFT_HOST'),
        'port': os.getenv('REDSHIFT_PORT'),
        'iam_arn': os.getenv('REDSHIFT_IAM_ARN')
    }
    
    return aws_credentials, redshift_credentials


def connect_to_redshift(redshift_credentials, region):
    """Connect to Redshift using IAM role authentication via redshift_connector."""
    try:
        connection = redshift_connector.connect(
            host=redshift_credentials['host'],
            database='h24parva',
            port=int(redshift_credentials['port']),
            user=redshift_credentials['user'],
            password=redshift_credentials['password']
        )
        print("Redshift connection successfull")
        return connection
    except redshift_connector.Error as e:
        print(f"Error connecting to Redshift: {e}")
        return None


def get_batch_details(connection):
    """Fetches the latest batch details from etl_metadata.batch_control in Redshift."""
    query = "SELECT etl_batch_no, etl_batch_date FROM etl_metadata.batch_control"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            if row:
                batch_no, batch_date = row
                print(f"Batch date successfully set to {batch_date}")
                return batch_no, batch_date
            else:
                print("No batch details found in etl_metadata.batch_control.")
                return None, None
    except Exception as e:
        print(f"Error executing query: {e}")
        return None, None

def copy_data_from_s3_to_redshift(cursor, schema_name, table_name, s3_path, iam_arn, region):
    """Copy data from S3 to Redshift table in devstage schema."""
    copy_query = f"""
    COPY h24parva.{schema_name}.{table_name}
    FROM '{s3_path}'
    IAM_ROLE '{iam_arn}'
    FORMAT AS CSV
    DELIMITER ','
    QUOTE '"'
    IGNOREHEADER 1
    REGION AS '{region}'
    """
    cursor.execute(copy_query)

def main():
    # Load environment variables
    aws_credentials, redshift_credentials = get_env_variables()
    schema_name = 'devstage'
    table_name = 'orderdetails'
    # Connect to Redshift and get batch details
    redshift_conn = connect_to_redshift(redshift_credentials, aws_credentials['region'])
    redshift_cursor = redshift_conn.cursor()
    _, batch_date = get_batch_details(redshift_conn)
    if not batch_date:
        print("No batch date found; exiting script.")
        return

    # S3 path configuration based on batch date
    s3_file_path = f"s3://{aws_credentials['bucket_name']}/{table_name}/{batch_date}/{table_name.upper()}.csv"

    # Copy data from S3 to Redshift
    try:
        copy_data_from_s3_to_redshift(redshift_cursor, schema_name, table_name, s3_file_path, redshift_credentials['iam_arn'], aws_credentials['region'])
        redshift_conn.commit()
        print(f"Data successfully loaded into Redshift table devstage.{table_name}.")
    except Exception as e:
        redshift_conn.rollback()
        print(f"Error loading data into Redshift: {e}")
    finally:
        redshift_cursor.close()
        redshift_conn.close()

if __name__ == "__main__":
    main()