import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Fetch Redshift credentials from environment variables
un = os.getenv('REDSHIFT_USER')
userpwd = os.getenv('REDSHIFT_PASSWORD')
host = os.getenv('REDSHIFT_HOST')
port = os.getenv('REDSHIFT_PORT')
database = os.getenv('REDSHIFT_DB')
batch_control_no = os.getenv('BATCH_CONTROL_NO')  # Ensure the variable name is in uppercase
batch_control_date = os.getenv('BATCH_CONTROL_DATE')  # Ensure the variable name is in uppercase

# Update function
def update_batch_control(batch_no, batch_date):
    query = """
        UPDATE etl_metadata.batch_control 
        SET etl_batch_no = %s, 
            etl_batch_date = TO_DATE(%s, 'YYYY-MM-DD')
    """
    try:
        # Establish connection to Redshift
        connection = psycopg2.connect(
            dbname=database, user=un, password=userpwd, host=host, port=port
        )
        with connection.cursor() as cursor:
            cursor.execute(query, (batch_no, batch_date))
            connection.commit()
            print("Batch control table updated successfully in Redshift.")
    except psycopg2.DatabaseError as e:
        print("Error updating batch control in Redshift:", e)
    finally:
        if 'connection' in locals():
            connection.close()

# Usage
update_batch_control(batch_control_no, batch_control_date)
