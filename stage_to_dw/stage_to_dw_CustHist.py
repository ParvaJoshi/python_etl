import redshift_connector
import os
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

def get_env_variables():
    """Retrieve environment variables for Redshift credentials."""    
    redshift_credentials = {
        'user': os.getenv('REDSHIFT_USER'),
        'password': os.getenv('REDSHIFT_PASSWORD'),
        'host': os.getenv('REDSHIFT_HOST'),
        'port': os.getenv('REDSHIFT_PORT'),
    }
    
    return redshift_credentials


def connect_to_redshift(redshift_credentials):
    """Connect to Redshift using redshift_connector."""
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

def stg_to_dw(cursor, batch_no, batch_date):
    """Transfers data from devstage to devdw"""
    update_query = f"""
        UPDATE devdw.customer_history ch
        SET 
            dw_active_record_ind = 0,
            effective_to_date = TO_DATE('{batch_date}', 'YYYY-MM-DD')::date - INTERVAL '1 day',  -- for etl_batch_date, adjusted for PostgreSQL
            update_etl_batch_no = {batch_no},  -- for etl_batch_no
            update_etl_batch_date = TO_DATE('{batch_date}', 'YYYY-MM-DD'),  -- for etl_batch_date
            dw_update_timestamp = CURRENT_TIMESTAMP
        FROM devdw.Customers C
        WHERE ch.dw_customer_id = C.dw_customer_id
        AND ch.dw_active_record_ind = 1
        AND C.creditLimit <> ch.creditLimit;
        
        """
    insert_query =  f"""
        INSERT INTO devdw.customer_history 
        (
            dw_customer_id,
            creditLimit,
            effective_from_date,
            dw_active_record_ind,
            create_etl_batch_no,
            create_etl_batch_date
        )
        SELECT 
            C.dw_customer_id,
            C.creditLimit,
            TO_DATE('{batch_date}', 'YYYY-MM-DD'),  -- for etl_batch_date
            1,  -- for dw_active_record_ind
            {batch_no},  -- for etl_batch_no
            TO_DATE('{batch_date}', 'YYYY-MM-DD')   -- for etl_batch_date
        FROM devdw.Customers C
        LEFT JOIN devdw.customer_history ch
            ON C.dw_customer_id = ch.dw_customer_id 
            AND ch.dw_active_record_ind = 1
        WHERE ch.dw_customer_id IS NULL;
        """
    cursor.execute(update_query)
    cursor.execute(insert_query)

def main():
    # Load environment variables
    redshift_credentials = get_env_variables()
    # Connect to Redshift and get batch details
    redshift_conn = connect_to_redshift(redshift_credentials)
    redshift_cursor = redshift_conn.cursor()
    batch_no, batch_date = get_batch_details(redshift_conn)
    if not batch_date or not batch_no:
        print("No batch details found; exiting script.")
        return
    # Copy data from stage to dw
    try:
        stg_to_dw(redshift_cursor, batch_no, batch_date)        
        redshift_conn.commit()
        print(f"Data successfully loaded into Redshift table devdw.CustomersHistory.")
    except Exception as e:
        redshift_conn.rollback()
        print(f"Error loading data into devdw: {e}")
    finally:
        redshift_cursor.close()
        redshift_conn.close()

if __name__ == "__main__":
    main()