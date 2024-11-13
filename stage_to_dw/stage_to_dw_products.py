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
       UPDATE devdw.Products B
        SET 
            productName = A.productName,
            productLine = A.productLine,
            productScale = A.productScale,
            productVendor = A.productVendor,
            quantityInStock = A.quantityInStock,
            buyPrice = A.buyPrice,
            MSRP = A.MSRP,
            dw_product_line_id = PL.dw_product_line_id,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = {batch_no},
            etl_batch_date = TO_DATE('{batch_date}', 'YYYY-MM-DD')
        FROM devstage.Products A
        JOIN devdw.ProductLines PL ON A.productLine = PL.productLine
        WHERE A.productCode = B.src_productCode;
        """
    insert_query =  f"""
        INSERT INTO devdw.Products
        (
            src_productCode,
            productName,
            productLine,
            productScale,
            productVendor,
            quantityInStock,
            buyPrice,
            MSRP,
            dw_product_line_id,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            A.productCode,
            A.productName,
            A.productLine,
            A.productScale,
            A.productVendor,
            A.quantityInStock,
            A.buyPrice,
            A.MSRP,
            PL.dw_product_line_id,
            A.create_timestamp,
            A.update_timestamp,
            {batch_no},
            TO_DATE('{batch_date}', 'YYYY-MM-DD')
        FROM devstage.Products A
        LEFT JOIN devdw.Products B ON A.productCode = B.src_productCode
        JOIN devdw.ProductLines PL ON A.productLine = PL.productLine
        WHERE B.src_productCode IS NULL;
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
        print(f"Data successfully loaded into Redshift table devdw.products.")
    except Exception as e:
        redshift_conn.rollback()
        print(f"Error loading data into devdw: {e}")
    finally:
        redshift_cursor.close()
        redshift_conn.close()

if __name__ == "__main__":
    main()