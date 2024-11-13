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
    insert_query =  f"""
        INSERT INTO devdw.daily_product_summary
        (
            summary_date,
            dw_product_id,
            customer_apd,
            product_cost_amount,
            product_mrp_amount,
            cancelled_product_qty,
            cancelled_cost_amount,
            cancelled_mrp_amount,
            cancelled_order_apd,
            dw_create_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        WITH CTE AS
        (
            -- Orders data (non-cancelled orders)
            SELECT CAST(o.orderDate AS DATE) AS summary_date,
                p.dw_product_id,
                1 AS customer_apd,
                SUM(od.quantityOrdered * od.priceEach) AS product_cost_amount,
                SUM(od.quantityOrdered * p.MSRP) AS product_mrp_amount,
                0 AS cancelled_product_qty,
                0 AS cancelled_cost_amount,
                0 AS cancelled_mrp_amount,
                0 AS cancelled_order_apd
            FROM devdw.Products p
            JOIN devdw.OrderDetails od ON p.dw_product_id = od.dw_product_id
            JOIN devdw.Orders o ON od.dw_order_id = o.dw_order_id
            WHERE o.orderDate >= TO_DATE('{batch_date}', 'YYYY-MM-DD')  -- etl_batch_date parameter
            GROUP BY CAST(o.orderDate AS DATE),
                    p.dw_product_id

            UNION ALL

            -- Cancelled orders data
            SELECT CAST(o.cancelledDate AS DATE) AS summary_date,
                p.dw_product_id,
                1 AS customer_apd,
                0 AS product_cost_amount,
                0 AS product_mrp_amount,
                COUNT(DISTINCT o.dw_order_id) AS cancelled_product_qty,
                SUM(od.quantityOrdered * od.priceEach) AS cancelled_cost_amount,
                SUM(od.quantityOrdered * p.MSRP) AS cancelled_mrp_amount,
                1 AS cancelled_order_apd
            FROM devdw.Products p
            JOIN devdw.OrderDetails od ON p.dw_product_id = od.dw_product_id
            JOIN devdw.Orders o ON od.dw_order_id = o.dw_order_id
            WHERE o.cancelledDate >= TO_DATE('{batch_date}', 'YYYY-MM-DD')  -- etl_batch_date parameter
            GROUP BY CAST(o.cancelledDate AS DATE),
                    p.dw_product_id
        )
        SELECT summary_date,
            dw_product_id,
            MAX(customer_apd) AS customer_apd,
            MAX(product_cost_amount) AS product_cost_amount,
            MAX(product_mrp_amount) AS product_mrp_amount,
            MAX(cancelled_product_qty) AS cancelled_product_qty,
            MAX(cancelled_cost_amount) AS cancelled_cost_amount,
            MAX(cancelled_mrp_amount) AS cancelled_mrp_amount,
            MAX(cancelled_order_apd) AS cancelled_order_apd,
            CURRENT_TIMESTAMP AS dw_create_timestamp,
            {batch_no} AS etl_batch_no,  -- etl_batch_no parameter
            TO_DATE('{batch_date}', 'YYYY-MM-DD') AS etl_batch_date  -- etl_batch_date parameter
        FROM CTE
        GROUP BY summary_date,
                dw_product_id;
        """
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
        print(f"Data successfully loaded into Redshift table devdw.daily_product_summary.")
    except Exception as e:
        redshift_conn.rollback()
        print(f"Error loading data into devdw: {e}")
    finally:
        redshift_cursor.close()
        redshift_conn.close()

if __name__ == "__main__":
    main()