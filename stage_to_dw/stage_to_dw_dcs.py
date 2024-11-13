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
        INSERT INTO devdw.daily_customer_summary
        (
            summary_date,
            dw_customer_id,
            order_count,
            order_apd,
            order_amount,
            order_cost_amount,
            order_mrp_amount,
            products_ordered_qty,
            products_items_qty,
            cancelled_order_count,
            cancelled_order_amount,
            cancelled_order_apd,
            shipped_order_count,
            shipped_order_amount,
            shipped_order_apd,
            payment_apd,
            payment_amount,
            new_customer_apd,
            new_customer_paid_apd,
            create_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        WITH CTE AS
        (
            -- Orders data
            SELECT CAST(o.orderDate AS DATE) AS summary_date,
                o.dw_customer_id,
                COUNT(DISTINCT o.dw_order_id) AS order_count,
                1 AS order_apd,
                SUM(od.priceEach * od.quantityOrdered) AS order_amount,
                SUM(p.buyPrice * od.quantityOrdered) AS order_cost_amount,
                SUM(p.MSRP * od.quantityOrdered) AS order_mrp_amount,
                COUNT(DISTINCT od.src_productCode) AS products_ordered_qty,
                SUM(od.quantityOrdered) AS products_items_qty,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Orders o
            JOIN devdw.OrderDetails od ON o.dw_order_id = od.dw_order_id
            JOIN devdw.Products p ON od.dw_product_id = p.dw_product_id
            WHERE o.orderDate >= TO_DATE('{batch_date}', 'YYYY-MM-DD')  -- etl_batch_date parameter
            GROUP BY CAST(o.orderDate AS DATE),
                    o.dw_customer_id

            UNION ALL

            -- Cancelled orders data
            SELECT CAST(o.cancelledDate AS DATE) AS summary_date,
                o.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_amount,
                0 AS order_cost_amount,
                0 AS order_mrp_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                COUNT(DISTINCT o.dw_order_id) AS cancelled_order_count,
                SUM(od.priceEach * od.quantityOrdered) AS cancelled_order_amount,
                1 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Orders o
            JOIN devdw.OrderDetails od ON o.dw_order_id = od.dw_order_id
            WHERE o.cancelledDate >= TO_DATE('{batch_date}', 'YYYY-MM-DD')  -- etl_batch_date parameter
            GROUP BY CAST(o.cancelledDate AS DATE),
                    o.dw_customer_id

            UNION ALL

            -- Shipped orders data
            SELECT CAST(o.shippedDate AS DATE) AS summary_date,
                o.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_amount,
                0 AS order_cost_amount,
                0 AS order_mrp_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                COUNT(DISTINCT o.dw_order_id) AS shipped_order_count,
                SUM(od.priceEach * od.quantityOrdered) AS shipped_order_amount,
                1 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Orders o
            JOIN devdw.OrderDetails od ON o.dw_order_id = od.dw_order_id
            WHERE o.shippedDate >= TO_DATE('{batch_date}', 'YYYY-MM-DD')  -- etl_batch_date parameter
            AND o.status = 'Shipped'
            GROUP BY CAST(o.shippedDate AS DATE),
                    o.dw_customer_id

            UNION ALL

            -- Payments data
            SELECT CAST(p.paymentDate AS DATE) AS summary_date,
                p.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_amount,
                0 AS order_cost_amount,
                0 AS order_mrp_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                1 AS payment_apd,
                SUM(p.amount) AS payment_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Payments p
            WHERE p.paymentDate >= TO_DATE('{batch_date}', 'YYYY-MM-DD')  -- etl_batch_date parameter
            GROUP BY CAST(p.paymentDate AS DATE),
                    p.dw_customer_id

            UNION ALL

            -- New customer data
            SELECT CAST(c.src_create_timestamp AS DATE) AS summary_date,
                c.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_amount,
                0 AS order_cost_amount,
                0 AS order_mrp_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                1 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Customers c
            WHERE c.src_create_timestamp >= TO_DATE('{batch_date}', 'YYYY-MM-DD')  -- etl_batch_date parameter
        )
        SELECT summary_date,
            dw_customer_id,
            MAX(order_count) AS order_count,
            MAX(order_apd) AS order_apd,
            MAX(order_amount) AS order_amount,
            MAX(order_cost_amount) AS order_cost_amount,
            MAX(order_mrp_amount) AS order_mrp_amount,
            MAX(products_ordered_qty) AS products_ordered_qty,
            MAX(products_items_qty) AS products_items_qty,
            MAX(cancelled_order_count) AS cancelled_order_count,
            MAX(cancelled_order_amount) AS cancelled_order_amount,
            MAX(cancelled_order_apd) AS cancelled_order_apd,
            MAX(shipped_order_count) AS shipped_order_count,
            MAX(shipped_order_amount) AS shipped_order_amount,
            MAX(shipped_order_apd) AS shipped_order_apd,
            MAX(payment_apd) AS payment_apd,
            MAX(payment_amount) AS payment_amount,
            MAX(new_customer_apd) AS new_customer_apd,
            MAX(new_customer_paid_apd) AS new_customer_paid_apd,
            CURRENT_TIMESTAMP AS create_timestamp,
            {batch_no} AS etl_batch_no,  -- etl_batch_no parameter
            TO_DATE('{batch_date}', 'YYYY-MM-DD') AS etl_batch_date  -- etl_batch_date parameter
        FROM CTE
        GROUP BY summary_date,
                dw_customer_id;
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
        print(f"Data successfully loaded into Redshift table devdw.daily_customer_summary.")
    except Exception as e:
        redshift_conn.rollback()
        print(f"Error loading data into devdw: {e}")
    finally:
        redshift_cursor.close()
        redshift_conn.close()

if __name__ == "__main__":
    main()