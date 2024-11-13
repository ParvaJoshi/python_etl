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
        WITH CTE AS
(
  SELECT TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE AS start_of_the_month_date, -- Date formatting in PostgreSQL
         dps.dw_product_id,
         SUM(dps.customer_apd) AS customer_apd,
         CASE WHEN MAX(dps.customer_apd) > 0 THEN 1 ELSE 0 END AS customer_apm,
         SUM(dps.product_cost_amount) AS product_cost_amount,
         SUM(dps.product_mrp_amount) AS product_mrp_amount,
         SUM(dps.cancelled_product_qty) AS cancelled_product_qty,
         SUM(dps.cancelled_cost_amount) AS cancelled_cost_amount,
         SUM(dps.cancelled_mrp_amount) AS cancelled_mrp_amount,
         SUM(dps.cancelled_order_apd) AS cancelled_order_apd,
         CASE WHEN MAX(dps.cancelled_order_apd) > 0 THEN 1 ELSE 0 END AS cancelled_order_apm
  FROM devdw.daily_product_summary dps
  WHERE dps.summary_date >= TO_DATE('{batch_date}', 'YYYY-MM-DD')  -- Replaced with parameter placeholder
  GROUP BY TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE, dps.dw_product_id
) 
UPDATE devdw.monthly_product_summary
SET
    customer_apd = mps.customer_apd + c.customer_apd,
    customer_apm = (mps.customer_apm::int | c.customer_apm::int),  -- Bitwise OR, casting booleans to integers
    product_cost_amount = mps.product_cost_amount + c.product_cost_amount,
    product_mrp_amount = mps.product_mrp_amount + c.product_mrp_amount,
    cancelled_product_qty = mps.cancelled_product_qty + c.cancelled_product_qty,
    cancelled_cost_amount = mps.cancelled_cost_amount + c.cancelled_cost_amount,
    cancelled_mrp_amount = mps.cancelled_mrp_amount + c.cancelled_mrp_amount,
    cancelled_order_apd = mps.cancelled_order_apd + c.cancelled_order_apd,
    cancelled_order_apm = (mps.cancelled_order_apm::int | c.cancelled_order_apm::int),  -- Bitwise OR
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = {batch_no},  -- Replaced with parameter placeholder
    etl_batch_date = TO_DATE('{batch_date}', 'YYYY-MM-DD')   -- Replaced with parameter placeholder
FROM CTE c
JOIN devdw.monthly_product_summary mps
ON mps.start_of_the_month_date = c.start_of_the_month_date
  AND mps.dw_product_id = c.dw_product_id;
        """
    insert_query =  f"""
       INSERT INTO devdw.monthly_product_summary (
   start_of_the_month_date,
   dw_product_id,
   customer_apd,
   customer_apm, 
   product_cost_amount,
   product_mrp_amount,
   cancelled_product_qty,
   cancelled_cost_amount,
   cancelled_mrp_amount,
   cancelled_order_apd,
   cancelled_order_apm,
   etl_batch_no,
   etl_batch_date
)
SELECT
   TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE AS start_of_the_month_date, -- Date formatting in PostgreSQL
   dps.dw_product_id, 
   SUM(dps.customer_apd),
   CASE WHEN MAX(dps.customer_apd) > 0 THEN 1 ELSE 0 END AS customer_apm,
   SUM(dps.product_cost_amount),
   SUM(dps.product_mrp_amount),
   SUM(dps.cancelled_product_qty),
   SUM(dps.cancelled_cost_amount),
   SUM(dps.cancelled_mrp_amount),
   SUM(dps.cancelled_order_apd),
   CASE WHEN MAX(dps.cancelled_order_apd) > 0 THEN 1 ELSE 0 END AS cancelled_order_apm,
   {batch_no},  -- Replaced with parameter placeholder
   TO_DATE('{batch_date}', 'YYYY-MM-DD')    -- Replaced with parameter placeholder
FROM devdw.daily_product_summary dps
LEFT JOIN devdw.monthly_product_summary mps
  ON TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE = mps.start_of_the_month_date
  AND dps.dw_product_id = mps.dw_product_id
WHERE mps.dw_product_id IS NULL
GROUP BY TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE, dps.dw_product_id;
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
        print(f"Data successfully loaded into Redshift table devdw.monthly_customer_summary.")
    except Exception as e:
        redshift_conn.rollback()
        print(f"Error loading data into devdw: {e}")
    finally:
        redshift_cursor.close()
        redshift_conn.close()

if __name__ == "__main__":
    main()