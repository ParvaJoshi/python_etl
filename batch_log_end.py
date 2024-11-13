import os
import json
import redshift_connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

redshift_credentials = {
    'user': os.getenv('REDSHIFT_USER'),
    'password': os.getenv('REDSHIFT_PASSWORD'),
    'host': os.getenv('REDSHIFT_HOST'),
    'port': os.getenv('REDSHIFT_PORT')
}

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
        print("Redshift connection successful")
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

# Insert batch log into Redshift
def end_batch_log(conn, batch_no):
    try:
        cursor = conn.cursor()

        # SQL statement
        query = f"""
        UPDATE etl_metadata.batch_control_log
        SET etl_batch_status = 'C',
            etl_batch_end_time = CURRENT_TIMESTAMP
        WHERE etl_batch_no = {batch_no};
        """
        
        # Execute the SQL query
        cursor.execute(query)
        
        # Commit the transaction
        conn.commit()
        print("Batch log inserted successfully.")
        
    except Exception as e:
        print("Error inserting batch log:", e)
    finally:
        cursor.close()
        conn.close()

# Main function
if __name__ == "__main__":
    redshift_conn = connect_to_redshift(redshift_credentials)
    batch_no, batch_date = get_batch_details(redshift_conn)
    end_batch_log(redshift_conn, batch_no)
