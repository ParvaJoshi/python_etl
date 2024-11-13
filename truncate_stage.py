import redshift_connector
import os
from dotenv import load_dotenv

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

def truncate_stage_tables(cursor):
    """Truncates all tables in the devstage schema."""
    tables = [
        "customers",
        "payments",
        "employees",
        "offices",
        "orders",
        "orderdetails",
        "products",
        "productlines"
    ]
    for table in tables:
        query = f"TRUNCATE TABLE devstage.{table};"
        cursor.execute(query)

def main():
    # Load environment variables
    redshift_credentials = get_env_variables()
    # Connect to Redshift
    redshift_conn = connect_to_redshift(redshift_credentials)
    redshift_cursor = redshift_conn.cursor()
    # Truncate stage tables
    try:
        truncate_stage_tables(redshift_cursor)        
        redshift_conn.commit()
        print(f"devstage tables truncated successfully.")
    except Exception as e:
        redshift_conn.rollback()
        print(f"Error truncating devstage tables: {e}")
    finally:
        redshift_cursor.close()
        redshift_conn.close()

if __name__ == "__main__":
    main()