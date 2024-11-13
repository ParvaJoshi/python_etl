import os
import oracledb
from dotenv import load_dotenv

load_dotenv()

# Database credentials
un = os.getenv('ORACLE_USERNAME')
userpwd = os.getenv('ORACLE_PASSWORD')
connect_string = os.getenv('ORACLE_DSN')
oracledb.init_oracle_client(lib_dir=os.getenv('d'))

# Fetch batch control number and date from environment variables
batch_control_no = os.getenv('BATCH_CONTROL_NO')  # Ensure the variable name is in uppercase
batch_control_date = os.getenv('BATCH_CONTROL_DATE')  # Ensure the variable name is in uppercase



# Update function
def update_batch_control(batch_no, batch_date):
    query = """
        UPDATE h24parva.batch_control 
        SET etl_batch_no = :batch_no, 
            etl_batch_date = TO_DATE(:batch_date, 'YYYY-MM-DD')
    """
    try:
        connection = oracledb.connect(user=un, password=userpwd, dsn=connect_string)
        with connection.cursor() as cursor:
            cursor.execute(query, batch_no=batch_no, batch_date=batch_date)
            connection.commit()
            print("Batch control table updated successfully.")
    except oracledb.DatabaseError as e:
        print("Error updating batch control:", e)
    finally:
        if 'connection' in locals():
            connection.close()

# Usage
# update_batch_control(1001, '2001-01-01')
update_batch_control(batch_control_no, batch_control_date)
