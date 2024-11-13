import os
import oracledb
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database credentials
un = os.getenv('ORACLE_USERNAME')
userpwd = os.getenv('ORACLE_PASSWORD')
connect_string = os.getenv('ORACLE_DSN')
oracledb.init_oracle_client(lib_dir=os.getenv('d'))
# Variables for database link creation
db_link_username = os.getenv('DBLINK_USERNAME')  # e.g., cm_20010109
db_link_password = os.getenv('DBLINK_PASSWORD')  # e.g., cm_20010109123
db_link_name = "parva_dblink"  # Adjust if link name varies


def manage_db_link():
    try:
        # Connect to Oracle
        connection = oracledb.connect(user=un, password=userpwd, dsn=connect_string)
        with connection.cursor() as cursor:
            # Set the schema
            cursor.execute("ALTER SESSION SET current_schema = h24parva")
            print("Schema set to 'h24parva'.")

            # Drop existing database link
            try:
                cursor.execute(f"DROP PUBLIC DATABASE LINK {db_link_name}")
                print(f"Database link '{db_link_name}' dropped.")
            except oracledb.DatabaseError as e:
                print(f"Database link '{db_link_name}' does not exist or could not be dropped: {e}")

            # Create the new database link
            create_link_query = f"""
                CREATE PUBLIC DATABASE LINK {db_link_name}
                CONNECT TO {db_link_username}
                IDENTIFIED BY {db_link_password}
                USING 'XE'
            """
            cursor.execute(create_link_query)
            print(f"Database link '{db_link_name}' created successfully for '{db_link_username}'.")

            # Commit changes
            connection.commit()

    except oracledb.DatabaseError as e:
        print("Error managing database link:", e)
    finally:
        if 'connection' in locals():
            connection.close()

# Run the function to manage the database link
manage_db_link()
