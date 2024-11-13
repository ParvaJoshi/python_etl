import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys

# Set the current directory to the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# List of setup scripts that must run successfully before export scripts
setup_scripts = ["etl_batch_update.py", "update-db-link.py"]

# List of export scripts to run in parallel if setup scripts succeed
export_scripts = [
    "oracle_to_s3/offices.py",
    "oracle_to_s3/customers.py",
    "oracle_to_s3/employees.py",
    "oracle_to_s3/payments.py",
    "oracle_to_s3/products.py",
    "oracle_to_s3/orders.py",
    "oracle_to_s3/productlines.py",
    "oracle_to_s3/orderdetails.py"
]

# Function to run a single script and capture output
def run_script(script_name):
    try:
        result = subprocess.run(
            ["python", script_name], capture_output=True, text=True, check=True
        )
        print(f"{script_name} completed successfully:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error in {script_name}:\n{e.stderr}")
        return False

# Main function to execute all export scripts in parallel
def run_all_exports():
    dir = sys.path[0]
    with ThreadPoolExecutor() as executor:

        futures = {executor.submit(run_script, f'{dir}\\{script}'): script for script in export_scripts}
        
        for future in as_completed(futures):
            script = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"{script} generated an exception: {e}")

if __name__ == "__main__":
    # Run setup scripts sequentially
    print("Running setup scripts...")
    for setup_script in setup_scripts:
        if not run_script(setup_script):
            print("Setup script failed. Halting execution.")
            exit(1)  # Exit the script if any setup script fails

    # If setup scripts succeed, proceed to run export scripts in parallel
    print("Running export scripts in parallel...")
    run_all_exports()
