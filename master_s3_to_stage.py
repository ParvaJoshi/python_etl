import subprocess
from multiprocessing import Pool


# List of scripts to execute
scripts = [
    "s3_to_dev(redshift)/s3_to_redshift_customers.py",
    "s3_to_dev(redshift)/s3_to_redshift_employees.py",
    "s3_to_dev(redshift)/s3_to_redshift_offices.py",
    "s3_to_dev(redshift)/s3_to_redshift_orderdetails.py",
    "s3_to_dev(redshift)/s3_to_redshift_orders.py",
    "s3_to_dev(redshift)/s3_to_redshift_payments.py",
    "s3_to_dev(redshift)/s3_to_redshift_productlines.py",
    "s3_to_dev(redshift)/s3_to_redshift_products.py"
]

def run_script(script):
    """Runs an individual script as a subprocess."""
    try:
        print(f"Starting {script}...")
        result = subprocess.run(["python", script], capture_output=True, text=True, check=True)
        print(f"{script} completed successfully.")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error occurred in {script}: {e.stderr}")
        return None

def main():
    # Use multiprocessing Pool to run scripts in parallel
    subprocess.run(["python", "truncate_stage.py"])
    with Pool(processes=len(scripts)) as pool:
        pool.map(run_script, scripts)

if __name__ == "__main__":
    main()