import subprocess

# List of scripts to execute
scripts = [
    "stage_to_dw/stage_to_dw_offices.py",
    "stage_to_dw/stage_to_dw_employees.py",
    "stage_to_dw/stage_to_dw_customers.py",
    "stage_to_dw/stage_to_dw_productlines.py",
    "stage_to_dw/stage_to_dw_products.py",
    "stage_to_dw/stage_to_dw_orders.py",
    "stage_to_dw/stage_to_dw_orderdetails.py",
    "stage_to_dw/stage_to_dw_payments.py",
    "stage_to_dw/stage_to_dw_CustHist.py",
    "stage_to_dw/stage_to_dw_ProdHist.py",
    "stage_to_dw/stage_to_dw_dcs.py",
    "stage_to_dw/stage_to_dw_dps.py",
    "stage_to_dw/stage_to_dw_mcs.py",
    "stage_to_dw/stage_to_dw_mps.py",

]

def run_script(script):
    """Runs an individual script as a subprocess."""
    try:
        print(f"Starting {script}...")
        result = subprocess.run(["python", script], text=True, check=True)
        print(f"{script} completed successfully.")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error occurred in {script}: {e.stderr}")
        return None

def main():
    print("Starting data loading from stage to dw")
    print("------------------------------------------------")
    # Run all scripts
    for script in scripts:
        run_script(script)
    print("Ending data loading from stage to dw")
    print("------------------------------------------------")

if __name__ == "__main__":
    main()
