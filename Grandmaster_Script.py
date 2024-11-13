import subprocess
import time

# List of scripts to execute
scripts = [
    "masterscript(redshift).py",
    "master_s3_to_stage.py",
    "master_stage_to_dw.py"
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
    start_time = time.time()  # Start the timer
    # batch_log start
    subprocess.run(["python", "batch_log_start.py"])
    # Run all scripts
    for script in scripts:
        run_script(script)
    # batch_log end
    subprocess.run(["python", "batch_log_end.py"])
    end_time = time.time()  # End the timer
    print(f"Total time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
