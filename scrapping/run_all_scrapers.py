import subprocess
import time

# List of scripts to run
scripts = [
    'financial_metrics_scraper.py',
    'market_cap_scraper.py',
    'earnings_scraper.py',
    'revenue_scraper.py',
    'employees_scraper.py',
    'dividend_yield_scraper.py',
    'total_assets_scraper.py',
    'net_assets_scraper.py',
    'liabilities_scraper.py',
    'debt_scraper.py',
    'cash_on_hand_scraper.py'
]

# Run each script
processes = []
for script in scripts:
    process = subprocess.Popen(['python', script])
    processes.append(process)
    time.sleep(2)  # Optional: Delay between starting scripts

# Wait for all scripts to complete
for process in processes:
    process.wait()

print("All scraping scripts have completed.")
