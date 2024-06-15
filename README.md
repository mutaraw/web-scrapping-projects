# Web-scrapping-projects
 
# Financial Metrics Data Scraper

This project involves scraping financial metrics data from various web sources and saving it for further analysis. The data includes metrics such as market capitalization, earnings, revenue, number of employees, dividend yield, total assets, net assets, liabilities, debt, and cash on hand. The scripts are designed to run daily to keep the data updated.

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Scripts Overview](#scripts-overview)
- [Data Cleaning](#data-cleaning)
- [Contributing](#contributing)
- [License](#license)

## Installation

1. **Clone the repository:**
    ```sh
    git clone https://github.com/yourusername/financial-metrics-scraper.git
    cd financial-metrics-scraper
    ```

2. **Create a virtual environment and activate it:**
    ```sh
    python3 -m venv env
    source env/bin/activate  # On Windows, use `env\Scripts\activate`
    ```

3. **Install the required packages:**
    ```sh
    pip install -r requirements.txt
    ```

4. **Install the `schedule` package if not included in `requirements.txt`:**
    ```sh
    pip install schedule
    ```

## Usage

To run the scripts manually at a specific time, simply execute the desired script. For example, to run the market cap scraper:
```sh
python market_cap_scraper.py
```
To run all scripts at once, use the master script:

python run_all_scrapers.py

## Scripts Overview

## market_cap_scraper.py
- Scrapes market capitalization data.
- Saves the data to market_cap_data.csv.

## earnings_scraper.py
- Scrapes earnings data.
- Saves the data to earnings_data.csv.

## revenue_scraper.py
- Scrapes revenue data.
- Saves the data to revenue_data.csv.

## employees_scraper.py
- Scrapes the number of employees data.
- Saves the data to employees_data.csv.

## dividend_yield_scraper.py
- Scrapes dividend yield data.
- Saves the data to dividend_yield_data.csv.

## total_assets_scraper.py
- Scrapes total assets data.
- Saves the data to total_assets_data.csv.

## net_assets_scraper.py
- Scrapes net assets data.
- Saves the data to net_assets_data.csv.

## liabilities_scraper.py
- Scrapes liabilities data.
- Saves the data to liabilities_data.csv.

## debt_scraper.py
- Scrapes debt data.
- Saves the data to debt_data.csv.

## cash_on_hand_scraper.py
- Scrapes cash on hand data.
- Saves the data to cash_on_hand_data.csv.

## run_all_scrapers.py
- Runs all the individual scrapers sequentially.
- master_scheduler.py
- (Optional) Uses schedule to run all scripts at a specified time daily.

## Data Cleaning

The data is cleaned to ensure consistency and usability:

- Replace 'N/A' with pd.NA.
- Convert values to standard units:
- Market cap values are converted to trillions.
- Other financial metrics are converted to billions.
- Rename columns to reflect the units.
- The cleaned data is saved to cleaned_data.csv.

## Contributing

**Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.**

- Fork the repository.
- Create a new branch (git checkout -b feature-branch).
- Make your changes.
- Commit your changes (git commit -am 'Add new feature').
- Push to the branch (git push origin feature-branch).
- Create a new Pull Request.

## License

- This project is licensed under the MIT License. See the LICENSE file for details.
