import os
import time
from datetime import datetime

import pandas as pd
import requests
import schedule
from bs4 import BeautifulSoup


# Define functions to extract data
def extract_data_from_page(html_content, metric):
    soup = BeautifulSoup(html_content, 'html.parser')

    ranks = [rank.get_text().strip() for rank in soup.find_all('td', class_='rank-td')]

    values_and_prices = []
    for rank_td in soup.find_all('td', class_='rank-td'):
        next_siblings = rank_td.find_next_siblings('td', class_='td-right')
        if len(next_siblings) >= 2:
            value = next_siblings[0].get_text(strip=True)
            price = next_siblings[1].get_text(strip=True)
            values_and_prices.append((value, price))

    def extract_name_and_code(name_div):
        company_name = name_div.find('div', class_='company-name').get_text(strip=True)
        company_code = name_div.find('div', class_='company-code').get_text(strip=True)
        return company_name, company_code

    name_divs = soup.find_all('div', class_='name-div')
    names_and_codes = [extract_name_and_code(name_div) for name_div in name_divs]

    today_spans = [
        f"-{span.get_text(strip=True)}" if 'percentage-red' in span.get('class', []) else span.get_text(strip=True)
        for span in soup.find('tbody').find_all('span')
        if 'percentage-green' in span.get('class', []) or 'percentage-red' in span.get('class', [])
    ]

    country_spans = soup.find_all('span', class_='responsive-hidden')
    countries = [span.get_text(strip=True) for span in country_spans if len(span.get('class', [])) == 1]

    current_date = datetime.today().strftime('%Y-%m-%d')

    return {
        'Rank': ranks,
        'Name': [name for name, code in names_and_codes],
        'Code': [code for name, code in names_and_codes],
        metric: [value for value, price in values_and_prices],
        'Price': [price for value, price in values_and_prices],
        'Today': today_spans,
        'Country': countries,
        'Date': [current_date] * len(ranks)
    }


# Function to scrape data for a specific metric
def scrape_metric_data(metric, initial_url, base_url, num_pages):
    all_data = []
    all_urls = [initial_url]  # Start with the initial URL

    for page_number in range(2, num_pages + 1):
        all_urls.append(base_url.format(page_number))

    for url in all_urls:
        response = requests.get(url)
        if response.status_code == 200:
            page_data = extract_data_from_page(response.content, metric)
            all_data.append(page_data)
        else:
            print(f"Failed to retrieve data from {url}")

    combined_data = {
        'Rank': [rank for data in all_data for rank in data['Rank']],
        'Company Name': [name for data in all_data for name in data['Name']],
        'Company Code': [code for data in all_data for code in data['Code']],
        metric: [value for data in all_data for value in data[metric]],
        'Price': [price for data in all_data for price in data['Price']],
        'Percentage Today': [today for data in all_data for today in data['Today']],
        'Company Country': [country for data in all_data for country in data['Country']],
        'Date': [date for data in all_data for date in data['Date']]
    }

    # Check lengths of lists in combined_data
    for key, value in combined_data.items():
        print(f"{key}: {len(value)}")

    # Find the minimum length
    min_length = min(len(value) for value in combined_data.values())

    # Trim lists to the minimum length
    for key in combined_data:
        combined_data[key] = combined_data[key][:min_length]

    return pd.DataFrame(combined_data)


# Function to scrape and save data for multiple metrics
def scrape_and_save_all_data():
    # Scrape data for each metric
    market_cap_df = scrape_metric_data('Market Cap', 'https://companiesmarketcap.com/',
                                       'https://companiesmarketcap.com/page/{}/', 86)
    earnings_df = scrape_metric_data('Earnings', 'https://companiesmarketcap.com/most-profitable-companies/',
                                     'https://companiesmarketcap.com/most-profitable-companies/page/{}/', 86)
    revenue_df = scrape_metric_data('Revenue', 'https://companiesmarketcap.com/largest-companies-by-revenue/',
                                    'https://companiesmarketcap.com/largest-companies-by-revenue/page/{}/', 86)
    employees_df = scrape_metric_data('Employees',
                                      'https://companiesmarketcap.com/largest-companies-by-number-of-employees/',
                                      'https://companiesmarketcap.com/largest-companies-by-number-of-employees/page/{}/',
                                      86)
    dividend_yield_df = scrape_metric_data('Dividend Yield',
                                           'https://companiesmarketcap.com/top-companies-by-dividend-yield/',
                                           'https://companiesmarketcap.com/top-companies-by-dividend-yield/page/{}/',
                                           86)
    total_assets_df = scrape_metric_data('Total Assets',
                                         'https://companiesmarketcap.com/top-companies-by-total-assets/',
                                         'https://companiesmarketcap.com/top-companies-by-total-assets/page/{}/', 86)
    net_assets_df = scrape_metric_data('Net Assets', 'https://companiesmarketcap.com/top-companies-by-net-assets/',
                                       'https://companiesmarketcap.com/top-companies-by-net-assets/page/{}/', 86)
    liabilities_df = scrape_metric_data('Liabilities',
                                        'https://companiesmarketcap.com/companies-with-the-highest-liabilities/',
                                        'https://companiesmarketcap.com/companies-with-the-highest-liabilities/page/{}/',
                                        86)
    debt_df = scrape_metric_data('Debt', 'https://companiesmarketcap.com/companies-with-the-highest-debt/',
                                 'https://companiesmarketcap.com/companies-with-the-highest-debt/page/{}/', 86)
    cash_on_hand_df = scrape_metric_data('Cash on Hand',
                                         'https://companiesmarketcap.com/companies-with-the-highest-cash-on-hand/',
                                         'https://companiesmarketcap.com/companies-with-the-highest-cash-on-hand/page/{}/',
                                         86)

    merged_df = (market_cap_df.merge(earnings_df[['Company Name', 'Company Code', 'Date', 'Earnings']], how='inner',
                                     on=['Company Name', 'Company Code', 'Date'])
                 .merge(revenue_df[['Company Name', 'Company Code', 'Date', 'Revenue']], how='inner',
                        on=['Company Name', 'Company Code', 'Date'])
                 .merge(employees_df[['Company Name', 'Company Code', 'Date', 'Employees']], how='inner',
                        on=['Company Name', 'Company Code', 'Date'])
                 .merge(dividend_yield_df[['Company Name', 'Company Code', 'Date', 'Dividend Yield']], how='inner',
                        on=['Company Name', 'Company Code', 'Date'])
                 .merge(total_assets_df[['Company Name', 'Company Code', 'Date', 'Total Assets']], how='inner',
                        on=['Company Name', 'Company Code', 'Date'])
                 .merge(net_assets_df[['Company Name', 'Company Code', 'Date', 'Net Assets']], how='inner',
                        on=(['Company Name', 'Company Code', 'Date'])
                        .merge(liabilities_df[['Company Name', 'Company Code', 'Date', 'Liabilities']], how='inner',
                               on=['Company Name', 'Company Code', 'Date'])
                        .merge(debt_df[['Company Name', 'Company Code', 'Date', 'Debt']], how='inner',
                               on=['Company Name', 'Company Code', 'Date']).merge(
                            cash_on_hand_df[['Company Name', 'Company Code', 'Date', 'Cash on Hand']], how='inner',
                            on=['Company Name', 'Company Code', 'Date'])))

    # Append the data to the existing file
    original_file_name = 'original_financial_metrics_data.csv'
    if not os.path.exists(original_file_name):
        merged_df.to_csv(original_file_name, index=False)
    else:
        existing_df = pd.read_csv(original_file_name)
        combined_df = pd.concat([existing_df, merged_df])
        combined_df.to_csv(original_file_name, index=False)

    # Create a copy for cleaning
    df = merged_df.copy()

    # Clean the data

    # 1. Replace 'N/A' with pd.NA
    df['Market Cap'] = df['Market Cap'].replace('N/A', pd.NA)

    # 2. Standardize all values to trillions
    def convert_to_trillions(value):
        if pd.isna(value):
            return value
        value = value.replace('$', '').replace(',', '')
        if 'T' in value:
            return float(value.replace('T', ''))
        elif 'B' in value:
            return float(value.replace('B', '')) / 1_000
        elif 'M' in value:
            return float(value.replace('M', '')) / 1_000_000
        else:
            return float(value) / 1_000_000_000_000

    df['Market Cap'] = df['Market Cap'].apply(convert_to_trillions)

    # 3. Rename the column
    df.rename(columns={'Market Cap': 'Market Cap In ($T)'}, inplace=True)

    # 1. Replace 'N/A' with pd.NA
    df['Price'] = df['Price'].replace('N/A', pd.NA)

    # 2. Convert all price values to numeric
    def convert_price_to_numeric(value):
        if pd.isna(value):
            return value
        return float(value.replace('$', '').replace(',', ''))

    df['Price'] = df['Price'].apply(convert_price_to_numeric)

    # 3. Rename the column to show its in dollars
    df.rename(columns={'Price': 'Price ($)'}, inplace=True)

    # 1. Replace 'N/A' with pd.NA
    df['Percentage Today'] = df['Percentage Today'].replace('N/A', pd.NA)

    # 2. Convert percentage values to numeric
    def convert_percentage(value):
        if pd.isna(value):
            return value
        if value.startswith('-'):
            return -float(value.replace('%', '').replace('-', '').replace(',', ''))
        else:
            return float(value.replace('%', '').replace(',', ''))

    df['Percentage Today'] = df['Percentage Today'].apply(convert_percentage)

    # 3. Rename the column to show it's a percentage
    df.rename(columns={'Percentage Today': 'Percentage Change (%)'}, inplace=True)

    # 1. Replace 'N/A' with pd.NA
    columns_to_clean = ['Earnings', 'Revenue', 'Debt', 'Liabilities', 'Total Assets', 'Net Assets', 'Dividend Yield',
                        'Cash on Hand', 'Employees']

    for column in columns_to_clean:
        df[column] = df[column].replace('N/A', pd.NA)

    # 2. Define a function to convert to numeric and standardize units to billions (if applicable), handling negatives
    def convert_to_numeric(value):
        if pd.isna(value):
            return value
        value = value.replace('$', '').replace(',', '')
        is_negative = value.startswith('-')
        value = value.replace('-', '')

        try:
            if 'T' in value:
                numeric_value = float(value.replace('T', '')) * 1_000
            elif 'B' in value:
                numeric_value = float(value.replace('B', ''))
            elif 'M' in value:
                numeric_value = float(value.replace('M', '')) / 1_000
            elif '%' in value:  # Specific for Dividend Yield
                numeric_value = float(value.replace('%', ''))
            else:
                numeric_value = float(value)

            if is_negative:
                numeric_value = -numeric_value

            return numeric_value
        except ValueError:
            return pd.NA

    # 3. Apply conversion function to each column
    for column in columns_to_clean:
        df[column] = df[column].apply(convert_to_numeric)

    # 4. Rename columns to indicate they are in billions or percentage
    rename_columns = {
        'Earnings': 'Earnings (B)',
        'Revenue': 'Revenue (B)',
        'Debt': 'Debt (B)',
        'Liabilities': 'Liabilities (B)',
        'Total Assets': 'Total Assets (B)',
        'Net Assets': 'Net Assets (B)',
        'Dividend Yield': 'Dividend Yield (%)',
        'Cash on Hand': 'Cash on Hand (B)',
        'Employees': 'Employees'
    }

    df.rename(columns=rename_columns, inplace=True)

    # Save the cleaned DataFrame
    cleaned_file_name = 'cleaned_data.csv'
    if not os.path.isfile(cleaned_file_name):
        df.to_csv(cleaned_file_name, index=False)
    else:
        existing_df = pd.read_csv(cleaned_file_name)
        combined_df = pd.concat([existing_df, df])
        combined_df.to_csv(cleaned_file_name, index=False)


# Schedule the job to run daily at 4:30 PM EDT
schedule.every().day.at("00:00").do(scrape_and_save_all_data)

print("Scheduler started. Waiting for the scheduled time to fetch data...")

while True:
    schedule.run_pending()
    time.sleep(1)
