import os
import time
from datetime import datetime

import pandas as pd
import requests
import schedule
from bs4 import BeautifulSoup


# Define function to extract data from the net assets page
def extract_data_from_net_assets_page(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    ranks = [rank.get_text().strip() for rank in soup.find_all('td', class_='rank-td')]

    net_assets_and_prices = []
    for rank_td in soup.find_all('td', class_='rank-td'):
        next_siblings = rank_td.find_next_siblings('td', class_='td-right')
        if len(next_siblings) >= 2:
            net_assets = next_siblings[0].get_text(strip=True)
            price = next_siblings[1].get_text(strip=True)
            net_assets_and_prices.append((net_assets, price))

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
        'Net Assets': [net_assets for net_assets, price in net_assets_and_prices],
        'Price': [price for net_assets, price in net_assets_and_prices],
        'Today': today_spans,
        'Country': countries,
        'Date': [current_date] * len(ranks)
    }


# Function to scrape and save data for net assets
def scrape_and_save_net_assets_data(initial_url, base_url, num_pages, file_name):
    all_data = []
    all_urls = [initial_url]  # Start with the initial URL

    for page_number in range(2, num_pages + 1):
        all_urls.append(base_url.format(page_number))

    for url in all_urls:
        response = requests.get(url)
        if response.status_code == 200:
            page_data = extract_data_from_net_assets_page(response.content)
            all_data.append(page_data)
        else:
            print(f"Failed to retrieve data from {url}")

    combined_data = {
        'Rank': [rank for data in all_data for rank in data['Rank']],
        'Company Name': [name for data in all_data for name in data['Name']],
        'Company Code': [code for data in all_data for code in data['Code']],
        'Net Assets': [net_assets for data in all_data for net_assets in data['Net Assets']],
        'Price': [price for data in all_data for price in data['Price']],
        'Percentage Today': [today for data in all_data for today in data['Today']],
        'Company Country': [country for data in all_data for country in data['Country']],
        'Date': [date for data in all_data for date in data['Date']]
    }

    net_assets_df = pd.DataFrame(combined_data)

    # Append the data to the existing file
    if not os.path.isfile(file_name):
        net_assets_df.to_csv(file_name, index=False)
    else:
        existing_df = pd.read_csv(file_name)
        combined_df = pd.concat([existing_df, net_assets_df])
        combined_df.to_csv(file_name, index=False)

    print(f"Data for Net Assets saved to {file_name}")


# Define URLs and file name for net assets
initial_url = 'https://companiesmarketcap.com/top-companies-by-net-assets/'
base_url = 'https://companiesmarketcap.com/top-companies-by-net-assets/page/{}/'
file_name = 'net_assets_data.csv'


# Function to run the scraping job
def job():
    scrape_and_save_net_assets_data(initial_url, base_url, 86, file_name)
    print(f"Data fetched and saved at {datetime.now()}")


# Schedule the job to run daily at 4:30 PM EDT
schedule.every().day.at("00:00").do(job)

print("Scheduler started. Waiting for the scheduled time to fetch data...")

while True:
    schedule.run_pending()
    time.sleep(1)
