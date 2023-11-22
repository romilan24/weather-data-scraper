import requests
import csv
import lxml.html as lh
import pandas as pd
from datetime import datetime

import config

from util.UnitConverter import ConvertToSystem
from util.Parser import Parser
from util.Utils import Utils

# configuration
stations_file = open('stations.txt', 'r')
URLS = stations_file.readlines()
# Date format: YYYY-MM-DD
START_DATE = config.START_DATE
END_DATE = config.END_DATE

# set to "metric" or "imperial"
UNIT_SYSTEM = config.UNIT_SYSTEM
# find the first data entry automatically
FIND_FIRST_DATE = config.FIND_FIRST_DATE


def scrap_station(weather_station_url):

    session = requests.Session()
    timeout = 5
    global START_DATE
    global END_DATE
    global UNIT_SYSTEM
    global FIND_FIRST_DATE

    url_gen = Utils.date_url_generator(weather_station_url, START_DATE, END_DATE)
    station_name = weather_station_url.split('/')[-1]
    raw_data = []  # To store the raw data for each station

    for url in url_gen:
        try:
            print(f'Scraping data from {url}')
            history_table = False
            while not history_table:
                html_string = session.get(url[1], timeout=timeout)  # Use the second element of the tuple as the URL
                doc = lh.fromstring(html_string.content)
                history_table = doc.xpath('//*[@id="main-page-content"]/div/div/div/lib-history/div[2]/lib-history-table/div/div/div/table/tbody')
                if not history_table:
                    print("refreshing session")
                    session = requests.Session()

            # parse html table rows
            data_rows = Parser.parse_html_table(url[0], history_table)

            # convert to metric system
            converter = ConvertToSystem(UNIT_SYSTEM)
            data_to_write = converter.clean_and_convert(data_rows)

            # Save raw data to the list
            raw_data.extend(data_to_write)

        except Exception as e:
            print(e)

    # Create a DataFrame from the raw data
    df = pd.DataFrame(raw_data, columns=['Date', 'Time', 'Temperature', 'Dew_Point', 'Humidity', 'Wind', 'Speed', 'Gust',
                                          'Pressure'])

    # Convert 'Date' and 'Time' columns to datetime and set as index
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    df.set_index('Datetime', inplace=True)

    # Resample data to hourly averages and round to the nearest hundredth decimal
    hourly_data = df.resample('H').mean().round(2)

    # Save hourly data to a new CSV
    hourly_file_name = f'{station_name}.csv'
    hourly_data.to_csv(hourly_file_name)


for url in URLS:
    url = url.strip()
    print(url)
    scrap_station(url)