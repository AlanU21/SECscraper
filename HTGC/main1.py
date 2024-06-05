from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime
import requests
import pandas as pd
import numpy as np
import regex as re
import logging
import unicodedata
import traceback
import openpyxl
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)

def setup_writer():
    return pd.ExcelWriter('output.xlsx', engine='openpyxl')

def download_file(url):
    headers = {'User-Agent': "alanuthuppan@email.com"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        logging.error(f"Error downloading {url}: {str(e)}")
        return None

def parse_html(content):
    if content:
        return BeautifulSoup(content, 'html.parser')
    return None

def clean_html(soup):
    if soup:
        for tag in soup.recursiveChildGenerator():
            try:
                tag.attrs = {}
            except AttributeError:
                pass
        for linebreak in soup.find_all('br'):
            linebreak.extract()
    return soup

def get_qtr_date(date_format):
    month = int(date_format.strftime("%m"))
    year = date_format.strftime("%Y")
    quarter_ends = {12: "December 31", 9: "September 30", 6: "June 30", 3: "March 31"}
    return f"{quarter_ends.get(month, 'March 31')}, {year}"

def extract_tables(soup, qtr_date):
    tables = []
    count = 0
    logging.info(f"Current file: {qtr_date}")

    qtr_date = unicodedata.normalize('NFKD', qtr_date).replace('\xa0', ' ')
    phrase = 'CONSOLIDATED SCHEDULE OF INVESTMENTS'
    date_pattern = re.compile(r'\b' + re.escape(qtr_date) + r'\b', re.IGNORECASE)

    for tag in soup.find_all(string=re.compile(phrase)):
        sibling = tag.parent.parent.find_next_sibling()
        if sibling and date_pattern.search(unicodedata.normalize('NFKD', sibling.get_text()).replace('\xa0', ' ')) and tag.strip() == phrase:
            logging.info(f"Found date matching {qtr_date} near tag: {tag}")
            html_table = sibling.find_next('table')
            if html_table:
                count += 1
                try:
                    if len(tables) == 0:
                        new_table = pd.read_html(StringIO(str(html_table.prettify())), keep_default_na=False, skiprows=0, flavor='bs4')[0]
                        new_table = new_table.dropna(how='all', axis=0)
                        tables.append(new_table)
                    else:
                        new_table = pd.read_html(StringIO(str(html_table.prettify())), keep_default_na=False, skiprows=(0, 1), flavor='bs4')[0]
                        new_table = new_table.dropna(how='all', axis=0)
                        tables.append(new_table)
                except Exception as e:
                    logging.error(f"Failed to read HTML table near '{qtr_date}':", e)

    if tables:
        for table in tables:
            table.map(lambda x: unicodedata.normalize('NFKD', x.strip().strip(u'\u200b').replace('—', '0').replace('%', '').replace('(', '').replace(')', '')) if isinstance(x, str) else x)
            table = table.replace(r'^\s*$', np.nan, regex=True).replace(r'^\s*\$\s*$', np.nan, regex=True).replace(r'^\s*\)\s*$', np.nan, regex=True)

    logging.info(f"# of tables extracted: {count}")
    return tables





def clean_table(frames):
    cleaned = []
    for df in frames:
        logging.info("Cleaning table")
        df = df.astype(str)
        df = df.map(lambda x: unicodedata.normalize('NFKD', str(x)).strip() if isinstance(x, str) else x)
        df = df[~df.map(lambda x: 'Subtotal' in x if isinstance(x, str) else False).any(axis=1)]
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        cleaned.append(df)
    
    
    return cleaned



def consolidate_data(data_frames):
    if data_frames:
        master_table = pd.concat(data_frames, ignore_index=True)
        master_table.dropna(how='all', inplace=True)
        return master_table
    return pd.DataFrame()

def save_data(writer, data, sheet_name):
    if not data.empty:
        try:
            data.to_excel(writer, sheet_name=sheet_name, index=False)
        except Exception as e:
            logging.error(f"Failed to save data for {sheet_name}: {traceback.format_exc()}")

def scrape_data():
    writer = setup_writer()
    links = pd.read_excel("all_filings.xlsx")
    urls = links['Filings URL'].str.strip()
    date_reported = links['Reporting date']

    for i in range(0, 13):
        content = download_file(urls[i])

        date_format = datetime.strptime(str(date_reported[i]), "%Y-%m-%d %H:%M:%S")
        qtr_date = get_qtr_date(date_format)

        if content:
            logging.info(f"Processing file: {urls[i]}")
            soup = parse_html(content)
            soup = clean_html(soup)
            tables = extract_tables(soup, qtr_date)
            cleaned = clean_table(tables)
            combined = consolidate_data(cleaned)
            
            save_data(writer, combined, qtr_date)
            
            logging.info(f"Data processing for {qtr_date} completed successfully.")
        else:
            logging.warning(f"No data was downloaded for URL {urls[i]}")

    writer._save()
    writer.close()

    logging.info("All data has been processed and saved.")

def main():
    try:
        scrape_data()
    except Exception as e:
        logging.error(f"Unexpected error occurred: {traceback.format_exc()}")

if __name__ == "__main__":
    main()