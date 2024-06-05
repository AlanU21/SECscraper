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
    return pd.ExcelWriter('output1.xlsx', engine='openpyxl')

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







def detect_headers(df):
    header_row = None
    for index, row in df.iterrows():
        if 'Portfolio Company' in row.values:
            header_row = index
            break
    return header_row

def detect_table_type(headers):
    debt_keywords = {
        re.compile(r'Maturity\s*Date', re.IGNORECASE),
        re.compile(r'Interest\s*Rate\s*and\s*Floor', re.IGNORECASE),
        re.compile(r'Principal\s*Amount', re.IGNORECASE)
    }
    equity_keywords = {
        re.compile(r'Acquisition\s*Date', re.IGNORECASE),
        re.compile(r'Series', re.IGNORECASE),
        re.compile(r'Shares', re.IGNORECASE)
    }
    
    for header in headers:
        if isinstance(header, str):
            for pattern in debt_keywords:
                if pattern.search(header):
                    logging.info("Detected DEBT table")
                    return 'Debt'
            for pattern in equity_keywords:
                if pattern.search(header):
                    logging.info("Detected EQUITY table")
                    return 'Equity'
    return None


def get_expected_headers(table_type):
    if table_type == 'Debt':
        return [
            'Portfolio Company', 'Sub-Industry', 'Type of Investment', 'Maturity Date',
            'Interest Rate and Floor', 'Principal Amount', 'Cost', 'Value', 'Footnotes'
        ]
    elif table_type == 'Equity':
        return [
            'Portfolio Company', 'Sub-Industry', 'Type of Investment', 'Acquisition Date',
            'Series', 'Shares', 'Cost', 'Value', 'Footnotes'
        ]
    return []


def standardize_headers(headers, table_type):
    if table_type == 'Debt':
        header_mapping = {
            re.compile(r'Portfolio\s*Company', re.IGNORECASE): 'Portfolio Company',
            re.compile(r'Sub-Industry', re.IGNORECASE): 'Sub-Industry',
            re.compile(r'Type\s*of\s*Investment', re.IGNORECASE): 'Type of Investment',
            re.compile(r'Maturity\s*Date', re.IGNORECASE): 'Maturity Date',
            re.compile(r'Interest\s*Rate\s*and\s*Floor', re.IGNORECASE): 'Interest Rate and Floor',
            re.compile(r'Principal\s*Amount', re.IGNORECASE): 'Principal Amount',
            re.compile(r'Cost', re.IGNORECASE): 'Cost',
            re.compile(r'Value', re.IGNORECASE): 'Value',
            re.compile(r'Footnotes', re.IGNORECASE): 'Footnotes'
        }
    elif table_type == 'Equity':
        header_mapping = {
            re.compile(r'Portfolio\s*Company', re.IGNORECASE): 'Portfolio Company',
            re.compile(r'Sub-Industry', re.IGNORECASE): 'Sub-Industry',
            re.compile(r'Type\s*of\s*Investment', re.IGNORECASE): 'Type of Investment',
            re.compile(r'Acquisition\s*Date', re.IGNORECASE): 'Acquisition Date',
            re.compile(r'Series', re.IGNORECASE): 'Series',
            re.compile(r'Shares', re.IGNORECASE): 'Shares',
            re.compile(r'Cost', re.IGNORECASE): 'Cost',
            re.compile(r'Value', re.IGNORECASE): 'Value',
            re.compile(r'Footnotes', re.IGNORECASE): 'Footnotes'
        }

    standardized_headers = []
    for header in headers:
        standardized_header = header
        if isinstance(header, str):
            for pattern, standard_header in header_mapping.items():
                if pattern.search(header):
                    standardized_header = standard_header
                    break
            standardized_header = standardized_header.strip()
        standardized_headers.append(standardized_header)

    standardized_headers = [i for i in standardized_headers if i != 'nan']

    return standardized_headers



def concatenate_currency(df):
    columns = df.columns.tolist()
    
    for i in range(len(columns) - 1):
        col = columns[i]
        next_col = columns[i + 1]
        try:
            if df[col].dtype == 'object':
                df[col] = df.apply(
                    lambda row: f"{row[col]} {row[next_col]}" if isinstance(row[col], str) and row[col].strip() in "$€£¥" else row[col], 
                    axis=1
                )
                
        except KeyError as e:
            logging.error(f"KeyError in concatenate_currency: {str(e)}")
            continue
    
    return df

def drop_column_after_currency(df):
    columns_to_drop = []

    for col in df.columns:
        for idx, value in df[col].items():
            for char in "$€£¥":
                if char in str(value):
                    col_index = df.columns.get_loc(col)
                    if col_index + 1 < len(df.columns):
                        columns_to_drop.append(df.columns[col_index + 1])
                        break
            else:
                continue
            break
    
    print(columns_to_drop)
    df = df.drop(columns=columns_to_drop)
    
    return df

def remove_nans(row):
    return pd.Series([x for x in row if pd.notna(x)])

def clean_table(df):
    logging.info("Cleaning table")
    df = df.astype(str)
    df = df.map(lambda x: unicodedata.normalize('NFKD', str(x)).strip() if isinstance(x, str) else x)
    df = df[~df.map(lambda x: 'Subtotal' in x if isinstance(x, str) else False).any(axis=1)]
    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df = df.apply(lambda row: remove_nans(row), axis=1)
    df = df.reset_index(drop=True)
    

    df = concatenate_currency(df)
    print(df.loc[[4]])
    df = drop_column_after_currency(df)

    header_row = detect_headers(df)
    if header_row is not None:
        headers = df.iloc[header_row].values.tolist()
        headers = [header for header in headers if pd.notna(header)]
        table_type = detect_table_type(headers)
        if table_type:
            expected_headers = get_expected_headers(table_type)
            standardized_headers = standardize_headers(headers, table_type)
            print(df.loc[[4]])
            df.columns = standardized_headers
            df = df.iloc[header_row + 1:].reset_index(drop=True)
            
            for col in expected_headers:
                if col not in df.columns:
                    df[col] = np.nan 
            
            df = df[expected_headers]
            
        else:
            df.columns = [f'Column{i}' for i in range(len(df.columns))]
    else:
        df.columns = [f'Column{i}' for i in range(len(df.columns))]
    
    return df

def separate_frames(frames):
    cleaned_frames_debt = []
    cleaned_frames_equity = []
    for df in frames:
        try:
            cleaned_df = clean_table(df)
            table_type = detect_table_type(cleaned_df.columns)
            if table_type == 'Debt':
                cleaned_frames_debt.append(cleaned_df)
            elif table_type == 'Equity':
                cleaned_frames_equity.append(cleaned_df)
            
        except Exception as e:
            logging.error(f"Error cleaning and separating frame: {traceback.format_exc()}")
    
    logging.info(f"Number of cleaned debt frames: {len(cleaned_frames_debt)}")
    logging.info(f"Number of cleaned equity frames: {len(cleaned_frames_equity)}")

    return cleaned_frames_debt, cleaned_frames_equity









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

    for i in range(1):
        content = download_file(urls[i])

        date_format = datetime.strptime(str(date_reported[i]), "%Y-%m-%d %H:%M:%S")
        qtr_date = get_qtr_date(date_format)

        if content:
            logging.info(f"Processing file: {urls[i]}")
            soup = parse_html(content)
            soup = clean_html(soup)
            tables = extract_tables(soup, qtr_date)
            cleaned_debt, cleaned_equity = separate_frames(tables)
            
            debt_sheet_name = f"{qtr_date}_DEBT"
            equity_sheet_name = f"{qtr_date}_EQUITY"
            
            consolidated_debt = consolidate_data(cleaned_debt)
            consolidated_equity = consolidate_data(cleaned_equity)
            
            save_data(writer, consolidated_debt, debt_sheet_name)
            save_data(writer, consolidated_equity, equity_sheet_name)
            
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

