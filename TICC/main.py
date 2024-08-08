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
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)



def setup_writer():
    return pd.ExcelWriter('cleaned_soi_tables.xlsx', engine='openpyxl')

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




###  EXTRACT  ###

def normalize_text(text):
    return unicodedata.normalize('NFKD', text).replace('\xa0', ' ').strip()

def create_flexible_pattern(some_str):
    parts = some_str.split()
    flexible_pattern = r'\s*'.join(map(re.escape, parts))
    return flexible_pattern



def extract_tables(soup, qtr_date):
    raw_tables = []
    count = 0
    logging.info(f"Current file: {qtr_date}")

    qtr_date = normalize_text(qtr_date)
    phrase = normalize_text('SCHEDULE OF INVESTMENTS')

    flexible_date_pattern = create_flexible_pattern(qtr_date)
    flexible_phrase_pattern = create_flexible_pattern(phrase)

    date_pattern = re.compile(r'\b' + flexible_date_pattern + r'\b', re.IGNORECASE)
    date_pattern1 = re.compile(r'\b' + re.escape(phrase) + r'\s*[\s\xa0]*' + flexible_date_pattern + r'\b', re.IGNORECASE)

    for tag in soup.find_all(string=re.compile(flexible_phrase_pattern)):
        parent = tag.parent
        date_found = False

        combined_text = ' '.join([normalize_text(t) for t in parent.stripped_strings])

        if date_pattern.search(combined_text):
            date_found = True
            logging.info(f"Found date matching {qtr_date} near combined text: {combined_text}")
        
        if not date_found:
            if date_pattern1.search(combined_text):
                date_found = True
                logging.info(f"Found date matching {qtr_date} near combined text: {combined_text}")

        if not date_found:
            sibling = parent.find_next_sibling()
            while sibling and sibling.name in ['font', 'span', 'p']:
                combined_text += ' ' + ' '.join([normalize_text(t) for t in sibling.stripped_strings])
                if date_pattern.search(combined_text):
                    date_found = True
                    logging.info(f"Found date matching {qtr_date} near combined text: {combined_text}")
                    break
                sibling = sibling.find_next_sibling()

        if not date_found:
            sibling = parent.parent.find_next_sibling()
            while sibling and sibling.name not in ['p', 'span']:
                sibling = sibling.find_next_sibling()

            if sibling and date_pattern.search(normalize_text(sibling.get_text())):
                date_found = True
                logging.info(f"Found date matching {qtr_date} near combined text: {combined_text}")
        

        
        if date_found:
            html_table = tag.parent.find_next('table')
            if html_table:
                try:
                    new_table = pd.read_html(StringIO(str(html_table.prettify())), keep_default_na=False, skiprows=0, flavor='bs4')[0]
                    new_table = new_table.dropna(how='all', axis=0)
                    new_table = new_table.dropna(how='all', axis=1)
                    raw_tables.append(new_table)

                except Exception as e:
                    logging.error(f"Failed to read HTML table near '{qtr_date}': {e}")
        else:
            logging.info(f"INCORRECT OR NO DATE FOUND near combined text: {combined_text}")
    
    if len(raw_tables) != 0:
        cleaned_tables = []
        for table in raw_tables:
            table = table.astype(str)
            table = table.replace(r'^\s*$', np.nan, regex=True)
            table = table.apply(lambda row: remove_nans(row), axis=1)

            currency_pattern = re.compile(r'[$€£¥₹]')
            percentage_pattern = re.compile(r'%')
            for i, row in table.iterrows():
                j = 0
                while j < len(row):
                    cell = row.iloc[j]
                    if currency_pattern.match(str(cell)):
                        if j + 1 < len(row):
                            next_cell = row.iloc[j + 1]
                            table.iat[i, j] = str(cell) + str(next_cell)
                            table.iloc[i, j + 1:] = table.iloc[i, j + 2:].values.tolist() + [None]
                            row = table.iloc[i]
                    
                    if percentage_pattern.match(str(cell)) and i != 0:
                        if j - 1 >= 0:
                            prev_cell = row.iloc[j - 1]
                            table.iat[i, j - 1] = str(prev_cell) + str(cell)
                            table.iloc[i, j:] = table.iloc[i, j + 1:].values.tolist() + [None]
                            row = table.iloc[i]

                    j += 1
                
            num_columns = table.shape[1]
            if num_columns == 8 or num_columns == 7:
                cleaned_tables.append(table)
                count += 1
            else:
                logging.info(f"REMOVING MISALIGNED TABLE with shape: {table.shape}")
        
        logging.info(f"# of tables extracted and initially cleaned: {count}")
        return cleaned_tables
    
    return None


###  EXTRACT  ###





###  CLEAN  ###

def remove_nans(row):
    return pd.Series([x for x in row if pd.notna(x)])

def detect_headers(df):
    header_row = None
    headers = None
    pattern = re.compile(r"COMPANY\s*/\s*INVESTMENT", re.IGNORECASE)


    for index, row in df.iterrows():
        if any(pattern.search(str(value)) for value in row):
            headers = row.values
            header_row = index
            return headers, header_row
        
    logging.info(f"No header row detected for df: {df}")
    return None, None


def consolidate_data(dataframes):
    if not dataframes:
        return pd.DataFrame()
    
    for i, df in enumerate(dataframes):
        empty_cols = df.columns[df.isna().all()]
        dataframes[i] = df.drop(empty_cols, axis=1)

    master_table = dataframes[0]
    headers = detect_headers(master_table)[0]

    for df in dataframes[1:]:
        current_headers, header_row = detect_headers(df)
        if header_row is not None:
            df = df.iloc[header_row+1:].reset_index(drop=True)
            if headers is None:
                headers = current_headers
            elif len(headers) != len(current_headers):
                logging.warning(f"Headers do not match across tables. Skipping inconsistent table.\nCurrent Headers:\n{current_headers}\nHeader Length: {len(current_headers)}\n{df}\n")
                continue
            df = df.reset_index(drop=True)
            master_table = pd.concat([master_table, df], ignore_index=True)
    
    master_table.dropna(how='all', inplace=True)
    master_table.columns = master_table.iloc[0]
    master_table = master_table.iloc[1:].reset_index(drop=True)

    logging.info(f"Consolidated data into a single table with shape {master_table.shape}")

    return master_table





def total_row_shift(df):
    if not df.empty:
        num_columns = df.shape[1]     
        for i, row in df.iterrows():
            if isinstance(row.iloc[0], str) and "Total" in row.iloc[0]:
                try:
                    if num_columns == 6:
                        row.iloc[3:6] = row.iloc[1:4]
                        row.iloc[1:3] = [np.nan, np.nan]
                    elif num_columns == 5:
                        row.iloc[2:5] = row.iloc[1:4]
                        row.iloc[1] = np.nan
                
                except Exception as e:
                    logging.error(f"'Total' row alignment error: {traceback.format_exc()}\n")
                    logging.error(f"ROW: {row}\n")
                    logging.error(df)

        return df


def extra_table_remover(df):
    pattern = re.compile(r"^\s*Total\s+Investments\s+in\s+Securities\s+and\s+Cash\s+Equivalents\s*$", re.IGNORECASE)
    if df is not None:
        for i, cell in enumerate(df.iloc[:, 0]):
            if isinstance(cell, str) and bool(pattern.match(str(cell).strip())):
                df = df.iloc[:i+1]
                logging.info("Extra table removed from this combined DataFrame")
                break
        return df

def first_and_last_check(df):
    try:
        if df.empty or df.shape[0] < 2:
            return False

        first_row_pattern = re.compile(r"^\s*Senior\s+Secured\s+Notes\s*$", re.IGNORECASE)
        last_row_pattern = re.compile(r"^\s*Total\s+Investments\s+in\s+Securities\s+and\s+Cash\s+Equivalents\s*$", re.IGNORECASE)
        
        first_row_phrase = df.iloc[0, 0]
        first_row_check = bool(first_row_pattern.match(str(first_row_phrase).strip()))

        last_row_phrase = df.iloc[-1, 0]
        last_row_check = bool(last_row_pattern.match(str(last_row_phrase).strip()))

        if first_row_check and last_row_check:
            return True
        else:
            logging.info(f"First cell: {first_row_phrase}\nLast cell: {last_row_phrase}")
            return False
    
    except Exception as e:
        logging.error(f"Failed first and last check: {traceback.format_exc()}")
        return False


### CLEAN ###




### WRITE ###

def save_data(writer, data, sheet_name):
    if data is not None:
        try:
            data.to_excel(writer, sheet_name=sheet_name, index=False)
        except Exception as e:
            logging.error(f"Failed to save data for {sheet_name}: {traceback.format_exc()}")


def post_process_excel(file_path):
    try:
        workbook = load_workbook(file_path)
        for sheet_name in workbook.sheetnames:
            worksheet = workbook[sheet_name]
            for column in worksheet.columns:
                max_length = 0
                column = list(column)
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                worksheet.column_dimensions[get_column_letter(column[0].column)].width = max_length


            for row in worksheet.iter_rows():
                if row[0].value and isinstance(row[0].value, str) and "Total" in row[0].value:
                    for cell in row:
                        cell.font = Font(bold=True)

            worksheet.freeze_panes = worksheet['A2']

        workbook.save(file_path)
        workbook.close()
        logging.info("Post-processing completed successfully.")
    except Exception as e:
        logging.error(f"Error during post-processing: {traceback.format_exc()}")



### WRITE ###

def scrape_data():
    writer = setup_writer()
    links = pd.read_excel("all_filings.xlsx")
    urls = links['Filings URL'].str.strip()
    date_reported = links['Reporting date']

    for i in range(25, 30):
        if urls[i]:
            content = download_file(urls[i])
        else:
            continue

        date_format = datetime.strptime(str(date_reported[i]), "%Y-%m-%d %H:%M:%S")
        qtr_date = get_qtr_date(date_format)

        if content:
            logging.info(f"Processing file: {urls[i]}")
            soup = parse_html(content)
            soup = clean_html(soup)
            tables = extract_tables(soup, qtr_date)

            combined = consolidate_data(tables)
            combined = total_row_shift(combined)

            first_last = first_and_last_check(combined)
            if not first_last:
                combined = extra_table_remover(combined)
            
                first_last = first_and_last_check(combined)

                if not first_last:
                    logging.error(f"SOME TABLES MAY BE MISSING FOR QTR DATE: {qtr_date}\n")
            
            save_data(writer, combined, qtr_date)
            
            logging.info(f"Data processing for {qtr_date} completed successfully.")
        else:
            logging.warning(f"No data was downloaded for URL {urls[i]}")

    writer._save()
    writer.close()

    post_process_excel('cleaned_soi_tables.xlsx')

    logging.info("All data has been processed and saved.")

def main():
    try:
        scrape_data()
    except Exception as e:
        logging.error(f"Unexpected error occurred: {traceback.format_exc()}")

if __name__ == "__main__":
    main()

