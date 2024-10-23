from unidecode import unidecode
import pandas as pd
import logging
import threading
import csv
import time
import os
import multiprocessing


def prepare_file(file_path, file_header):
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            with open(file_path, 'w', newline='') as file:
                file.write(file_header)
        logging.info(f"Crawler - {file_path} - File path checking completed.")
    except Exception as e:
        logging.error(f"Crawler - File path error..!! - {file_header} - {e}")


write_lock = threading.Lock()
def writeMainInfo(result_entry: dict, file_name: str) -> None:
    time.sleep(0.5)
    try:
        with write_lock:
            with open(file_name, mode='a', newline='', encoding='utf-8-sig') as file:
                writer = csv.DictWriter(f=file, quoting=csv.QUOTE_MINIMAL, fieldnames=result_entry.keys())
                writer.writerow(result_entry)
    except Exception as e:
        logging.error(f'Crawler - FAILED at "writeMainInfo()" - {e}.')
        
def collectRecordsTobeChecked(success_path: str, failed_path: str, source_path: str, file_header: str) -> pd.DataFrame:
    # def filter_dataframes(df):
    #     """Filters a DataFrame, dropping rows where the length of 'code' is not 36."""
    #     return df[df['code'].str.len() == 36]
    try:
        dest_header = file_header
        prepare_file(success_path, dest_header)
        prepare_file(failed_path, dest_header)


        df_source = pd.read_csv(source_path,on_bad_lines="skip",engine='python', quotechar='"')

        df_success = pd.read_csv(success_path,on_bad_lines="skip",engine='python', quotechar='"') 
        df_failed = pd.read_csv(failed_path,on_bad_lines="skip",engine='python', quotechar='"') 

        # df_source = filter_dataframes(df_source)
        # df_success = filter_dataframes(df_success)
        # df_failed = filter_dataframes(df_failed)
    
        success_codes = set(df_success['query'].dropna().unique())
        failed_codes = set(df_failed['query'].dropna().unique())
        excluded_codes = success_codes.union(failed_codes)

        processing_df = df_source[~df_source['query'].isin(excluded_codes)]

        logging.info(f"Total batch records: {len(df_source)} records, "
                        f"Skipped: {len(excluded_codes)} records, "
                        f"Ordered to be processed: {len(processing_df)}")

        return processing_df

    except Exception as e:
        logging.info(f"Crawler - FAILED at 'collectRecordsTobeChecked()' - {e}.")
        return pd.DataFrame()
def unicode_to_ascii(text):
    """
    Convert unicode text to ASCII, replacing special characters.
    """
    
    if text is None:
        return None

    # Replacing 'ë' with 'e' and return the ASCII text
    return unidecode(text).replace("ë", "e")

def unique_strings(lst):
    # Use a set to remove duplicates, then convert back to a list
    return list(dict.fromkeys(lst))


def writeFile(result_entries: list, file_name: str) -> None:
    write_lock = multiprocessing.Lock()
    # time.sleep(1)
    try:
        with write_lock:
            file_exists = os.path.exists(file_name) and os.path.getsize(file_name) > 0

            # Open file in append mode
            with open(file_name, mode='a', newline='', encoding='utf-8-sig') as file:
                # Use the fieldnames from the first entry
                if result_entries:
                    writer = csv.DictWriter(file, quoting=csv.QUOTE_MINIMAL, fieldnames=result_entries[0].keys())

                    # Write the header if the file is empty
                    if not file_exists:
                        writer.writeheader()

                    # Write all rows from the list
                    writer.writerows(result_entries)

    except Exception as e:
        logging.error(f'Crawler - FAILED at "writeMainInfo()" - {e}.')

