import os
import logging
import csv
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.chrome.options import Options as ChromeOptions
import re
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.chrome.service import Service as ChromeService
from fake_useragent import UserAgent
import time
from unicodedata import normalize, combining
import configparser
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import multiprocessing
from selenium.webdriver.chrome.service import Service
import numpy as np
# import unicodedata


def remove_accents(text):
    """Removes accents from text in various languages and performs additional cleaning."""
    if pd.isna(text) or not str(text).strip():
        return ''
    
    # Normalize accents
    normalized = normalize('NFKD', text)
    text_without_accents = ''.join(c for c in normalized if not combining(c))
    
    # Replace tabs with spaces
    text_without_tabs = text_without_accents.replace('\t', '')
    
    # Trim leading and trailing whitespace and normalize spaces
    clean_text = ' '.join(text_without_tabs.split())
    
    return clean_text

def parselongGoolonggleLongLat(url: str) -> dict:
    patterns = [
        r'3d(-?\d+\.\d+).*?4d(-?\d+\.\d+)',
        r'@(-?\d+\.\d+),(-?\d+\.\d+)'
    ]

    # Initialize latitude and longitude to empty strings
    latitude = ''
    longitude = ''

    # Iterate over patterns and try to find a match
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            latitude = match.group(1)
            longitude = match.group(2)
            break

    # Return a dictionary with the results
    return {'latitude': latitude, 'longitude': longitude}


def cleanUnicode(input_str: str) -> str:
    try:
        unicode_pattern = re.compile(r'[\uE000-\uF8FF]|\n')
        return unicode_pattern.sub('', input_str)
    except:
        return input_str

def create_firefox_driver():
    def get_random_user_agent():
        user_agent = UserAgent()
        return user_agent.random

    def get_options(config):
        options = FirefoxOptions()
        
        # Set headless mode if specified in config
        if config.getboolean('firefox', 'headless', fallback=False):
            options.add_argument('--headless')

        # Set window size if specified in config
        window_size = config.get('firefox', 'window_size', fallback=None)
        if window_size:
            width, height = window_size.split(',')
            options.add_argument(f'--width={width}')
            options.add_argument(f'--height={height}')

        # Add additional Firefox options
        for option in [
            'disable_infobars',
            'disable_extensions',
            'disable_popup_blocking',
            'ignore_ssl_errors',
            'ignore_certificate_errors',
            'no_sandbox',
            'disable_dev_shm_usage',
            'headless'
        ]:
            if config.getboolean('firefox', option, fallback=False):
                options.add_argument(f'--{option.replace("_", "-")}')

        # Set up Firefox profile
        profile = webdriver.FirefoxProfile()
        profile.set_preference("browser.cache.disk.enable", False)
        # profile.set_preference("browser.cache.memory.enable", False)
        profile.set_preference("browser.cache.offline.enable", False)
        profile.set_preference("network.http.use-cache", False)

        profile.set_preference("browser.cache.memory.enable", True)  # Enable memory caching for faster access
        profile.set_preference("browser.cache.memory.capacity", 1024)
        profile.set_preference("dom.webnotifications.enabled", False)  # Block web notifications
        profile.set_preference("dom.push.enabled", False)  # Block push notifications
        profile.set_preference("permissions.default.geo", 2)  # Block geolocation pop-ups
        profile.set_preference("permissions.default.desktop-notification", 2)  # Block notification pop-ups # Block microphone access pop-ups
        profile.set_preference("dom.popup_maximum", 0)
        # Disable image loading
        profile.set_preference("browser.urlbar.autoFill", False)  # Disable URL bar autofill
        profile.set_preference("browser.urlbar.autocomplete.enabled", False)  # Disable URL autocomplete


        # profile.set_preference('permissions.default.image', 2)

        options.profile = profile
        return options

    # Generate a random user agent
    user_agent = get_random_user_agent()
    config_path = r'Code\Utils\config.ini'
    config = configparser.ConfigParser()
    config.read(config_path)
    
    options = get_options(config)

    options.set_preference("general.useragent.override", user_agent)  # Use the correct method to set user agent

    driver = webdriver.Firefox(options=options)
    
    # driver.set_page_load_timeout(90)
    # driver.set_window_size(1024, 768)
    return driver


def create_chrome_driver():
    config_path = r'Code\Utils\config.ini'
    config = configparser.ConfigParser()
    config.read(config_path)
    
    def get_chrome_options(config):
        options = ChromeOptions()

        # Set headless mode if specified in config
        if config.getboolean('chrome', 'headless', fallback=False):
            options.add_argument('--headless=old')
            # options.add_argument('--headless=chrome')

        # Set window size if specified in config
        window_size = config.get('chrome', 'window_size', fallback=None)
        if window_size:
            width, height = window_size.split(',')
            options.add_argument(f'--window-size={width},{height}')

        # Add additional Chrome options
        for option in [
            'disable_infobars',
            'disable_extensions',
            'disable_popup_blocking',
            'ignore_ssl_errors',
            'ignore_certificate_errors',
            'no_sandbox',
            'disable_dev_shm_usage',
            'disable_gpu',
            'disable_background_timer_throttling',
            'disable_backgrounding_occluded_windows',
            'disable_client_side_phishing_detection',
            'disable_crash_reporter',
            'disable_oopr_debug_crash_dump',
            'no_crash_upload',
            'disable_low_res_tiling',
            'silent',
            'disable-long-res-tiling'
        ]:
            if config.getboolean('chrome', option, fallback=False):
                options.add_argument(f'--{option.replace("_", "-")}')

        options.add_argument('--log-level=3')
        options.add_argument('--disable-logging')  
        options.add_argument('--disk-cache-size=0')  
        options.add_argument('--disable-application-cache')

        prefs = {
        #  "profile.managed_default_content_settings.images":2,
         "profile.default_content_setting_values.notifications":2,
         "profile.managed_default_content_settings.stylesheets":2,
         "profile.managed_default_content_settings.cookies":2,
        #  "profile.managed_default_content_settings.javascript":1,
        #  "profile.managed_default_content_settings.plugins":1,
         "profile.managed_default_content_settings.popups":2,
         "profile.managed_default_content_settings.geolocation":2,
         "profile.managed_default_content_settings.media_stream":2,
         }
        options.add_experimental_option("prefs", prefs)

        return options

    chrome_options = get_chrome_options(config)


    # Suppress Selenium logs
    logging.getLogger('WDM').setLevel(logging.NOTSET)
    chrome_service = Service(log_path='NUL')

    # Initialize the Chrome driver
    driver = webdriver.Chrome(options=chrome_options, service=chrome_service)
    # driver = webdriver.Chrome(options=chrome_options)
    return driver

def prepare_file(file_path, file_header):
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            with open(file_path, 'w', newline='') as file:
                file.write(file_header)
        logging.info(f"Crawler - {file_path} - File path checking completed.")
    except Exception as e:
        logging.error(f"Crawler - File path error..!! - {file_header} - {e}")


# write_lock = threading.Lock()
# write_lock = threading.RLock()
def writeMainInfo(result_entry: dict, file_name: str) -> None:
    write_lock = multiprocessing.Lock() 
    # time.sleep(1)
    try:
        with write_lock:
            with open(file_name, mode='a', newline='', encoding='utf-8-sig') as file:
                writer = csv.DictWriter(f=file, quoting=csv.QUOTE_MINIMAL, fieldnames=result_entry.keys())
                writer.writerow(result_entry)
    except Exception as e:
        logging.error(f'Crawler - FAILED at "writeMainInfo()" - {e}.')

# write_lock = threading.Lock()   
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

    
def collectRecordsTobeChecked(success_path: str, failed_path: str, source_path: str, file_header: str) -> pd.DataFrame:
    def filter_dataframes(df):
        """Filters a DataFrame, dropping rows where the length of 'code' is not 36."""
        return df[df['code'].str.len() == 36]
    try:
        dest_header = file_header
        prepare_file(success_path, dest_header)
        prepare_file(failed_path, dest_header)


        df_source = pd.read_csv(source_path,on_bad_lines="skip",engine='python', quotechar='"')

        df_success = pd.read_csv(success_path,on_bad_lines="skip",engine='python', quotechar='"') 
        df_failed = pd.read_csv(failed_path,on_bad_lines="skip",engine='python', quotechar='"') 

        df_source = filter_dataframes(df_source)
        df_success = filter_dataframes(df_success)
        df_failed = filter_dataframes(df_failed)
    
        success_codes = set(df_success['code'].dropna().unique())
        failed_codes = set(df_failed['code'].dropna().unique())
        excluded_codes = success_codes.union(failed_codes)

        processing_df = df_source[~df_source['code'].isin(excluded_codes)]

        processing_df = processing_df.drop_duplicates(subset='code', keep='first')

        logging.info(f"Total batch records: {len(df_source)} records, "
                        f"Skipped: {len(excluded_codes)} records, "
                        f"Ordered to be processed: {len(processing_df)}")

        return processing_df

    except Exception as e:
        logging.info(f"Crawler - FAILED at 'collectRecordsTobeChecked()' - {e}.")
        return pd.DataFrame()
    
def create_driver_extension():   
    
    def init_extension(driver): 
        wait = WebDriverWait(driver, 15)
        extension_protocol = "chrome-extension"
        extension_id = "bihmplhobchoageeokmgbdihknkjbknd"  


        index_page = f"{extension_protocol}://{extension_id}/panel/index.html"
        driver.get(index_page)
        time.sleep(4)
        
        tabs = driver.window_handles
        if len(tabs) > 1:
            driver.switch_to.window(tabs[1])
            driver.close()
            driver.switch_to.window(tabs[0])

        wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="Main"]/div[1]/div/span'))).click() # choice
        wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="Locations"]/div[2]/div[2]'))).click() # best
        # wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="Locations"]/div[2]/div[3]'))).click() # us
        # wait.until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="Locations"]/div[2]/div[{np.random.randint(2, 9)}]'))).click() #randome

        wait.until(EC.element_to_be_clickable((By.ID, "ConnectionButton"))).click() # connect_button
        time.sleep(7)
        driver.refresh()

    extension_path = r"Code\Utils\Touch VPN.crx"
    options = webdriver.ChromeOptions()
    options.add_extension(extension_path)
    # options.add_argument("--headless=old")
    options.add_argument('--log-level=3')
    # options.add_argument('--log-level=OFF')
    options.add_argument('--disable-logging')  
    options.add_argument('--silent') 
 
    driver = webdriver.Chrome(options=options)
    init_extension(driver)

    return driver
    
   