import logging
import os
import time
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from Utils import create_driver_extension, writeMainInfo, collectRecordsTobeChecked, create_chrome_driver
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LocationScraper:
    def __init__(self, source_path, success_path, failed_path, num_drivers, header):
        self.success_path = success_path
        self.failed_path = failed_path
        self.source_path = source_path
        self.num_drivers = num_drivers
        self.df = collectRecordsTobeChecked(success_path, failed_path, source_path, header)
        self.df.rename(columns={'fulladdress': 'address', 'orignalname': 'name'}, inplace=True)
        self.webdriver_pool = Queue(maxsize=num_drivers + 1)

    def initialize_webdrivers(self):
        # for _ in tqdm(range(self.num_drivers), desc="Initializing WebDrivers", leave=True):
        for _ in range(self.num_drivers):
            try:
                self.webdriver_pool.put(create_driver_extension())
            except Exception as e:
                logging.error(f"Error initializing WebDriver: {e}")

    def parse(self, driver, row):
        # Check if the row has a place_id
        if 'place_id' in row:
            # If place_id exists, use it to construct the URL and extract data
            place_id_url = f"https://developers-dot-devsite-v2-prod.appspot.com/maps/documentation/utils/geocoder/#place_id%3D{row['place_id']}"
            self.process_place_id(driver, place_id_url, row)
        else:
            # Construct a search value from name and address fields if no place_id
            # search_value = f"{row['name']} {row['address']}"
            self.process_search_value(driver, row)

    def process_place_id(self, driver, place_id_url, row):
        """Process and extract data using place_id."""
        data_collected = False
        try:
            for _ in range(50):  # Retry up to 50 times
                driver.get(place_id_url)
                # WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                # time.sleep(5)
                wait = WebDriverWait(driver, 10)
                wait.until(EC.presence_of_element_located((By.ID, "result-0")))

                bs4 = BeautifulSoup(driver.page_source, 'html.parser')
                result_div = bs4.find('div', id="result-0")

                if result_div:
                    data_collected = True
                    result_entry = {}
                    result_entry = self.extract_data_from_result(result_div, row)
                    
                    if self.has_valid_data(result_entry):
                        writeMainInfo(result_entry, self.success_path)
                        return  # Data collected successfully
                    
                time.sleep(3)
            
            if not data_collected:
                self.log_failed_entry(row)
                
        except Exception as e:
            # logging.error(f"Error while collecting data for place_id {row['place_id']}: {e}")
            self.log_failed_entry(row)

    def process_search_value(self, driver, row):
        """Process and extract data using search value."""
        base_url = "https://developers-dot-devsite-v2-prod.appspot.com/maps/documentation/utils/geocoder/"
        search_value = f"{row['name']} {row['address']}"
        data_collected = False
        try:
            driver.get(base_url)
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            idlogin = WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[id="query-input"][placeholder="Enter a location"]')))

            for _ in range(50):  # Retry up to 50 times
                try:
                    idlogin.clear()
                    idlogin.send_keys(search_value)
                    time.sleep(1)
                    
                    try:
                        opensuggest = driver.find_element(By.CSS_SELECTOR, 'div[class="pac-item"]')
                        opensuggest.click()
                    except NoSuchElementException:
                        openReview = driver.find_element(By.CSS_SELECTOR, 'input[id="geocode-button"]')
                        openReview.click()
                    time.sleep(2)
                    
                    bs4 = BeautifulSoup(driver.page_source, 'html.parser')
                    result_div = bs4.find('div', id="result-0")

                    if result_div:
                        data_collected = True
                        result_entry = {}
                        result_entry = self.extract_data_from_result(result_div, row)
                        
                        if self.has_valid_data(result_entry):
                            writeMainInfo(result_entry, self.success_path)
                            return

                    time.sleep(5)
                except Exception as e:
                    # logging.error(f"Error during search value processing: {e}")
                    time.sleep(5)
            
            if not data_collected:
                self.log_failed_entry(row)
                
        except Exception as e:
            # logging.error(f"Error while collecting data for {search_value}: {e}")
            self.log_failed_entry(row)

    def extract_data_from_result(self, result_div, row):
        """Extract data from the result div in the page."""
        lv0_or, lv1_or, lv2_or, lv3_or, lv4_or, locality, lv1_short = '', '', '', '', '', '', ''
        
        address_table = result_div.find('table', class_='address-components')
        if address_table:
            tr_tags = address_table.find_all('tr')
            for tr in tr_tags:
                td_tags = tr.find_all('td', class_='vtop')
                if len(td_tags) >= 2:
                    key = td_tags[0].text.strip()
                    value = td_tags[1].text.strip()
                    if key == 'country' and not lv0_or:
                        lv0_or = value
                    elif key == 'administrative_area_level_1' and not lv1_or:
                        lv1_or = value
                        try:
                            lv1_short = value.split('\n')[1].strip()
                        except IndexError:
                            pass
                    elif key == 'administrative_area_level_2' and not lv2_or:
                        lv2_or = value
                    elif key == 'administrative_area_level_3' and not lv3_or:
                        lv3_or = value
                    elif key == 'administrative_area_level_4' and not lv4_or:
                        lv4_or = value
                    elif key == 'sublocality_level_1' and not locality:
                        locality = value

        location = result_div.find('p', class_='result-location')
        if location:
            location = location.text.strip().split(':', 1)[-1].strip()

        try:
            lv0_or = lv0_or.replace('\n', ' ').replace('\t', '-').split('\n')[0].strip().split('   ')[0].strip()
            lv1_or = lv1_or.replace('\n', ' ').replace('\t', '-').split('\n')[0].strip().split('   ')[0].strip()
            lv2_or = lv2_or.replace('\n', ' ').replace('\t', '-').split('\n')[0].strip().split('   ')[0].strip()
            lv3_or = lv3_or.replace('\n', ' ').replace('\t', '-').split('\n')[0].strip().split('   ')[0].strip()
            lv4_or = lv4_or.replace('\n', ' ').replace('\t', '-').split('\n')[0].strip().split('   ')[0].strip()
            locality = locality.replace('\n', ' ').replace('\t', '-').split('\n')[0].strip().split('   ')[0].strip()
            lv1_short = lv1_short.replace('\n', ' ').replace('\t', '-')
        except:
            pass
        
        return {
            'code': row['code'],
            'map_url': row['map_url'],
            'address': row['address'],
            'latitude': str(location.split(',')[0].strip()),
            'longitude': str(location.split(',')[1].split('   ')[0].strip()),
            'lv0_or': lv0_or if lv0_or else 'N/A',
            'lv1_or': lv1_or if lv1_or else 'N/A',
            'lv2_or': lv2_or if lv2_or else 'N/A',
            'lv3_or': lv3_or if lv3_or else 'N/A',
            'lv4_or': lv4_or if lv4_or else 'N/A',
            'sublocality': locality if locality else 'N/A',
            'lv1_short': lv1_short if lv1_short else 'N/A'
        }

    def has_valid_data(self, result_entry):
        """ Check if any column in the result_entry has a value other than 'N/A' """
        return any(value != 'N/A' for key, value in result_entry.items() if key not in ['code', 'map_url', 'address', 'latitude', 'longitude'])

    def log_failed_entry(self, row):
        """Log a failed entry to the failed file."""
        failed_entry = {
            'code': row['code'],
            'map_url': row['map_url'],
            'address': row['address'],
            'latitude': 'N/A',
            'longitude': 'N/A',
            'lv0_or'
            'lv1_or'
            'lv2_or'
            'lv3_or'
            'lv4_or'
            'sublocality'
            'lv1_short': 'N/A'
        }
        writeMainInfo(failed_entry, self.failed_path)

    def process_row(self, row, webdriver_pool: Queue):
        driver = webdriver_pool.get()
        try:
            self.parse(driver, row)
        except Exception as e:
            logging.error(f"Error processing row: {e}")
        finally:
            webdriver_pool.put(driver)

    def crawl_location(self):
        if self.df.empty:
            logging.warning("No records to process.")
            return
        self.initialize_webdrivers()
        self.df.rename(columns={'orignalname': 'name', 'fulladdress': 'address'}, inplace=True)

        with ProcessPoolExecutor(max_workers=self.num_drivers) as executor:
            futures = [executor.submit(self.process_row, row, self.webdriver_pool) for _, row in self.df.iterrows()]
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing URLs", leave=True):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error in future result: {e}")

        self.shutdown_webdrivers()

    def shutdown_webdrivers(self):
        while not self.webdriver_pool.empty():
            driver = self.webdriver_pool.get()
            try:
                driver.quit()
            except Exception as e:
                logging.error(f"Error shutting down WebDriver: {e}")

if __name__ == '__main__':
    start_time = time.time()
    source_path = r"D:\DATA\2024\Aug\Output\Final\16_10_rerun_location.csv"
    base_dir = r"D:\DATA\2024\Aug\Output\Final"
    os.makedirs(base_dir, exist_ok=True)
    file_prefix = "16_10_2024_rerun_location_"
    success_path = os.path.join(base_dir, f"{file_prefix}(success).csv")
    failed_path = os.path.join(base_dir, f"{file_prefix}(failed).csv")
    output_header = 'code,map_url,address,latitude,longitude,lv0_or,lv1_or,lv2_or,lv3_or,lv4_or,sublocalityh,lv1_short\n'

    scraper = LocationScraper(source_path, success_path, failed_path, num_drivers=10, header=output_header)

    scraper.crawl_location()
    logging.info(f"Crawling place done in {round((time.time()-start_time)/3600, 2)} hours")
