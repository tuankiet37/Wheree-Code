import os
import time
import logging
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from tqdm import tqdm 
from Utils import collectRecordsTobeChecked, writeMainInfo, create_chrome_driver, writeFile

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
def extract_features(code, map_url, success_features_path, failed_features_path):
    results = []
    driver = create_chrome_driver()  # Create driver for features extraction
    try:
        driver.get(map_url)
        
        # Wait for the body to load completely
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        
        # Now wait for the About button to be clickable
        buttonAbout = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '//button[@role="tab" and contains(@aria-label, "About")]'))
        )
        buttonAbout.click()
        
        # Ensure the relevant elements are present before proceeding
        WebDriverWait(driver, 30).until(
    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde'))
)
        
        # Give the page some time to fully render
        time.sleep(2)  # Adjust based on page load time
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        div_element = soup.findAll('div', class_='iP2t7d fontBodyMedium')
        if not div_element:
            div_element = soup.findAll('div', class_='CK16pd dc6iWb')
            for feature in div_element:
                feature_avai = feature.find('span', class_='fontBodySmall gSamH')
                if feature_avai:
                    result_entry = {
                        'code': code,
                        'main_feature': 'Amenities',
                        'feature': feature_avai.get_text(strip=True),
                    }
                    results.append(result_entry)
        else:
            for tags in div_element:
                main_feature = tags.find('h2', class_='iL3Qke fontTitleSmall').get_text(strip=True)
                features = tags.findAll('div', class_='iNvpkb SwaGS fontBodyMedium')
                for feature in features:
                    feature_text = feature.get_text(strip=True).replace('\n', ' ')
                    result_entry = {
                        'code': code,
                        'main_feature': main_feature,
                        'feature': feature_text,
                    }
                    results.append(result_entry)

        if results:
            writeFile(results, success_features_path)
        #else:
        #    logging.warning(f"No results found for code {code}")

    except Exception as e:
        #logging.error(f"Error extracting features for code {code}: {e}", exc_info=True)
        result_entry = {
            'code': code,
            'main_feature': '',
            'feature': '',
        }
        writeMainInfo(result_entry, failed_features_path)

    finally:
        driver.quit()  # Ensure driver is closed after each task

def extract_features_wrapper(args):
    return extract_features(*args)

def main(success_features_path, failed_features_path, source_path, num_cpus):
    features_headers = "code,main_feature,feature\n"

    df = collectRecordsTobeChecked(success_features_path, failed_features_path, source_path, features_headers)
    total_urls = len(df)
    logging.info(f"Total URLs to process: {total_urls}")

    # Prepare the arguments for the map function
    args = [(row['code'], row['map_url'], success_features_path, failed_features_path) for _, row in df.iterrows()]

    # Use process pool executor to parallelize the work and tqdm to track progress
    with ProcessPoolExecutor(max_workers=num_cpus) as executor:
        list(tqdm(executor.map(extract_features_wrapper, args), total=len(args), desc="Processing URLs"))


if __name__ == '__main__':
    start = time.time()
    source_path = r"\\Admin-pc\data\2024\Oct\Output\Main\18_10_2024_main_hour_(success).csv"
    output_folder = r"D:\DATA\2024\Oct\Output\Features"
    os.makedirs(output_folder, exist_ok=True)
    
    success_features_path = os.path.join(output_folder, '19_10_features_(success).csv')
    failed_features_path = os.path.join(output_folder, '19_10_features_(failed).csv')

    num_cpus = 20
    
    main(success_features_path, failed_features_path, source_path, num_cpus)
    print(f"Time taken: {round((time.time() - start) / 3600, 2)} hours")

    time.sleep(300)
