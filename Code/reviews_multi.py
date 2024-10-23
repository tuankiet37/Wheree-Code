import logging
import os
import time
from queue import Queue
from concurrent.futures import ProcessPoolExecutor, as_completed
from Utils import create_chrome_driver, collectRecordsTobeChecked, writeMainInfo, remove_accents, writeFile
import configparser
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from tqdm import tqdm

class CrawlReviews:
    def __init__(self, source_path, success_path, failed_path, num_drivers, header):
        self.num_drivers = num_drivers
        self.success_path = success_path
        self.failed_path = failed_path
        self.source_path = source_path
        self.header = header
        self.df = collectRecordsTobeChecked(self.success_path, self.failed_path, self.source_path, self.header)
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    
    def extract_data(self, soup, row):
        reviews = soup.find_all('div', class_='MyEned')
        reviewers = soup.find_all(class_='WNxzHc qLhwHc')
        times = soup.find_all(class_='rsqaWe')
        if not times:
            times = soup.find_all(class_='qmhsmd')

        result = []
        for review, reviewer, review_time in zip(reviews, reviewers, times):
            reviewer_name = reviewer.find('div', class_='d4r55').text or ''
            guide_status = reviewer.find('div', class_='RfnDt').text or ''
            time_info = review_time.text
            try:
                star_rating = review.find_previous('span', class_='kvMYJc').get('aria-label') or ''
            except:
                try:
                    star_rating = review.find_previous('span', class_='BHOKXe').get('aria-label') or ''
                except:
                    star_rating = ''
            
            review_text = review.find('span', class_='wiI7pd').text.replace('\n', '.') if review.find('span', class_='wiI7pd') else ''
            
            additional_info_blocks = review.find_all('div', class_='PBK6be')
            additional_info_text = []
            for block in additional_info_blocks:
                spans = block.find_all('span', class_='RfDO5c')
                for i in range(0, len(spans), 2):
                    category = spans[i].get_text(strip=True).replace('\n','')
                    value = spans[i + 1].get_text(strip=True).replace('\n','') if i + 1 < len(spans) else ''
                    additional_info_text.append(f"{category}: {value}")
            combined_additional_info = ', '.join(additional_info_text)

            result_extry = {
                'code': row['code'],
                'gg_url': row['map_url'],
                'reviewer': reviewer_name,
                'status': guide_status,
                'time': time_info,
                'star': star_rating,
                'review': remove_accents(review_text),
                'cat': combined_additional_info
            }
            result.append(result_extry)
        if result:
            writeFile(result, self.success_path)

    def parse(self, driver, row):
        if row['map_url'] == "https://www.google.com/maps" or "data=!" not in row['map_url']:
            result_entry = {
                "code": row['code'],
                'gg_url': row['map_url'],
            }
            writeMainInfo(result_entry, self.failed_path)
            return
        
        tabs = driver.window_handles
        if len(tabs) > 1:
            driver.switch_to.window(tabs[1])
            driver.close()
            driver.switch_to.window(tabs[0])

        wait = WebDriverWait(driver, 10)
        driver.get(row['map_url'])
        open_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[role="tab"][class="hh2c6 "][aria-label*="Reviews"]')))
        if open_button:
            open_button.click()
            time.sleep(1)  # To ensure the reviews section is loaded

            ### add send keys
            element = driver.find_element(By.CSS_SELECTOR, 'div.vyucnb')
            element.click()
            for _ in range(2):
                driver.find_element(By.CSS_SELECTOR, 'div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde').send_keys(Keys.END)
                time.sleep(0.5)
            time.sleep(1)
            more_buttons = driver.find_elements(By.CSS_SELECTOR, 'button.w8nwRe.kyuRq')
            if more_buttons:
                actions = ActionChains(driver)
                for more_button in more_buttons:
                    actions.move_to_element(more_button).click().perform()
                    time.sleep(0.5)

            reviews = driver.find_elements(By.CLASS_NAME, 'd4r55 ')
       
            if len(reviews)>=10:
                actions = ActionChains(driver)
                actions.move_to_element(reviews[-1]).perform()


                soup = BeautifulSoup(driver.page_source, 'html.parser')

                self.extract_data(soup, row)
            else:
                result_entry = {
                "code": row['code'],
                'gg_url': row['map_url'],
                }
                writeMainInfo(result_entry, self.failed_path)
        else:
            logging.error(f"Error processing URL {row['map_url']}: {e}")
            result_entry = {
                "code": row['code'],
                'gg_url': row['map_url'],
            }
            writeMainInfo(result_entry, self.failed_path)

    def process_row(self, row):
        driver = create_chrome_driver()
        try:
            self.parse(driver, row)
        finally:
            driver.quit()
    

    def crawl(self):
        start_time = time.time()

        with ProcessPoolExecutor(max_workers=self.num_drivers) as executor:
            futures = [executor.submit(self.process_row, row) for _, row in self.df.iterrows()]
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing URLs"):
                try:
                    future.result()
                    
                except Exception as e:
                    logging.error(f"Error in future: {e}")
        logging.info(f'Completed crawling MainExtract in {(time.time() - start_time) / 3600:.2f} hours.')

if __name__ == "__main__":

    source_path = r"D:\Wheree_Kiet\Sep\Output\Filter\main_hour_multi_(success).csv"
    base_dir = r"D:\Wheree_Kiet\Sep\Output\Filter"
    file_prefix = "reviews_sep_"
    success_path = os.path.join(base_dir, f"{file_prefix}(success).csv")
    failed_path = os.path.join(base_dir, f"{file_prefix}(missing).csv")

    output_header = 'code,gg_url,reviewer,status,time,star,review,cat\n'

    crawler = CrawlReviews(source_path, success_path, failed_path, num_drivers=15, header=output_header)
    crawler.crawl()
