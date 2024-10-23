import logging
import re
import os
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from multiprocessing import Pool, Manager
from Utils import create_chrome_driver, writeMainInfo, collectRecordsTobeChecked, writeFile
from tqdm import tqdm
from selenium.webdriver.common.keys import Keys 
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ImgScraper:
    def __init__(self, source_path, success_path, failed_path, num_drivers, header):
        self.success_path = success_path
        self.failed_path = failed_path
        self.source_path = source_path
        self.num_drivers = num_drivers
        self.df = collectRecordsTobeChecked(success_path, failed_path, source_path, header)
        df = pd.read_csv(source_path)
        # df = df[len(df)//2:int(len(df)*0.75)]   
        # self.df = self.df[len(self.df)//2:int(len(self.df)*0.75)]
        self.df = self.df[self.df['code'].isin(df['code'])]
        self.menu = False


    def extract_and_modify_url(self, style_att):
        match = re.search(r'url\("([^"]+)"\)', style_att)
        return re.sub(r'=.*', '=s0', match.group(1)) if match else None

    def extract_images(self, driver):
        images = []
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        for img_elem in soup.find_all('a', class_=re.compile('OKAoZd')):
            video_label_elem = img_elem.find('div', class_='fontLabelMedium a3lFge')
            if video_label_elem and "video" in video_label_elem.text.lower():
                continue

            style_att = img_elem.find('div', class_='U39Pmb').get('style')
            url = self.extract_and_modify_url(style_att)

            if url and 'googleusercontent.com/p/' in url:
                images.append(url)

        return images
    

    def send_end(self, driver, times=10):
        time.sleep(1)
        div_element = driver.find_element(By.CLASS_NAME, "m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde")
        for _ in range(times):
            time.sleep(0.5)
            div_element.send_keys(Keys.END)

    def parse(self, driver, row):
        try:
            wait = WebDriverWait(driver, 5)
            wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label, 'Photo of')]"))).click()
        except:
            try: 
                driver.find_element(By.CLASS_NAME, 'd6JfQc').click()
            except:
                writeMainInfo({"code": row['code'], "image": "", "type": '', "map_url": row['map_url']}, self.failed_path)
                return

        self.send_end(driver, 8)
        general_images = self.extract_images(driver)
        owner_images = self.get_owner_images(driver)

        general_images = [img for img in general_images if img not in owner_images]

        menu_images = self.get_menu_images(driver)
        menu_images = [img for img in menu_images if img not in general_images]
        menu_images = [img for img in menu_images if img not in owner_images]

        if len(general_images) > 6:
            self.save_images(row, general_images, 'IMAGE', 30)
            if len(menu_images) >= 6:
                self.save_images(row, list(menu_images), 'MENU', 30)

        else:
            writeMainInfo({"code": row['code'], "image": len(general_images) , "type": '', "map_url": row['map_url']}, self.failed_path)

    def get_menu_images(self, driver):
        menu_images = []
        menu_path = "//button[@role='tab' and contains(@class, 'hh2c6') and .//div[contains(@class, 'Gpq6kf') and contains(@class, 'fontTitleSmall') and contains(text(), 'Menu')]]"
        try:
            menu_button = driver.find_element(By.XPATH, menu_path)
            menu_button.click()
            self.send_end(driver, 8)
            menu_images = self.extract_images(driver)
            self.menu = True
        except:
            pass

        return menu_images
    
    def get_owner_images(self, driver):
        owner_images = []
        owner_path = "//button[@role='tab' and contains(@class, 'hh2c6') and .//div[contains(@class, 'Gpq6kf') and contains(@class, 'fontTitleSmall') and contains(text(), 'By owner')]]"
        try:
            owner_button = driver.find_element(By.XPATH, owner_path)
            owner_button.click()
            self.send_end(driver, 5)
            owner_images = self.extract_images(driver)  
        except:
            pass
        return owner_images

    def save_images(self, row, images, image_type, max_images):
        if not images:
            return
        
        if self.menu:
            menu_check = 'x'
        else:
            menu_check = ''

        result = []
        first_link = images[0]
        result_entry = {
            "code": row['code'],
            "image": first_link,
            "type": 'MAIN' if image_type == 'IMAGE' else image_type,
            "menu_check": menu_check,
            "map_url": row['map_url']
        }
        result.append(result_entry)

        for link in images[1:max_images]:
            result.append({"code": row['code'], "image": link, "type": image_type, "menu_check": menu_check, "map_url": row['map_url']})
        writeFile(result, self.success_path)

    def process_row(self, row):
        if 'data=!' not in row['map_url']:
            writeMainInfo({"code": row['code'], "image": "", "type": '', "menu_check": '', "map_url": row['map_url']}, self.failed_path)
            return
        driver = create_chrome_driver() 
        try:
            driver.get(row['map_url'])
            try:
                wait = WebDriverWait(driver, 3)
                element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "vrdm1c K2FXnd Oz0bd oNZ3af")))  
                element.click()
            except:
                try:
                    driver.find_element(By.CLASS_NAME, "vfi8qf qgMOee").click()
                except:
                    pass
                pass
            self.parse(driver, row)
        except Exception as e:
            writeMainInfo({"code": row['code'], "image": "", "type": '', "map_url": row['map_url']}, self.failed_path)
        finally:
            driver.quit() 
            # self.webdriver_pool.put(driver)

    def crawl_images_by_place(self):
        if self.df.empty:
            logging.warning("No records to process.")
            return

        with Pool(processes=self.num_drivers) as pool:
            for _ in tqdm(pool.imap_unordered(self.process_row, [row for _, row in self.df.iterrows()]), total=len(self.df), desc="Processing URLs"):
                pass


if __name__ == '__main__':
    start_time = time.time()
    source_path = r"D:\DATA\2024\Oct\Output\Main\11_10_2024_main_hour_(success).csv"
    base_dir = r"D:\DATA\2024\Oct\Output\Img"
    os.makedirs(base_dir, exist_ok=True)
    file_prefix = "21_10_img_"
    success_path = os.path.join(base_dir, f"{file_prefix}(success).csv")
    failed_path = os.path.join(base_dir, f"{file_prefix}(failed).csv")
    output_header = 'code,image,type,menu_check,map_url\n'

    # Initialize ImgScraper
    scraper = ImgScraper(source_path, success_path, failed_path, num_drivers=50, header=output_header)

    scraper.crawl_images_by_place()

    time.sleep(5)
    logging.info(f"Crawling image done in {round((time.time()-start_time)/3600, 2)} hours")
