import logging
import os
import time
from Utils import create_chrome_driver, collectRecordsTobeChecked, writeMainInfo, parselongGoolonggleLongLat, cleanUnicode, convert_emebed, contains_pictographic, remove_emoji, translate_text, writeFile, prepare_file
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm
from selenium.webdriver.common.action_chains import ActionChains
from multiprocessing import Pool
import re

class MainCrawler:
    def __init__(self, source_path, success_path, failed_path, success_path_aff, num_drivers, header):
        self.num_drivers = num_drivers
        self.success_path = success_path
        self.failed_path = failed_path
        self.source_path = source_path
        self.header = header
        self.processing_df = collectRecordsTobeChecked(self.success_path, self.failed_path, self.source_path, self.header)
        self.processing_df = self.processing_df[['code','map_url','place_id']]
        self.processing_df = self.processing_df[::-1]


        self.success_path_aff = success_path_aff
        aff_header = "code,name,link,type,map_url\n"
        prepare_file(self.success_path_aff, aff_header)
        self.setup_logging()

    
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


    def aff_link(self, driver, row):
        result_list = []
        draft_urls = []
        type = ''

        try:
            click_order = WebDriverWait(driver, 1).until(
                EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Place an order')]"))
            )
            click_order.click()
        except:
            pass

        order_list = driver.find_elements(By.CSS_SELECTOR, 'div[class*="RcCsl fVHpi rXaZJb"]')
        try:
            href = driver.find_element(By.CSS_SELECTOR, 'a[class="CsEnBe"][data-tooltip="Place an order"]').get_attribute('href')
            result_list.append(href)
        except:
            # print("No main")
            pass

        if len(order_list) > 0:
            for order in order_list:
                href = order.find_element(By.CSS_SELECTOR, 'a[class*="CsEnBe"]').get_attribute('href')
                href = re.sub(r'\?utm.*', '', href)
                result_list.append(href)
            type = 'Restaurant'
        else:
            order_list = driver.find_elements(By.CSS_SELECTOR, 'div[class*="Jqsiwd fontBodyMedium M4DWu"]')
            for order in order_list:
                href = order.find_element(By.CSS_SELECTOR, 'a[target*="_blank"]').get_attribute('href')
                draft_urls.append(href)
                time.sleep(1)
            type = 'Accommodation'

        if len(draft_urls) > 0:
            for url in draft_urls:
                driver.get(url)
                WebDriverWait(driver, 10).until(EC.url_changes(url))
                href = driver.current_url
                result_list.append(href)

        results = []
        for url in result_list:
            pattern = r'(?<=\/\/)(.*?)(?=\/)'
            match = re.search(pattern, url)
            extracted_url = match.group(0) if match else ''
            dot_index = extracted_url.count(".")
            if dot_index != -1:
                extracted_url = extracted_url.split('.')[dot_index - 1:]
                extracted_url = ".".join(extracted_url)

            result_entry = {
                'code': row['code'],
                'name': extracted_url,
                'link': url,
                'type': type,
                'map_url': row['map_url'],
            }
            results.append(result_entry)

        if results:
            writeFile(results, self.success_path_aff)

        
    def parse(self, driver, row):
        
        driver.get(row['map_url'])
        wait = WebDriverWait(driver, 5)

        wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'a5H0ec')))
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        about_tab_present = driver.find_elements(By.CSS_SELECTOR, "button[role='tab'][aria-label*='About']")
        about_status = "Yes" if about_tab_present else "No"

        try:
            name_element = soup.find('h1', class_='DUwDvf lfPIob')
            name = name_element.text.strip() if name_element else None
            englishname = None
            if contains_pictographic(remove_emoji(name)):
                englishname = translate_text(remove_emoji(name))

        except Exception as e:
            pass

        try:
            hour_element = soup.find('div', class_='t39EBf GUrTXd')
            hour = hour_element['aria-label'].strip() if hour_element else None
        except:
            try: 
                check_in_element = soup.find('span', text=lambda x: x and 'Check-in time' in x)
                check_in_time = check_in_element.text.split(': ', 1)[-1].strip() if check_in_element else None

                check_out_element = soup.find('span', text=lambda x: x and 'Check-out time' in x)
                check_out_time = check_out_element.text.split(': ')[1][-1].strip() if check_out_element else None
                if check_in_time is not None and check_out_time is not None:
                    hour = f'{check_in_time} - {check_out_time}'
            except Exception as e:
                pass
        if hour: 
            hour = hour.replace(". Hide open hours for the week", '')

        try:     
            hour_close = driver.find_element(By.CLASS_NAME, 'aSftqf ')
        except:
            hour_close = None
        if hour_close is not None:
            hour = 'Closed'

        try:
            phone_element = soup.select_one('button.CsEnBe[data-tooltip="Copy phone number"]')
            phone = cleanUnicode(str(phone_element.text.strip())) if phone_element else None
            if phone:
                phone_code = phone.split(' ')[0].replace('+', '')
                phone = ''.join(phone.split(' ')[1:]).replace('-', '').strip()
            else:
                phone_code = None
        except Exception as e:
            pass
        
        try:
            price_element = soup.select_one('#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div.TIHn2 > div > div.lMbq3e > div.LBgpqf > div > div.fontBodyMedium.dmRWX > span > span > span > span:nth-child(2) > span > span')
            price = price_element.text.strip() if price_element else None
        except Exception as e:
            try:
                price_element = soup.select_one('#QA0Szd div div div:nth-of-type(1) div:nth-of-type(2) div div:nth-of-type(1) div div div:nth-of-type(2) div div:nth-of-type(1) div:nth-of-type(2) div div:nth-of-type(1) span span span span:nth-of-type(2) span span')
                price = price_element.text.strip() if price_element else None
            except:
                pass
            pass

        try:
            address_element = soup.select_one('button.CsEnBe[data-tooltip="Copy address"]')
            address = cleanUnicode(str(address_element.text.strip())) if address_element else None
        except Exception as e:
            pass
        
        try:
            star_element = soup.find('span', class_='ceNzKf')
            star_rating = star_element['aria-label'].split()[0] if star_element else None
        except Exception as e:
            pass

        try:
            reviews_element = soup.find('span', attrs={'aria-label': lambda x: x and 'reviews' in x})
            num_reviews = reviews_element['aria-label'].split()[0] if reviews_element else None
        except Exception as e:
            pass

        try:
            brand_type_element = soup.find('button', class_='DkEaL')

            brand_type = brand_type_element.text.strip() if brand_type_element else None
        except Exception as e:
            pass

        html = None
        try:
            WebDriverWait(driver, 10).until(
            EC.invisibility_of_element_located((By.CSS_SELECTOR, 'div[class="r0ZAkd "]'))
            )

            # Click on the first "Share" button
            share_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[class="g88MCb S9kvJb "][data-value="Share"]'))
            )
            actions = ActionChains(driver)
            actions.move_to_element(share_button).click().perform()

            # Click on the first "Embed a map" button
            embed_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[aria-label="Embed a map"]'))
            )
            actions = ActionChains(driver)
            actions.move_to_element(embed_button).click().perform()

            # Get the value of the input field with class "yA7sBe"
            html_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'input[class="yA7sBe"]'))
            )
            html = html_input.get_attribute('value')
            html = convert_emebed(html)

            close_button = driver.find_element(By.CSS_SELECTOR, 'button[aria-label="Close"]')
            close_button.click()
        except Exception as e:
            pass

        longlat = parselongGoolonggleLongLat(row['map_url'])

        try:
            num_image_element = driver.execute_script("return document.querySelector('button div.fontBodyMedium.YkuOqf');")
            
            num_image_text = driver.execute_script("return arguments[0].innerText;", num_image_element)
            # num_image_text
            
            match = re.search(r'\d+', num_image_text)
            
            if match:
                # num_image = int(match.group())
                num_image = int(num_image_text.split(' ')[0].strip().replace(',', ''))
            else:

                num_image = 0
        except:
            num_image = 0

        result_entry =  {
            "code": row['code'],
            "orignalname": name, 
            "englishname": englishname,
            "fulladdress": address,
            "price_levels": price,
            "phone": phone,
            "phone_code": phone_code,
            "rating": star_rating,
            "num_reviews": num_reviews,
            'ordinatex': longlat.get('latitude'),
            'ordinatey': longlat.get('longitude'),
            'emble': html,
            'map_url': row['map_url'],
            "hour": hour,
            "brand_type":brand_type,
            'about_status': about_status,
            'num_image': num_image,
            'place_id':row['place_id']
        }

        if result_entry['emble'] is not None and result_entry['orignalname'] is not None and result_entry['fulladdress'] is not None and result_entry['rating'] is not None and result_entry['num_reviews'] is not None and result_entry['num_image'] > 5 and result_entry['hour'] != 'Closed':
            writeMainInfo(result_entry, self.success_path)
            self.aff_link(driver, row)
        else:
            writeMainInfo(result_entry, self.failed_path)

    def process_row(self, row):
        driver = create_chrome_driver()
        try:
            self.parse(driver, row)
        except Exception as e:
            print(e)
            pass
            driver.quit()
        finally:
            driver.quit()

    def crawl(self):
        print(f"Processing {len(self.processing_df)} URLs")

        with Pool(processes = self.num_drivers) as pool:
            for _ in tqdm(pool.imap_unordered(self.process_row, [row for _, row in self.processing_df.iterrows()]), total=len(self.processing_df), desc="Processing URLs"):
                pass

if __name__ == '__main__':
    start_time = time.time()
    source_path = r"\\Admin-pc\data\2024\11_Nov\Input\22-10-24_map_url_api_(processed).csv"
    base_dir = r"D:\DATA\2024\11_Nov\Output"
    file_prefix = "22_10_main_hour"
    success_path = os.path.join(base_dir, f"{file_prefix}_(success).csv")
    failed_path = os.path.join(base_dir, f"{file_prefix}_(missing).csv")

    success_path_aff = os.path.join(base_dir, f"{file_prefix}_(aff).csv")

    output_header = "code,orignalname,englishname,fulladdress,price_levels,phone,phone_code,rating,num_reviews,ordinatex,ordinatey,emble,map_url,hour,brand_type,about_status,num_image,place_id\n"

    crawler = MainCrawler(source_path, success_path, failed_path, success_path_aff, num_drivers = 60, header=output_header)
    crawler.crawl()
    logging.info(f'Completed crawling MainExtract with {round((time.time() - start_time) / 3600,2)} hours.')
