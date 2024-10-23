import requests
import re
import logging
import os
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from tqdm import tqdm
import time
import datetime
from Utils import writeMainInfo, collectRecordsTobeChecked

class FAQ:
    def __init__(self, source_path, success_path, failed_path, header, num_drivers):
        self.success_path = success_path
        self.failed_path = failed_path
        self.source_path = source_path
        self.num_drivers = num_drivers
        self.df = collectRecordsTobeChecked(success_path, failed_path, source_path, header)
        self.converted_urls = self.convert_url()
        self.initialize_logging()

    def initialize_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def convert_url(self):
        output_data = []
        pattern = r'!1s(.*?)!'
        for _, row in self.df.iterrows():
            code = row['code']
            map_url = str(row['map_url'])
            match = re.search(pattern, map_url)
            if match:
                result = match.group(1)
                faq_url = f'https://www.google.com/local/place/qa/question/{result}'
                output_data.append({'code': code, 'FAQ_url': faq_url, 'map_url': map_url})
        
        logging.info(f"Converted URLs: {len(output_data)}")
        return output_data

    def crawFAQ(self, url):
        data_result = []
        pattern = r'\(Translated by Google\) (.*?)\(Original\)'
        try:
            page = requests.get(url)
            page.raise_for_status()
            soup = BeautifulSoup(page.content, "html.parser")
            question_elements = soup.find_all('div', class_='u1aFWb')
            for element in question_elements:
                match = re.search(pattern, element.text)
                if match:
                    cleaned_text = match.group(1).replace('"', '').replace("'", '').replace(',', '')
                else:
                    cleaned_text = element.text.replace('"', '').replace("'", '').replace(',', '')
                data_result.append(cleaned_text)
        except Exception as e:
            logging.error(f"Error fetching FAQ from {url}: {e}")
        return data_result

    def process_url(self, entry):
        code = entry['code']
        url = entry['FAQ_url']
        map_url = entry['map_url']
        try:
            page = requests.get(url)
            page.raise_for_status()
            soup = BeautifulSoup(page.content, "html.parser")
            question_elements = soup.find_all('div', class_='EkyQO')
            if question_elements:
                question_ids = [element.get("data-question-id") for element in question_elements]
                for question_id in question_ids:
                    faq_url = f'{url}/answers/{question_id}'
                    data_result = self.crawFAQ(faq_url)
                    for i in range(len(data_result) - 1):
                        data_question = {
                            'code': code,
                            'map_url': map_url,
                            'question': data_result[0],
                            'answer': data_result[i + 1],
                        }
                        writeMainInfo(data_question, file_name=self.success_path)
            else:
                writeMainInfo({'code': code, 'map_url': map_url, 'question': '', 'answer': ''}, file_name=self.failed_path)
        except Exception as e:
            logging.error(f"Error processing URL {url}: {e}")
            writeMainInfo({'code': code, 'map_url': map_url, 'question': '', 'answer': ''}, file_name=self.failed_path)

    def crawlMainTask(self):
        start_time = time.time()
        start_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with ProcessPoolExecutor(max_workers=self.num_drivers) as executor:
            futures = {executor.submit(self.process_url, entry): entry for entry in self.converted_urls}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing URLs"):
                try:
                    future.result()
                    
                except Exception as e:
                    logging.error(f"Error in future: {e}")
        end_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = round((time.time() - start_time) / 3600, 2)
        logging.info(f"Task started at: {start_date}")
        logging.info(f"Task ended at: {end_date}")
        logging.info(f"Crawling FAQ done in {duration} hours")

# Example usage
# source_path = r"D:\Wheree_Kiet\Sep\Output\Main\map_url_success_to_check.csv"
source_path = r"D:\DATA\2024\Oct\Output\Main\11_10_2024_main_hour_(success).csv"
base_dir = r"D:\DATA\2024\Oct\Output\FAQ"
os.makedirs(base_dir, exist_ok=True)
file_prefix = 'FAQ_'
success_path = os.path.join(base_dir, f"{file_prefix}(success).csv")
failed_path = os.path.join(base_dir, f"{file_prefix}(failed).csv")
output_header = 'code,map_url,question,answer\n'

crawler = FAQ(source_path, success_path, failed_path, output_header, num_drivers=100)
crawler.crawlMainTask()