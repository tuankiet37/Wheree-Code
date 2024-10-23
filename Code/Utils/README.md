# Utils Function

This repository contains utility functions that are commonly used across various projects. These functions are designed to simplify and streamline common tasks.
There are four main files:
- [`condition_utils.py`](#condition_utilspy): This file contains utility functions for performing conditional checks on **images** and **reviews**, as well as translating the original names of brands that contain pictographic characters.
- [`token_utils.py`](#token_utilspy): This file is used to tokenize, remove stop words, and remove punctuation using NLTK to make the **reviews** shorter for generating **content**.
- [`Utils.py`](#utilspy): This file is used for generating a driver using **Selenium** and writing files.
- [`config.ini`](#configini): This is a settings file for configuring the driver used in `Utils.py`.
- [`Touch VPN.crx`](#touch-vpncrx): This file is a Chrome extension file for Touch VPN.

## condition_utils.py
Four main functions:
- **`contains_pictographic`**: Check the original name contains pictographic or not
- **`translate_text`**: Used to translate pictographic characters.
- **`filter_img`**: Returns a DataFrame that satisfies the condition of having more than 6 `IMAGE` and 1 `MAIN` type. If there is a tab menu, it must have at least 6 `MENU`.
- **`check_img`**: Checks the average number of images per code and per type.
- **`check_review`**: Checks the number of reviews per code, including minimum and maximum values.
- **`convert_emebed`**: Convert the raw emebed 

``` python
from Code.Utils import translate_text, filter_img, check_img, check_review, convert_emebed

# Function translate
df['check'] = df['orignalname'].apply(contains_pictographic)

df['englishname'] = df.apply(lambda row: translate_text(row['orignalname']) if row['check'] else row['orignalname'], axis=1)

#Convert emebed
df['emble'] = df['map_url'].apply(convert_emebed)

# Filter and check the number of image
img_df = filter_img(df=img_df, min_gen=6, min_menu=6) # Config min_gen and min_menu for the min of IMAGE and min MENU type for each code

check_img(img_df)

# Check reviews
check_review(reviews_df)
```

## token_utils.py
Two main functions:
- **`preprocess_text`**: Splits text into individual words, removes common English stop words but keeps negation words (like 'not', 'no', etc.).
- **`normalize_text`**: Used to lower all the character and convert it to ASCII, which is called in `preprocess_text`.

``` python
from Code.Utils.token_utils import preprocess_text

df['content'] = df['content'].apply(preprocess_text)
```

## Utils.py
- **`create_chrome_driver`, `create_firefox_driver`**: Initialize and return Selenium WebDriver instances for `Chrome` or `Firefox`.
- **`create_driver_extension`**: Initialize and return Selenium WebDriver for `Chrome` with `Touch VPN extension`.
- **`collectRecordsTobeChecked`**: Reads data from `source_path`, and optionally from `success_path` and `failed_path`, to filter out records that have already been processed.
- **`writeMainInfo`, `writeFile`**: Save results, either in a dictionary format (`writeMainInfo`) or a list of dictionaries (`writeFile`).

``` python
from Code.Utils import create_chrome_driver, create_firefox_driver, create_driver_extension, collectRecordsTobeChecked, writeMainInfo, writeFile

# Create driver
driver_chrome = create_chrome_driver()
driver_firefox = create_firefox_driver()
driver_chrome_VPN = create_driver_extension()

# Check the records to process
source_path = 'path-to-source-csv-file'
base_dir = 'path-to-folder-save-file'

file_prefix = "img"
success_path = os.path.join(base_dir, f"{file_prefix}_(success).csv")
failed_path = os.path.join(base_dir, f"{file_prefix}_(missing).csv")

header = "code,image,type\n"

processing_df = collectRecordsTobeChecked(success_path, failed_path, source_path, header)

# Write file function
result_entry = {
            "code": row['code'],
            "image": link,
            "type": 'MAIN' if image_type == 'IMAGE' else image_type,
        }
writeMainInfo(result, success_path)

result = []
for link in images[max_images]:
    result.append({"code": row['code'], "image": link, "type": image_type})
writeFile(result, success_path)
```

