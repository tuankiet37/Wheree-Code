# Main Task
This section focuses on data extraction and interaction with Google Maps, using **Selenium** for automation. Consistent input parameters across all modules ensure a seamless and efficient workflow.

- **Unified Input**:
 All files share the same input structure (`map_url`, `code`) except **`map_url_multi.py`**.

- **Modify Paths**:
  Each file requires modification of the `source_path` *\(input file\)* and `base_dir` *\(directory to store success and failed output files\)*.

# Usage

- To run the script, use the following command:

```bash
'path-to-env 'path-to-file'
```

- Examples

```bash
C:/Users/Admin/anaconda3/envs/myenv/python.exe D:/Wheree_Kiet/Code/features_op.py
```

## Main file 
- **Filter Condition**
  - Must have `orignalname`, `fulladdress`, `embed_url`, `rating`, `num_reviews`
  - Must have more than 5 `images`
  - Still opening

- **Output**: A structured dataset with the following fields:
  - `code`: Unique code identifier for each brand (UUID 4).
  - `orignalname`: The original name of the brand as displayed on Google Maps.
  - `englishname`: The English-translated name of the brand (if is contains pictographic words).
  - `fulladdress`: Complete address of the brand.
  - `price_levels`: Indicates the price level (if available).
  - `phone`: Contact phone number of the brand (if available).
  - `phone_code`: International phone code (if available).
  - `rating`: Average user rating of the brand.
  - `num_reviews`: Number of reviews.
  - `ordinatex`: The latitude of the brand.
  - `ordinatey`: The longitude of the brand.
  - `emble`: Embled URL for the map.
  - `map_url`: URL link to the Google Maps.
  - `hours`: Raw opening and closing hours.
  - `brand_type`: The type of business or brand (e.g., restaurant, café, store).
  - `about_status`: `Yes` or `No` value check for the status of `About` tab.
  - `num_image`: Number of images of this brand.

### Notes
- After running the script, navigate to `Format/convert_hour.ipynb` to process and parse the hours data.


## Image
The first image from "All Photos" is classified as **MAIN**, others as **IMAGE**, and images under the "Menu" tab are labeled **MENU**.

- **Filter Condition**: 
  - Each brand must have at least 6 images (excluding the "By Owner" tab). 
  - If a "Menu" tab exists, it must also contain at least 6 images.
- **Output**: A structured dataset with the following fields:
  - `code`: Unique code identifier for each brand (UUID 4).
  - `image`: Photo URLs.
  - `type`: Categorized into *MAIN*, *IMAGE*, or *MENU*.
  - `map_url`: URL to the Google Maps brand.

## Features
This script extracts all available data from the 'About' tab of each brand.

- **Output**: The extracted data is organized into the following structured dataset:
  - `code`: Unique code identifier for each brand (UUID 4).
  - `main_feature`: The primary feature or category. In the case of some brands such as *hotels*, this is filled by `Amenities`.
  - `feature`: Additional features or attributes of the brand.

## Reviews
This script extracts about 20-30 reviews per brand. It extracts all the data such as reviews, the number of reviews, the rating of these reviews, and the category in each review. It also clicks the `more`button for long reviews to ensure the website renders enough content for extraction.
- **Filter Condition**: Each brand must have at least 5 reviews.
- **Output**: A structured dataset with the following fields:
  - `code`: Unique code identifier for each brand (UUID 4).
  - `content`: The text of the user review.
  - `type`: Set to 'REVIEW' for all entries.

## FAQ
In the FAQ crawling, we need to convert `map_url` to `FAQ_url` by extracting the ID after `1s` in the `map_url`.
- **Output**: A structured dataset with the following fields:
  - `code`: Unique code identifier for each brand (UUID 4).
  - `question`: The FAQ question.
  - `answer`: The FAQ answer.

### Notes
- After running the script, navigate to `Format/Code_Process_FAQ.py` to process and parse the FAQ file.

## Location
This script uses [Google Maps Geocoding API](https://developers-dot-devsite-v2-prod.appspot.com/maps/documentation/utils/geocoder/). However, this website limits the number of queries per minute, so I use the Touch VPN extension in the Chrome driver for each driver and retry each query about 50 times.

We can extract query of each brand by two methods:
- [Name Fulladdress Search](#namefulladdresssearch)
- [Place ID Search](#placeidsearch)


### Name Fulladdress Search
- Access the [Google Maps Geocoding API](https://developers-dot-devsite-v2-prod.appspot.com/maps/documentation/utils/geocoder/) and send the query to search bar as `name + fulladdress`. This brand will pop up a suggestion box or will be the first result.

### Place ID Search

- Access via place ID of a place: `place_id_url = f"https://developers-dot-devsite-v2-prod.appspot.com/maps/documentation/utils/geocoder/#place_id%3D{['place_id']}"`.

### **Output**: A structured dataset with the following fields:
  - `code`: Unique code identifier for each brand (UUID 4).
  - `address`: Full address of the brand.
  - `latitude`: Latitude of the brand.
  - `longitude`: Longitude of the brand.
  - `lv0_or`: Country level.
  - `lv1_or`: First administrative area (usually state or province).
  - `lv2_or`: Second administrative area (usually city).
  - `lv3_or`: Third administrative area (smaller division within the city).
  - `lv4_or`: Fourth administrative area (smaller locality).
  - `sublocality`: Detailed locality within the city.
  - `lv1_short`: Short form of the first administrative area.


