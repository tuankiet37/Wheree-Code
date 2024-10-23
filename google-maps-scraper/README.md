# Google Map Scraper
This folder contains the scripts using 🤖 Botasaurus 🤖 to search for places by query (e.g., `Hotels in Ho Chi Minh City`) and also the scripts to crawl reviews using the Google API.

- There are two main functions:
  - [`scraper.py`](#scraperpy): Scrapes places based on a query.
  - [`reviews_scraper.py`](#reviews_scraperpy): Uses the Google API endpoint to extract reviews.

## scraper.py
This script is designed to scrape place data from Google Maps based on a search query.

The `scrape_places()` function is used to scrape Google Maps places based on specific criteria. Below is a breakdown of the parameters that can be passed to the function.

### Parameters

- `query` (str): **Required**  
  The search term to look for places on Google Maps. This could be a place type, such as `Hotels in Ho Chi Minh City`, or a specific business name.

- `max` (int or None): **Optional**  
  The maximum number of results to scrape. If set to `None`, it will scrape all available results.

- `lang` (str or None): **Optional**  
  The language code for scraping, such as `en` for English or `vi` for Vietnamese. If `None`, the default language will be `en`.

- `geo_coordinates` (str): **Optional**  
  The geographical coordinates (latitude and longitude) to center the search around. This should be in the format `latitude,longitude`. If left empty (`''`), the query will rely on the general location specified in the search.

- `zoom` (int): **Optional**  
  The zoom level for the map when performing the search. The higher the zoom level, the more localized the search will be. Default is `16`, which represents a close-up city view.

- `links` (list): **Optional**  
  A list of URLs or place IDs to scrape specific places. If left empty (`[]`), the script will scrape based on the query.


## reviews_scraper.py
This script accesses Google’s API to extract reviews for places listed on Google Maps. It retrieves the reviews and related data, which can be further analyzed or stored for reference.

### Parameters
- `n_reviews` (int):  
  The number of reviews to scrape. In this case, it is hard-coded to `100` reviews.

- `hl` (str):  
  The language in which to scrape reviews. Currently set to `en` (English).

- `sort_by` (str):  
  Defines how to sort reviews. Set to `qualityScore`, which likely prioritizes high-quality or relevant reviews.
    - `"most_relevant"` : `"qualityScore"`: Sorts reviews by relevance, showing the most relevant reviews first. Reviews are ranked by factors like helpfulness and user interaction.
  
  - `"newest"` : `"newestFirst"`: Sorts reviews by the most recent first. This option is useful to get the latest feedback.
  
  - `"highest_rating"`: `"ratingHigh"`: Sorts reviews by the highest ratings first (e.g., 5-star reviews). It helps highlight the most positive reviews.
  
  - `"lowest_rating"` : `"ratingLow"`: Sorts reviews by the lowest ratings first (e.g., 1-star reviews). This helps identify negative feedback and potential issues.



  
  







