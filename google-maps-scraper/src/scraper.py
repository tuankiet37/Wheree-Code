import traceback
from botasaurus import cl, bt
from botasaurus.cache import DontCache
from extract_data import extract_data, perform_extract_possible_map_link
from scraper_utils import create_search_link, perform_visit
from utils import unique_strings, writeMainInfo, prepare_file, collectRecordsTobeChecked, writeFile
from reviews_scraper import GoogleMapsAPIScraper
from time import sleep, time
from botasaurus.browser import Driver, browser, AsyncQueueResult, Wait, DetachedElementException
from botasaurus.request import request
import pandas as pd
from tqdm import tqdm   
import os
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor


def is_errors_instance(instances, error):
    for i in range(len(instances)):
        ins = instances[i]
        if isinstance(error, ins):
            return True, i
    return False, -1


def istuple(el):
    return type(el) is tuple

def retry_if_is_error(func, instances=None, retries=3, wait_time=None, raise_exception=True, on_failed_after_retry_exhausted=None):
    tries = 0
    errors_only_instances = list(
        map(lambda el: el[0] if istuple(el) else el, instances))

    while tries < retries:
        tries += 1
        try:
            created_result = func()
            return created_result
        except Exception as e:
            is_valid_error, index = is_errors_instance(
                errors_only_instances, e)

            if not is_valid_error:
                raise e
            if raise_exception:
                traceback.print_exc()

            if istuple(instances[index]):
                instances[index][1]()

            if tries == retries:
                if on_failed_after_retry_exhausted is not None:
                    on_failed_after_retry_exhausted(e)
                if raise_exception:
                    raise e

            print('Retrying')

            if wait_time is not None:
                sleep(wait_time)

def process_reviews(reviews):
    processed_reviews = []

    for review in reviews:
        # Convert user_photos and user_reviews to integers, handling None and commas
        user_photos = review.get("user_photos")
        number_of_photos_by_reviewer = user_photos
        # int(user_photos.replace(",", "").replace(".", "")) if user_photos else 0

        user_reviews = review.get("user_reviews")
        number_of_reviews_by_reviewer = user_reviews
        # int(user_reviews.replace(",", "").replace(".", "")) if user_reviews else 0

        lk = review.get("likes")
        processed_review = {
            # "name": review.get("user_name"),
            # "reviewer_profile": review.get("user_url"),            
            "review_id": review.get("review_id"),
            "rating": int(review.get("rating")),
            "review_text": review.get("text"),
            "published_at": review.get("relative_date"),
            "published_at_date": review.get("text_date"),
            "response_from_owner_text": review.get("response_text"),
            "response_from_owner_ago": review.get("response_relative_date"),
            "response_from_owner_date": review.get("response_text_date"),
            "review_likes_count": 0 if lk <= -1 else lk,
            "total_number_of_reviews_by_reviewer": number_of_reviews_by_reviewer,
            "total_number_of_photos_by_reviewer": number_of_photos_by_reviewer,
            "is_local_guide": review.get("user_is_local_guide"),
            "review_translated_text": review.get("translated_text"),
            "response_from_owner_translated_text": review.get("translated_response_text"),
            # "extracted_at": review.get("retrieval_date")
        }
        processed_reviews.append(processed_review)

    return processed_reviews


@request(

    close_on_crash=True,
    output=None,
    parallel=40,
)
def scrape_reviews(requests, data):
    place_id = data["place_id"]
    link = data["link"]

    max_r = data["max"]

    reviews_sort = data["reviews_sort"]
    lang = data["lang"]
    
    processed = []
    with GoogleMapsAPIScraper() as scraper:

        result = scraper.scrape_reviews(
            link,  max_r, lang, sort_by=reviews_sort
        )
        processed = process_reviews(result, )
    
    return {"place_id":place_id, "reviews": processed}


class RetryException(Exception):
    pass
@request(
    parallel=50,
    async_queue=True,

    close_on_crash=True,
    output=None,

    # TODO: IMPLEMENT AND UNCOMMENT
    max_retry=0,
    retry_wait=0,
    # request_interval=0.2, {ADD}
)
def scrape_place(requests, link, metadata):
        cookies = metadata["cookies"]
        os = metadata["os"]
        user_agent = metadata["user_agent"]
        try:
            html =  requests.get(link,cookies=cookies, 
                                 browser='chrome',
                                 os=os, user_agent=user_agent, timeout=12,).text
            try:
                # Splitting HTML to get the part after 'window.APP_INITIALIZATION_STATE='
                initialization_state_part = html.split(';window.APP_INITIALIZATION_STATE=')[1]

                # Further splitting to isolate the APP_INITIALIZATION_STATE content
                app_initialization_state = initialization_state_part.split(';window.APP_FLAGS')[0]
            except:
                raise RetryException("Retrying...")
            

            # Extracting data from the APP_INITIALIZATION_STATE
            data = extract_data(app_initialization_state, link)
            # data['link'] = link

            data['is_spending_on_ads'] = False
            cleaned = data
            
            return cleaned  
        except Exception as e:
            raise

def extract_possible_map_link(html):
        try:
            # Splitting HTML to get the part after 'window.APP_INITIALIZATION_STATE='
            initialization_state_part = html.split(';window.APP_INITIALIZATION_STATE=')[1]

            # Further splitting to isolate the APP_INITIALIZATION_STATE content
            app_initialization_state = initialization_state_part.split(';window.APP_FLAGS')[0]
            # Extracting data from the APP_INITIALIZATION_STATE
            link = perform_extract_possible_map_link(app_initialization_state,)
            # print(link)
            if link and cl.extract_path_from_link(link).startswith("/maps/place"):
                return link
        except:
            return None

def merge_sponsored_links(places, sponsored_links):
    for place in places:
        place['is_spending_on_ads'] = place['link'] in sponsored_links

    return places

def get_lang(data):
     return data['lang']

class StuckInGmapsException(Exception):
    pass



@browser(
    lang=get_lang,
    close_on_crash=True,
    max_retry = 0,
    reuse_driver=True,
    headless=True,
    output=None,
)
def scrape_places(driver:Driver, data):
    # This fixes consent Issues in Countries like Spain 
    max_results = data['max']

    scrape_place_obj: AsyncQueueResult = scrape_place()

    sponsored_links = None
    def get_sponsored_links():
         nonlocal sponsored_links
         if sponsored_links is None:
              sponsored_links = driver.run_js('''function get_sponsored_links() {
  try {

    // Get all elements with the "Sponsored" text in the h1 tag.
    const sponsoredLinks = [...document.querySelectorAll('.kpih0e.f8ia3c.uvopNe')]
    
    // Extract the parent <div> elements of the sponsored links.
    const sponsoredDivs = sponsoredLinks.map(link => link.closest('.Nv2PK'));
    
    // Extract the links (href) from the sponsored <a> tags.
    const sponsoredLinksList = sponsoredDivs.map(div => div.querySelector('a').href);

    return sponsoredLinksList    
  } catch (error) {
    return []
  }
}

return get_sponsored_links()''')
         return sponsored_links



    def put_links():
                start_time = time()
                
                WAIT_TIME = 40 # WAIT 40 SECONDS

                metad = {"cookies":driver.get_cookies_dict(), "os": bt.get_os(), "user_agent" : driver.user_agent}
                if data['links']:
                  scrape_place_obj.put(data['links'], metadata = metad)
                  return
                while True:
                    el = driver.select(
                        '[role="feed"]', Wait.SHORT)
                    if el is None:
                        if driver.is_in_page("/maps/search/"):
                            link = extract_possible_map_link(driver.page_html)
                            if link:
                                rst = [link]
                                scrape_place_obj.put(rst, metadata = metad)
                            rst = []
                        elif driver.is_in_page("/maps/place/"):
                            rst = [driver.current_url]
                            scrape_place_obj.put(rst, metadata = metad)
                        return
                    else:
                        el.scroll_to_bottom()

                        links = None
                        
                        if max_results is None:
                            links = driver.get_all_links(
                                '[role="feed"] >  div > div > a', wait=Wait.SHORT)
                        else:
                            links = unique_strings(driver.get_all_links(
                                '[role="feed"] >  div > div > a', wait=Wait.SHORT))[:max_results]
                                                    
                        
                            
                        scrape_place_obj.put(links, metadata = metad)

                        if max_results is not None and len(links) >= max_results:
                            return

                        # TODO: If Proxy is Given Wait for None, and only use wait to Make it Faster, Example Code 
                        # end_el_wait = bt.Wait.SHORT if driver.config.is_retry else None

                        end_el_wait = Wait.SHORT
                        end_el = driver.select(
                            "p.fontBodyMedium > span > span", end_el_wait)

                        if end_el is not None:
                            return
                        elapsed_time = time() - start_time

                        if elapsed_time > WAIT_TIME :
                            print('Google Maps was stuck in scrolling. Retrying after a minute.')
                            sleep(63)
                            raise StuckInGmapsException()                           
                            # we increased speed so occurence if higher than 
                            #   - add random waits
                            #   - 3 retries  
                             
                        if driver.can_scroll_further('[role="feed"]'):
                            start_time = time()
                        else:
                            sleep_time = 0.1
                            sleep(sleep_time)
    
    search_link = create_search_link(data['query'], data['lang'], data['geo_coordinates'], data['zoom'])
    
    perform_visit(driver, search_link)

    if driver.is_in_page('/sorry/'):
        raise Exception("Detected by Google, Retrying ")
    
    STALE_RETRIES = 5
    # TODO
    # I need to ask to restart browser 
    # use proxy addition
    failed_to_scroll = False
    def on_failed_after_retry_exhausted(e):
        nonlocal failed_to_scroll
        failed_to_scroll = True
        print('Failed to scroll after 5 retries. Skipping.')

#         print('''Google has silently blocked IP. Kindly follow these Steps to change IP.
# # If using Wifi:
# #     - Turn Router off and on 
# # If using Mobile Data
# #     - Connect your PC to the Internet via a Mobile Hotspot.
# #     - Toggle airplane mode off and on on your mobile device. This will assign you a new IP address.
# #     - Turn the hotspot back on.                      
# # ''')
    try:
      retry_if_is_error(put_links, [DetachedElementException], STALE_RETRIES, raise_exception=False
                    #   , on_failed_after_retry_exhausted=on_failed_after_retry_exhausted
                      )
    # todo remove check later      
      if driver.config.is_retry:
          print("Successfully scrolled to the end.")
    
    except StuckInGmapsException as e:
      if driver.config.is_last_retry:
          on_failed_after_retry_exhausted(e)
      else:
          raise e
    

    places = scrape_place_obj.get()

    hasnone = False
    for place in places:
      if place is None:
        hasnone = True
        break
    
    places = bt.remove_nones(places)

    for p in places:
        p['query'] = data['query']
    
    sponsored_links = [] if data['links'] else get_sponsored_links() 
    places = merge_sponsored_links(places, sponsored_links)
    

    result = {"query": data['query'], "places": places}
    
    if failed_to_scroll:
        return DontCache(result)

    if hasnone:
        return DontCache(result)

    return result 

def process_text(text):
    try:
        return text.replace('\n', ' ')
    except:
        return text

def scrape_brand(row):

    data = scrape_places({'query': row['query'], 'max': None, 'lang': None, 'geo_coordinates': '', 'zoom': 16, 'links': []})

    if data['places'] != []:
        results = []
        for i in range(len(data['places'])):
            place = data['places'][i]
            orignalname = place['name']
            fulladdress = place['address']
            rating = place.get('rating', None)  
            map_url = place['link']
            num_reviews = place.get('reviews', None)  
            category = place['main_category']
            categories = place.get('categories', []) or []
            subcate1 = categories[0] if len(categories) > 0 else None
            subcate2 = categories[1] if len(categories) > 1 else None
            subcate3 = categories[2] if len(categories) > 2 else None

            result_entry = {
                'query': row['query'],
                'place_id': place.get('place_id'),
                'orignalname': orignalname,
                'rating': rating,
                'num': num_reviews,
                'fulladdress': fulladdress,
                'map_url': map_url,
                'category': category,
                'subcate1': subcate1,
                'subcate2': subcate2,
                'subcate3': subcate3,
            }
            if num_reviews >10 and rating >= 3.8 and fulladdress !='' and orignalname !='':
                results.append(result_entry)

                if place['featured_reviews'] != []:
                    featured_reviews = place.get('featured_reviews', [])
                    results_rev = []
                    for j in range(len(featured_reviews)):
                        review_org = featured_reviews[j]['review_text']
                        reviews_translated =  featured_reviews[j]['review_translated_text']
                        review_org = process_text(review_org)
                        reviews_translated = process_text(reviews_translated)

                        result_entry = {
                            # 'code': row['code'],
                            'map_url': map_url,
                            'review_id': featured_reviews[j]['review_id'],
                            'published_at_date' : featured_reviews[j]['published_at_date'],
                            'reviews_original': review_org,
                            'reviews_translated': reviews_translated,
                            'rating': featured_reviews[j]['rating'],
                            'type': 'REVIEW'
                        }
                        if review_org !='':
                            results_rev.append(result_entry)

                    if results_rev:
                        writeFile(results_rev, success_path_rev)
        if results:
            writeFile(results, success_path)
    else:
        result_entry ={
            'query': row['query'],
        }
        writeMainInfo(result_entry, failed_path)

if __name__ == "__main__":

    source_path = r"D:\DATA\cat_city_country\queries_uk.csv"
    base_dir = r"D:\DATA\cat_cit_country_result"
    file_prefix = "map_url_api_"
    success_path = os.path.join(base_dir, f"{file_prefix}(success).csv")
    failed_path = os.path.join(base_dir, f"{file_prefix}(missing).csv")

    # header = 'code,top_rated_url,place_id,orignalname,rating,num,fulladdress,map_url,category,subcate1,subcate2,subcate3\n'
    header = 'query,place_id,orignalname,rating,num,fulladdress,map_url,category,subcate1,subcate2,subcate3\n'

    file_prefix_rev = "reviews_api_"
    success_path_rev = os.path.join(base_dir, f"{file_prefix_rev}(success).csv")
    # failed_path_rev = os.path.join(base_dir, f"{file_prefix_rev}(missing).csv")
    header_rev = 'map_url,review_id,published_at_date,reviews_original,reviews_translated,rating,type\n'
    prepare_file(success_path_rev, header_rev)

    df = collectRecordsTobeChecked(success_path, failed_path, source_path, header)
    # df = pd.read_csv(source_path)

    start = time()
    with ProcessPoolExecutor(max_workers=61) as executor:
        futures = [executor.submit(scrape_brand, row) for _, row in df.iterrows()]
        for future in tqdm (as_completed(futures), total=len(futures), desc="Processing URLs"):
            future.result()
    print(f"Finished in {round((time() - start)/3600,2)} hours")
    