import pandas as pd
import re
from deep_translator import GoogleTranslator
import unicodedata

def remove_emoji(text):
    # Emoji range covers various Unicode blocks that include emojis
    emoji_pattern = re.compile(
        "[\U0001F600-\U0001F64F"  # Emoticons
        "\U0001F300-\U0001F5FF"  # Misc Symbols and Pictographs
        "\U0001F680-\U0001F6FF"  # Transport and Map
        "\U0001F700-\U0001F77F"  # Alchemical Symbols
        "\U0001F900-\U0001F9FF"
        # "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
        # "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
        # "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        # "\U0001FA00-\U0001FA6F"  # Chess Symbols
        # "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        # "\U00002702-\U000027B0"  # Dingbats
        # "\U000024C2-\U0001F251"  # Enclosed characters
        "]+", flags=re.UNICODE)
    
    text = text.replace('®', '')
    text = text.replace('°', '')
    text = text.replace('™', '')
    text = text.replace('№', 'No')

    symbols_to_remove = [
    '⭐', '★', '☆', '✨', '🌟', '🌠', '🌌', '🌆', '🌇', '🌃', '🌉', '🌁', 
    '🌄', '🌅', '🌈', '🌊', '🌋', '🌍', '🌎', '🌏', '🌐', '🌑', '🌒', 
    '🌓', '🌔', '🌕', '🌖', '🌗', '🌘', '🌙', '🌚', '🌛', '🌜', '🌝', 
    '🌞', '🌡', '🌤', '⚓️', '☀️', '✨', 'º' , 'ª','🥇','⭐️'
    ]
    for symbol in symbols_to_remove:
        text = text.replace(symbol, '')

    # Remove emojis using the pattern
    result = emoji_pattern.sub(r'', text)
    result = re.sub(r'\s{2,}', ' ', result).strip()
    return result

def rename_columns(main_df):
    main_df.drop(columns=['hour'], inplace=True)
    main_df.drop(columns=['brand_type'], inplace=True)
    main_df.rename(columns={'address': 'fulladdress', 
                            'brand_name':'orignalname', 
                            'price':'price_levels', 
                            'embed_url':'emble', 
                            'star':'rating', 
                            'num':'num_reviews', 
                            'latitude':'ordinatex', 
                            'longitude':'ordinatey',
                            'gg_url': 'map_url'}, 
                            inplace=True)
    main_df['yelp_link'] = ''
    return main_df

def contains_pictographic(text):
    text = remove_emoji(text)
    
    for char in text:
        category = unicodedata.category(char)
        
        if category.startswith('So') or category.startswith('Lo'):
            return True
    return False

def remove_duplicate_words(text):
    # Define a regex pattern to remove special characters
    pattern = r'[()]'

    # Substitute special characters with an empty string
    cleaned_text = re.sub(pattern, '', text)

    # Initialize a set to track seen words and a list to store the result
    seen = set()
    result = []

    # Iterate through each word in the cleaned text
    for word in cleaned_text.split():
        if word not in seen:  # If the word has not been encountered before
            seen.add(word)    # Add it to the seen set
            result.append(word)  # Append it to the result list

    return ' '.join(result)  # Return the result as a string with words joined by spaces

def clear_pictographic(text):
    pictographic_chars = []
    for char in text:
        category = unicodedata.category(char)

        # Check for pictographic characters (Symbols, Letters that aren't Latin)
        if category.startswith('So') or category.startswith('Lo'):
            pictographic_chars.append(char)


    # Join the collected characters into strings
    pictographic = ''.join(pictographic_chars)

    english = text.replace(pictographic, '')
    english = re.sub(r'\s{2,}', ' ', english).strip()

    return english

def translate_text(text, dest_lang='en'):
    translator = GoogleTranslator(source='auto', target=dest_lang)
    translated_text = translator.translate(text)
    return remove_duplicate_words(clear_pictographic(translated_text))

def filter_img(df, min_gen=6, min_menu=6):
    # Map 'type' column values efficiently
    df['type'] = df['type'].map({
        'banner': 'MAIN',
        'general_image': 'IMAGE',
        'menu_image': 'MENU'
    }).fillna(df['type'])  # Fills in unchanged types

    # Filter only relevant types early on
    df = df[df['type'].isin(['MAIN', 'IMAGE', 'MENU'])].copy()

    print('Before', df['code'].nunique())
    print(f'Image types: {df["type"].unique()}')

    # Group by 'code' and collect summary statistics
    grouped = df.groupby('code').agg(
        main_present=('type', lambda x: 'MAIN' in x.values),
        image_count=('type', lambda x: (x == 'IMAGE').sum()),
        menu_count=('type', lambda x: (x == 'MENU').sum()),
        first_image_idx=('type', lambda x: x.eq('IMAGE').idxmax() if 'IMAGE' in x.values else None)
    )

    # Identify codes that do not have 'MAIN' but have an 'IMAGE'
    no_main_but_image = grouped[
        (~grouped['main_present']) & 
        grouped['first_image_idx'].notna()
    ]

    # For those, rename the first 'IMAGE' to 'MAIN' and decrement image_count
    image_indices = no_main_but_image['first_image_idx'].values
    df.loc[image_indices, 'type'] = 'MAIN'

    # Update the grouped dataframe by decrementing image_count by 1 for these codes
    grouped.loc[no_main_but_image.index, 'image_count'] -= 1

    # Filter codes based on image count only
    valid_codes = grouped[
        (grouped['image_count'] >= min_gen)
    ].index

    # Remove all 'MENU' rows where 'menu_count' is less than 'min_menu'
    insufficient_menu_codes = grouped[
        grouped['menu_count'] < min_menu
    ].index

    # Delete all 'MENU' types for these codes
    df = df[~((df['code'].isin(insufficient_menu_codes)) & (df['type'] == 'MENU'))]

    # Recalculate valid codes based on the updated image_count
    valid_codes = grouped[
        (grouped['image_count'] >= min_gen)
    ].index

    # Filter the DataFrame to include only valid codes based on image count
    filtered_df = df[df['code'].isin(valid_codes)].copy()

    print('Filter', filtered_df['code'].nunique())
    return filtered_df

def check_img(img_df):

    print("Number of types:", img_df['type'].nunique())

    average_types_per_code = img_df.groupby('code')['type'].count().mean()
    print("Average number of types per code:", round(average_types_per_code))

    code_with = img_df.groupby('code')['type'].nunique().eq(img_df['type'].nunique()).sum()
    print(f"Number of codes with exactly {img_df['type'].nunique()} types:", code_with)

    for img_type in img_df['type'].unique():
        average_type = img_df[img_df['type'] == img_type].groupby('code')['type'].count().mean()
        print(f"Average number of {img_type} per code:", round(average_type))

def check_review(df):
    # Calculate the number of reviews for each code
    if 'review' in df.columns:
        reviews_per_code = df.groupby('code')['review'].count()
    elif 'content' in df.columns:
        reviews_per_code = df.groupby('code')['content'].count()
    elif 'reviews' in df.columns:
        reviews_per_code = df.groupby('code')['reviews'].count()

    # Calculate average, max, and min reviews per code
    avg_reviews_per_brand = reviews_per_code.mean()
    max_reviews_per_brand = reviews_per_code.max()
    min_reviews_per_brand = reviews_per_code.min()

    # Remove NaN values before calculating the average
    valid_reviews = reviews_per_code.dropna()
    avg_reviews_per_brand = valid_reviews.mean()

    # Print the results
    print("Average number of reviews per link:", round(avg_reviews_per_brand))
    print("Maximum number of reviews per link:", max_reviews_per_brand)
    print("Minimum number of reviews per link:", min_reviews_per_brand)

def convert_emebed(url):
  match = re.search(r'src="([^"]+)"', url)

  src_link = match.group(1)
  return src_link





