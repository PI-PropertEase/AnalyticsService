import pandas as pd
import time
import math
import serpapi

API_KEY = "b0d7dc66d35955a0b73cbd71e9cb0441851d216c3c2fb4c5af3a31a8000ce9f1"

def is_bad_trend(location):
    search = serpapi.search({
        "engine": "google_trends",
        "q": location,
        "data_type": "RELATED_TOPICS",
        "api_key": API_KEY,
        "date": "now 1-d",
    })

    results = search.as_dict()
    topics = []

    if "rising" in results["related_topics"]:
        topics = [f"{topic['topic']['title']} {topic['topic']['type']}" for topic in results["related_topics"]["rising"]]
    if "top" in results["related_topics"]:    
        topics = topics + [f"{topic['topic']['title']} {topic['topic']['type']}" for topic in results["related_topics"]["top"]]
    
    if topics == []:
        return False
    
    df = pd.DataFrame(topics, columns=['topics'])

    strings_to_check = ['disaster', 'tragedy', r'flood(?!plain)', 'murder', 'invasion', 'homicide', 'assassination',
                    r'fire(?!work|cracker|proof|fighter|baller|place|board|brick|break|guard|stone|house|wall|\sstation)',
                    'storm', 'hurricane', 'cyclone', 'tornado', 'earthquake', 'weapon', 'missile',
                    'shooting', 'terrorism', r'bomb(?!astic)', r'^killing', 'kidnapping', 'genocide', r'^war$']
    
    if df['topics'].str.contains('|'.join(strings_to_check), case=False).any():
        return True
    else:
        return False


def calculate_price_by_trends(location, prices_by_location_df, recommended_prices_by_model):

    if is_bad_trend(location):
        print("\n" + location + " -> Bad trend!\n")
        return prices_by_location_df.set_index('id')['price'].to_dict()
    
    search = serpapi.search({
        "engine": "google_trends",
        "q": location,
        "data_type": "TIMESERIES",
        "api_key": API_KEY,
        "date": "today 1-m"
    })

    results = search.as_dict()
    interest_over_time = results["interest_over_time"]["timeline_data"]
    data = [{'value': value_entry['extracted_value']} for entry in interest_over_time for value_entry in entry['values']]
    df_trends_yesterday_today = pd.DataFrame(data).tail(2)
    
    df_pct_changes = df_trends_yesterday_today.pct_change()
    trend_pct_change = df_pct_changes.iloc[-1]['value']

    if trend_pct_change > 0.1:
        price_change = math.log10( (trend_pct_change + 0.3) / 4 ) + 1
    elif trend_pct_change < -0.1:
        price_change = -1 * math.log10( ((-1*trend_pct_change) + 0.3) / 4 ) - 1
    else:
        price_change = 0

    recommended_prices = {}

    for _, property in prices_by_location_df.iterrows():
        new_price = property["price"] * (1 + price_change)
        if new_price >= recommended_prices_by_model[property["id"]]:
            recommended_prices[property["id"]] = property["price"] * (1 + price_change)
        else:
            recommended_prices[property["id"]] = property["price"]

    return recommended_prices


def recommend_prices_by_trends(real_prices_df, recommended_prices_by_model):
    
    recommended_prices_trends = {}
    locations = real_prices_df["location"].unique()

    for location in locations:
        real_prices_by_location_df = real_prices_df[real_prices_df["location"] == location]
        recommended_prices_by_location = calculate_price_by_trends(location, real_prices_by_location_df, recommended_prices_by_model)
        recommended_prices_trends = recommended_prices_trends | recommended_prices_by_location

    return recommended_prices_trends