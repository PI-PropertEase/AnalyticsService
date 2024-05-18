import pandas as pd
import time                        
import serpapi
import math

API_KEY = "b0d7dc66d35955a0b73cbd71e9cb0441851d216c3c2fb4c5af3a31a8000ce9f1"

search = serpapi.search({
  "engine": "google_trends",
  "q": "Fátima",
  "data_type": "TIMESERIES",
  "api_key": API_KEY,
  "date": "today 1-m"
})

results = search.as_dict()
interest_over_time = results["interest_over_time"]["timeline_data"]
data = [{'value': value_entry['extracted_value']} for entry in interest_over_time for value_entry in entry['values']]

# Create the DataFrame
df = pd.DataFrame(data)

print(df)

""" search = serpapi.search({
  "engine": "google_trends",
  "q": "Rio Grande do Sul",
  "data_type": "RELATED_TOPICS",
  "api_key": API_KEY,
  "date": "now 1-d"
})

results = search.as_dict()

rising_topics = [f"{topic['topic']['title']} {topic['topic']['type']}" for topic in results["related_topics"]["rising"]]
top_topics = [f"{topic['topic']['title']} {topic['topic']['type']}" for topic in results["related_topics"]["top"]]

# Combining rising and top related topics into a single list
topics = rising_topics + top_topics

# Creating the DataFrame with a single column
df = pd.DataFrame(topics, columns=['topics'])

# Display the DataFrame
print(df)

strings_to_check = ['disaster', 'tragedy', 'flood', 'murder', 'death', 'dead', 'donation',
                    'fire', 'storm', 'hurricane', 'cyclone', 'tornado', 'earthquake',
                    'shooting', 'terrorism', 'bomb', 'killed', 'kidnapping', 'genocide', 'war']

if df['topics'].str.contains('|'.join(strings_to_check), case=False).any():
    print("Bad indicator")
else:
    print("Good indicator") """

""" pytrend = TrendReq(tz=330)

pytrend.build_payload(kw_list=['Fátima'], timeframe='today 1-m')
df = pytrend.interest_over_time()
print(df)




print("Waiting for that bitch named GOOGLE..")
time.sleep(120)
pytrend.build_payload(kw_list=['Rio Grande do Sul'])
related_topics = pytrend.related_topics()

rising_related_topics = related_topics['Rio Grande do Sul']['rising']
top_related_topics = related_topics['Rio Grande do Sul']['top']
rising_topics = rising_related_topics['topic_title'] + ' ' +  rising_related_topics['topic_type']
top_topics = top_related_topics['topic_title'] + ' ' + top_related_topics['topic_type']
topics = pd.concat([rising_topics, top_topics], ignore_index = True)

print(topics)

strings_to_check = ['disaster', 'tragedy', 'flood', 'murder', 'death', 'dead', 'donation',
                    'fire', 'storm', 'hurricane', 'cyclone', 'tornado', 'earthquake',
                    'shooting', 'terrorism', 'bomb', 'killed', 'kidnapping', 'genocide', 'war']

if topics.str.contains('|'.join(strings_to_check), case=False).any():
    print("Bad indicator")
else:
    print("Good indicator")


 """
