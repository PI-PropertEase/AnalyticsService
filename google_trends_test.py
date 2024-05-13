import pandas as pd
import time                        
from pytrends.request import TrendReq
pytrend = TrendReq()

#pytrend.build_payload(kw_list=['Fátima'], timeframe='2019-05-05 2024-05-13')
#pytrend.interest_over_time()

df = pd.read_csv('fatimaTrends.csv')

df = df.set_index(df['Semana'])
df = df.drop(['Semana'], axis=1)

pct_changes = df.pct_change()

peak_dates = pct_changes[pct_changes["Fátima"] > 0.8]
#print(peak_dates)


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


