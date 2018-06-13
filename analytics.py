# Attempt at recreating the analytics download via python
import csv
import requests
import pandas as pd
from urllib.parse import urlparse
import os
from pathlib import Path
import datetime

# All 30 days data frame creation
# Look into checking previous running total date and only running the 30 day processing if change
all_domains_30_days = 'https://analytics.usa.gov/data/agriculture/all-domains-30-days.csv'
with requests.Session() as s:
    download = s.get(all_domains_30_days)

    decoded_content = download.content.decode(download.encoding)

    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    df_30_days = pd.DataFrame(my_list)
    new_header = df_30_days.iloc[0] #grab the first row for the header
    df_30_days = df_30_days[1:] #take the data less the header row
    df_30_days.columns = new_header

# Realtime data frame creation
all_pages_realtime = 'https://analytics.usa.gov/data/agriculture/all-pages-realtime.csv'
with requests.Session() as s:
    download = s.get(all_pages_realtime)

    decoded_content = download.content.decode(download.encoding)

    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    df_realtime = pd.DataFrame(my_list)
    new_header = df_realtime.iloc[0] #grab the first row for the header
    df_realtime = df_realtime[1:] #take the data less the header row
    df_realtime.columns = new_header
    df_realtime['domain'] = ''

# Add domain to realtime data frame
for index, row in df_realtime.iterrows():
    parsed_url = urlparse(('//' + row['page']))
    domain_in = parsed_url.netloc
    row['domain'] = domain_in


# Merge 30 days and Realtime, redefine & reorganize field names
df_merge = pd.merge(df_30_days, df_realtime, on='domain', how='outer')
cols = ['domain', 'page', 'page_title', 'visits', 'pageviews', 'users', 'pageviews_per_session', 'avg_session_duration', 'exits', 'active_visitors']
df_merge = df_merge[cols]
ts = datetime.datetime.now().timestamp()
colsNew = ['domain', 'page', 'page_title', 'visits', 'pageviews', 'users', 'pageviews_per_session', 'avg_session_duration', 'exits', 'active_visitors']
for i, column in enumerate(colsNew):
    if(column != 'domain' and column != 'page' and column != 'page_title'):
        column = column + ' TS' + str(ts)
        colsNew[i] = column
df_merge.columns = colsNew

'''
###
#THIS IS A MESS AND DOES NOT WORK
###
# Check for existing running total file
if((Path(os.path.dirname((os.path.realpath(__file__))) + '\\data\\program_analytics_running_total.csv')).is_file()):
    print('Updating existing file')
    df_running_total = pd.read_csv(os.path.dirname(os.path.realpath(__file__)) + '\\data\\program_analytics_running_total.csv')
    df_running_total = pd.merge(df_running_total, df_merge, on='page', how='outer', suffixes=('','_y'))
    try:
        df_running_total = df_running_total.drop(columns=['domain_y'])
    except:
        pass
    try:
        df_running_total = df_running_total.drop(columns=['page_y'])
    except:
        pass
    try:
        df_running_total = df_running_total.drop(columns=['page_title_y'])
    except:
        pass
else:
    print('Creating new running file')
    df_running_total = df_merge
'''

# All 4 data frames to CSV

df_merge.to_csv(os.path.dirname(os.path.realpath(__file__)) + '\\data\\program_analytics.csv', index = False)
df_30_days.to_csv(os.path.dirname(os.path.realpath(__file__)) + '\\data\\30days.csv', index = False)
df_realtime.to_csv(os.path.dirname(os.path.realpath(__file__)) + '\\data\\realtime.csv', index = False)
#df_running_total.to_csv(os.path.dirname(os.path.realpath(__file__)) + '\\data\\program_analytics_running_total.csv', index = False)
