# -*- coding: utf-8 -*-

import pandas as pd
import requests
import os
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

data_file = r'F:\bucom\INPUT DATA\Oil\EIA\contract_prices.csv'
key = ''


#function to return all of the scraped data that is not in the database
def return_not_in_csv(df1, df2):
    intersected_df = pd.merge(df1, df2, how='right', indicator=True) #change the merge to join on the key
    intersected_df = intersected_df[intersected_df['_merge']=='right_only']
    intersected_df = intersected_df.drop('_merge',1)
    return(intersected_df)

def add_key(url,key=key):
    url = url.replace('YOUR_API_KEY_HERE',str(key))
    return(url)
    
def return_df(js):
    series_id = js['request']['series_id']
    #name=js['series'][0]['name']
    #updates = js['series'][0]['updated']
    #description = js['series'][0]['description']
    df = pd.DataFrame(js['series'][0]['data'])
    df.rename(columns = {0:'Date',1:'Value'},inplace=True)
    df['Data'] = js['series'][0]['description']
    df['Frequency'] = js['series'][0]['f']
    df['Units'] = js['series'][0]['unitsshort']
    df['Units Description'] = js['series'][0]['units']
    #df['Source'] = js['series'][0]['source']
    #todo: add option to return a metadataframe
    df['series id'] = series_id
    return(df)
    
def return_json(category_id,category):
    if category:
        url = 'http://api.eia.gov/category/?api_key=YOUR_API_KEY_HERE&category_id=CATEGORY'
    else:
        
        url = 'http://api.eia.gov/series/?api_key=YOUR_API_KEY_HERE&series_id=CATEGORY'
    url = add_key(url)
    url = url.replace('CATEGORY',str(category_id))
    r = requests.get(url, allow_redirects=True, stream=True, headers=headers).json()
    
    if not category:
        #get the actual data and reutn the df. More 'columns' could be added.
        df = return_df(r)
    else:
    
        parent = r['category']['parent_category_id']
        children = r['category']['childcategories']
        
        if len(children)==0:
        
            children = r['category']['childseries']
            
        df = pd.DataFrame(children)
        df['Parent_ID'] = parent
        df['parent'] = category_id
    return(df)

def gather_prices(price_list):
    price_data = []
    for s in price_list:
        prices = return_json(s,category = False)
        price_data.append(prices)
    df = pd.concat(price_data, axis=0, sort=False, ignore_index=True)
    df['Date'] = df['Date'].astype('datetime64[ns]')
    return(df)
    

#%%
# try to get a large list of petroleum prices
prices = return_json(714757,category = True) # then get all of the series id's from these categories
prices = [i for i in prices['category_id']]

#gather all of the series_id's
series_list = []
for price in prices:
    df = return_json(price,category=True)
    try:
        #need to keep drilling until there are series available. What should the while be??
        series_list.append([i for i in df['series_id']])
    except:
        #print('cant get series for '+str(df))
        None

for s in series_list:
    print(s)

#%%
#main program
price_list = ['PET.RCLC1.D','PET.RCLC2.D','PET.RCLC3.D','PET.RCLC4.D','PET.RWTC.D']
df = gather_prices(price_list)

if os.path.isfile(data_file):
    #if file exists, then append all the new data
    df_csv = pd.read_csv(data_file)
    df_csv['Date'] = df_csv['Date'].astype('datetime64[ns]')
    not_in_csv = return_not_in_csv(df1=df_csv,df2=df)

    if not not_in_csv.empty: #if there are new rows
    
        with open(data_file, 'a') as f:
            rows_added = str(not_in_csv.shape[0])
            print(rows_added)
            not_in_csv.to_csv(f, header=False,index=False)
else:
    #if the file does not exists, then save it and wait for the next day
    first_dataframe = df
    first_dataframe.to_csv(data_file, header=True,index=False)
