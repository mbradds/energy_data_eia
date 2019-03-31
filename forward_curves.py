# -*- coding: utf-8 -*-
import pandas as pd
import requests
import os
import matplotlib
import matplotlib.pyplot as plt
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
module_path = r'/home/grant/Documents/web_scraping/scraping_modules'
if module_path not in sys.path:
    sys.path.insert(0,module_path)

import scraping as sc

#%%

class eia_api_data:
    
    def __init__(self,key):
        self.key = str(key)
    

    def add_key(self,url):
        url = url.replace('YOUR_API_KEY_HERE',self.key)
        return(url)
    
    def return_df(self,js):
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
        
    def return_json(self,category_id,key,category):
        if category:
            url = 'http://api.eia.gov/category/?api_key=YOUR_API_KEY_HERE&category_id=CATEGORY'
        else:
            
            url = 'http://api.eia.gov/series/?api_key=YOUR_API_KEY_HERE&series_id=CATEGORY'
        url = self.add_key(url)
        url = url.replace('CATEGORY',str(category_id))
        r = requests.get(url, allow_redirects=True, stream=True, headers=headers).json()
        
        if not category:
            #get the actual data and reutn the df. More 'columns' could be added.
            df = self.return_df(r)
        else:
        
            parent = r['category']['parent_category_id']
            children = r['category']['childcategories']
            
            if len(children)==0:
            
                children = r['category']['childseries']
                
            df = pd.DataFrame(children)
            df['Parent_ID'] = parent
            df['parent'] = category_id
        return(df)
    
    def gather_prices(self,id_list):
        price_data = []
        for s in id_list:
            prices = self.return_json(s,self.key,category = False)
            price_data.append(prices)
        df = pd.concat(price_data, axis=0, sort=False, ignore_index=True)
        df['Date'] = df['Date'].astype('datetime64[ns]')
        return(df)
    
#%%
#main program
direc = '/home/grant/Documents/EIA'
s = sc.scrape(direc)
file = s.config_file('key.json')
key = file['api_key']    

wti_list = ['PET.RWTC.D','PET.RCLC1.D','PET.RCLC2.D','PET.RCLC3.D','PET.RCLC4.D']
brent_list = ['PET.RBRTE.D']
eia = eia_api_data(key)
wti = eia.gather_prices(wti_list)
brent = eia.gather_prices(brent_list)
#%%
#graphing a preliminary analysis
wti_pivot = df.pivot(index='Date', columns='Data',values='Value')
#filter based on date
wti_pivot = wti_pivot.loc['2014-01-01':]

fig, ax = plt.subplots(figsize=(12, 9))
ax.plot(wti_pivot)
ax.legend(wti_pivot.columns)




