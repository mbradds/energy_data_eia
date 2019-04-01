#TODO: take a look at futures prices here:
#https://ca.finance.yahoo.com/quote/CL%3DF/futures?p=CL%3DF
import pandas as pd
import requests
import os
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta
import datetime
from datetime import datetime
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
from web_scraping.scraping_modules import scraping as sc
from calendar import monthrange

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

# futures curve functions

#this gets a list of all days in a month, for a given input date.        
def futures_dates(date):
    date_list = []
    year = str(date.year)
    month = str(date.month)
    ran = monthrange(date.year, date.month)
    for d in range(1,ran[1]+1): #add the +1 to include months with 31 days!
        date_string = year+'-'+month+'-'+str(d)
        date_string = datetime.strptime(date_string,'%Y-%m-%d')
        date_list.append(date_string)
    return(date_list)

#add all trade dates for future contracts:
def product_futures(product_frame):
    #add columns to determine how many months into the future the contract will apply
    product_frame['months to add'] = [int(x[-1]) if x.find('Spot')==-1 else 0 for x in product_frame['Data']]
    product_frame['futures date'] = [x+relativedelta(months=m) for x,m in zip(product_frame['Date'],product_frame['months to add'])]
    product_frame['future trade dates'] = [futures_dates(x) if m != 0  else np.nan for x,m in zip(product_frame['futures date'],product_frame['months to add'])]
    #add all potential trade dates
    split = [x if x.find('Spot')!=-1 else 0 for x in wti_add['Data']][0]
    futures = wti_add[wti_add['Data']!= str(split)]
    flt = futures['future trade dates'].apply(pd.Series)
    flt['pivot_id'] = flt.index
    flt = pd.melt(flt,id_vars = 'pivot_id', value_name = 'trade dates')
    flt = flt.drop('variable',axis=1)
    flt = flt.dropna(axis=0)
    #merge together all trade dates, with the futures contract prices.
    merged = wti_add.merge(flt,how = 'left', left_index=True,right_on='pivot_id')
    return(merged)
#%%
#main program
s = sc.scrape(os.getcwd())
file = s.config_file('key.json')
key = file['api_key']    

wti_list = ['PET.RWTC.D','PET.RCLC1.D','PET.RCLC2.D','PET.RCLC3.D','PET.RCLC4.D']
brent_list = ['PET.RBRTE.D']
eia = eia_api_data(key)
wti = eia.gather_prices(wti_list)
brent = eia.gather_prices(brent_list)
wti_add = wti.copy()

#%%

merged = product_futures(wti_add)




#everything below here is garbage....
#%%
data_list = wti_add['Data'].unique()
contract_dict = {}

for contract in data_list:
    df = wti_add[wti_add['Data']== str(contract)] 
    contract_dict.update({str(contract):df})
#%%
for key,value in contract_dict.items():
    dates = value['futures date']
    print(value[dates.isin(dates[dates.duplicated()])])
    #df = value.pivot(index='futures date', columns='Data',values='Value')

#split = [x if x.find('Spot')!=-1 else 0 for x in wti_add['Data']][0]
#spot = wti_add[wti_add['Data']== str(split)]
#futures = wti_add[wti_add['Data']!= str(split)]

#spot_pivot = spot.pivot(index='futures date', columns='Data',values='Value')
#futures_pivot = futures.pivot(index=None, columns='Data',values='Value')


#%%


#%%
#graphing a preliminary analysis
wti_pivot = wti.pivot(index='Date', columns='Data',values='Value')
#filter based on date
wti_pivot = wti_pivot.loc['2014-01-01':]

fig, ax = plt.subplots(figsize=(12, 9))
ax.plot(wti_pivot)
ax.legend(wti_pivot.columns)

#%%


int('b')==True