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
from pandas.tseries.offsets import BDay
#%%
#TODO: get a column in the data that says what the product is. If WTI, then apply WTI trading rules, etc
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
        
    #this fuction returns the JSON associated with an EIA API request
    def return_js(self,category_id,key,category):
        if category:
            url = 'http://api.eia.gov/category/?api_key=YOUR_API_KEY_HERE&category_id=CATEGORY'
        else:
            
            url = 'http://api.eia.gov/series/?api_key=YOUR_API_KEY_HERE&series_id=CATEGORY'
        url = self.add_key(url)
        url = url.replace('CATEGORY',str(category_id))
        r = requests.get(url, allow_redirects=True, stream=True, headers=headers).json()
        return(r)
        
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

def business_day(date):
    #the commented line below changes string to date if neccecary.
    #date = datetime.strptime(date_string,'%Y-%m-%d')
    business = bool(len(pd.bdate_range(date, date)))
    return(business)

def nymex_rules(date):
    #returns the last trade date of any given date
    
    cutoff_str = str(date.year)+'-'+str(date.month)+'-'+str(25)
    cutoff = datetime.strptime(cutoff_str,'%Y-%m-%d')
    
    #determine if the 25th of the month is a business day. If not, then find the next business day
    
    #if not business_day(cutoff):
    
    while not business_day(cutoff):
        cutoff = cutoff - relativedelta(days=1)
    
    three_days = 0
    while three_days < 3:
        
        cutoff = cutoff - relativedelta(days=1)
        
        if business_day(cutoff):
            three_days = three_days+1
    
    return(cutoff) 

#splits a flat dataframe into spot and futures dataframes
def spot_futures(df):
    split = [x if x.find('Spot')!=-1 else 0 for x in df['Data']][0]
    futures = df[df['Data']!= str(split)]
    spot = df[df['Data']== str(split)]
    return(spot,futures)

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


#this function does all the data transformation needed for futures curves. The input(dataframe) will change depending on if the user wants
#to calculate futures curves for all possible dates, or a given input list    
def transformation(df):
        df['months to add'] = [int(x[-1]) if x.find('Spot')==-1 else 0 for x in df['Data']]
        #calculate the cutoff for all dates. If a date for any contract is > cutoff, then the months to add needs to incease by 1!
        df['cutoff'] = [nymex_rules(x) if r in [1,2,3,4] else np.nan for x,r in zip(df['Date'],df['months to add'])]
        
        #months to add needs to be increased by one, if the date is greater than the contract one cutoff, specified in the rules
        df['months to add'] = [x+1 if ltd<d else x for x, ltd,d in zip(df['months to add'],df['cutoff'],df['Date'])]
        
        df['futures date'] = [x+relativedelta(months=m) for x,m in zip(df['Date'],df['months to add'])]
        df['future trade dates'] = [futures_dates(x) if m != 0  else np.nan for x,m in zip(df['futures date'],df['months to add'])]
        #add all potential trade dates
        split = [x if x.find('Spot')!=-1 else 0 for x in df['Data']][0]
        futures = df[df['Data']!= str(split)]
        flt = futures['future trade dates'].apply(pd.Series)
        flt['pivot_id'] = flt.index
        flt = pd.melt(flt,id_vars = 'pivot_id', value_name = 'trade dates')
        flt = flt.drop('variable',axis=1)
        flt = flt.dropna(axis=0)
        #merge together all trade dates, with the futures contract prices.
        merged = df.merge(flt,how = 'left', left_index=True,right_on='pivot_id')
        merged = merged.drop('future trade dates', axis=1)
        merged = merged.drop('pivot_id',axis=1)
        merged = merged.drop('futures date',axis=1)
        return(merged)
    
#add all trade dates for future contracts:
#TODO: add option to calculate all dates, or the given list of dates used in the graph
def product_futures(product_frame,specified_dates=None,all_data = False):    
    
    #spot, futures = spot_futures(product_frame)
    
    if not all_data and specified_dates == None:
        raise Exception('A list of dates was not specified. Either provide a list of dates, or set all_date=True')
    
    if specified_dates != None and all_data == False:
        #use this when only a few futures curves need to be calculated
        #only calculate futures curves for specified start dates
        #TODO: dont include spot prices. Add all spot prices to df at the end
        #TODO: dont pass any spot prices to the transformation function. This is useless! Append spor prices later!
        l = []
        for date in specified_dates:
            slicer = product_frame[(product_frame['Date']==date)].copy() #adding .copy() avoids a setting with copy warning
            x = transformation(slicer)
            l.append(x)
            df = pd.concat(l)
        return(df)
    
    elif specified_dates == None and all_data:  
        return(transformation(product_frame))
    
    else: 
        raise Exception('Either provide a list of spot dates, or set all_data=True')

#used to graph the data before adding futures curves
def first_graph(df):
    df_pivot = df.pivot(index='Date', columns='Data',values='Value')
    #filter based on date
    df_pivot = df_pivot.loc['2014-01-01':]
    
    fig, ax = plt.subplots(figsize=(12, 9))
    ax.plot(df_pivot)
    ax.legend(df_pivot.columns)

def graph_forward(merged,spot,forward_dates):
    """ 
    TODO: add docstrings to heat functions
    TODO: find the most recent date and graph it, with appropriate label. Also include +/- day over day
    """
    #get the minimum date in order to cut off spot prices on graph
    dates_list = [datetime.strptime(date, '%Y-%m-%d').date() for date in forward_dates]
    min_date = min(dates_list)
    
    #TODO: refactor these names
    spot = spot[(spot['Date'] >= pd.Timestamp(min_date))] #adding pd.Timestamp is neccecary to avoid a matplotlib warning
    merged_futures = merged
    merged_spot = spot
    
    x_spot = merged_spot['Date']
    y_spot = merged_spot['Value']
    
    fig, ax = plt.subplots(figsize=(12, 9))
    ax.plot(x_spot,y_spot,label = 'WTI Spot Price')
  
    
    for fd in forward_dates:
        #plot a dot on the spot line where the futures curve applies:
        spot_dot = merged_spot[(merged_spot['Date']==str(fd))]
        x_dot = spot_dot['Date']
        y_dot = spot_dot['Value']
        ax.plot(x_dot, y_dot, marker='o', markersize=10,label = None)
        
        
        futures_plot = merged_futures[(merged_futures['Date']==str(fd))]
        x_fut = futures_plot['trade dates']
        y_fut = futures_plot['Value']
        ax.plot(x_fut, y_fut, label = fd)
        
        
    ax.legend(loc='best')
    return(fig)

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
forward_dates = ['2018-04-02','2019-04-01','2018-10-05']
#merged = product_futures(wti_add,all_data=True)
spot,futures = spot_futures(wti)
merged = product_futures(futures,specified_dates=forward_dates)
#merged = merged[(merged['Date']>'2018-01-01')]
#%%
#merged.to_csv(r'C:\Users\mossgrant\merged.csv')

#%%
#default min date should be the min specified_date
fig = graph_forward(merged,spot,forward_dates)     

#%%
#split up into seperate dataframes:
    
#data_list = wti_add['Data'].unique()
#contract_dict = {}
#
#for contract in data_list:
#    df = wti_add[wti_add['Data']== str(contract)] 
#    contract_dict.update({str(contract):df})
#    
#for key,value in contract_dict.items():
#    dates = value['futures date']
#    print(value[dates.isin(dates[dates.duplicated()])])
