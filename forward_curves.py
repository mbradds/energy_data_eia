#TODO: take a look at futures prices here:
#https://ca.finance.yahoo.com/quote/CL%3DF/futures?p=CL%3DF
import pandas as pd
import requests
import os
import math
import numpy as np
import matplotlib.pyplot as plt 
from dateutil.relativedelta import relativedelta
from datetime import datetime
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
from Documents.web_scraping.scraping_modules import scraping as sc
from calendar import monthrange
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
#%%
class eia_api_data:
    '''contains all the neccecary methods to create flat (stacked) real time data from the EIA's API service'''
    
    def __init__(self,key):
        '''Data access requires an api key from the EIA'''
        self.key = str(key)
    

    def add_key(self,url):
        '''adds the users api key to the url for requesting later on in the return_json function'''
        url = url.replace('YOUR_API_KEY_HERE',self.key)
        return(url)
    
    def return_df(self,js):
        '''Takes a json api response and creates usable panel data.
        Each EIA api response includes useful metadata (units, location, etc) as well as the panel data (date, value). 
        These are combined into one dataframe here
        '''
        
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
        
        
    def return_json(self,category_id,key,category,return_request=False):
        '''builds a url, sends a request, and builds either a dataframe, or a json respnse from the EIA
        
        Keyword arguments:
            category_id -- The appropriate id used to select EIA data. These can be found using the API query browse: https://www.eia.gov/opendata/qb.php?category=371
            key -- Your individual api key issued by the EIA
            category -- Used to determine if the api request should return data, or drill to the next level of energy data. (True/False)
            return_request -- tells the function to transform the api request into a formatted dataframe, or raw json.
        
        method dependencies:
            return_df
        '''
        if category:
            url = 'http://api.eia.gov/category/?api_key=YOUR_API_KEY_HERE&category_id=CATEGORY'
        else:
            
            url = 'http://api.eia.gov/series/?api_key=YOUR_API_KEY_HERE&series_id=CATEGORY'
        url = self.add_key(url)
        url = url.replace('CATEGORY',str(category_id))
        r = requests.get(url, allow_redirects=True, stream=True, headers=headers).json()
        
        if return_request == False:
        
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
        else:
            return(r)
    
    def gather_prices(self,id_list):
        '''loops through a list of EIA series id's and stackes the dataframes into flat data. This is the proper format for curve analysis.'''
        
        price_data = []
        for s in id_list:
            prices = self.return_json(s,self.key,category = False)
            price_data.append(prices)
        df = pd.concat(price_data, axis=0, sort=False, ignore_index=True)
        df['Date'] = df['Date'].astype('datetime64[ns]')
        return(df)


class trading_rules:
    '''Contains the structure of the nymex trading rules for relevant products. See the websites below for the rules.
    
    WTI -- https://www.cmegroup.com/trading/energy/crude-oil/light-sweet-crude_contract_specifications.html
    
    The idea is that for each month, you need to know the last trade date for contract one. From here you can determine
    what the potential delivery dates are for each day the contract trades.
    Note: right now, only crude oil rules are programmed.
    '''
    
    def __init__(self):
        self = self
    
    
    def business_day(self,date):
        #the commented line below changes string to date if neccecary.
        #date = datetime.strptime(date_string,'%Y-%m-%d')
        business = bool(len(pd.bdate_range(date, date)))
        return(business)

    def nymex_rules(self,date):
        '''returns the last trade date for any given date (month)'''
        
        cutoff_str = str(date.year)+'-'+str(date.month)+'-'+str(25)
        cutoff = datetime.strptime(cutoff_str,'%Y-%m-%d')
        
        #determine if the 25th of the month is a business day. If not, then find the next business day
        #if not business_day(cutoff):
        
        while not self.business_day(cutoff):
            cutoff = cutoff - relativedelta(days=1)
        
        three_days = 0
        while three_days < 3:
            
            cutoff = cutoff - relativedelta(days=1)
            
            if self.business_day(cutoff):
                three_days = three_days+1
        
        return(cutoff) 

class futures(trading_rules):
    '''inherits the trading rules, and applies them to real data.'''
    
    def __init__(self,api_codes):
        self.api_codes = api_codes
        trading_rules.__init__(self)

    #calculates the daily return of eia contract data
    def contract_returns(self,df):
        ''' unpivots the flat daily price data and calculates daily returns and spreads for each contract/spot combination'''
        
        ret = df.copy()
        ret = ret.pivot(index='Date', columns='Data',values='Value')
    
        for x in ret.columns:
            if x.find('Spot') != -1:
                spot = x
            elif x.find('1') != -1:
                contract_1 = x
            elif x.find('2') != -1:
                contract_2 = x
            elif x.find('3') != -1:
                contract_3 = x
            elif x.find('4') != -1:
                contract_4 = x
            else:
                None
            
        ret['contract_4 - contract_1'] = ret[contract_4] - ret[contract_1]
        ret['contract_3 - contract_1'] = ret[contract_3] - ret[contract_1]
        ret['contract_2 - contract_1'] = ret[contract_2] - ret[contract_1] 
        ret['contract_4 - contract_3'] = ret[contract_4] - ret[contract_3]   
        ret['contract_3 - contract_2'] = ret[contract_3] - ret[contract_2]
        ret['contract_4 - Spot'] = ret[contract_4] - ret[spot]
        ret['contract_3 - Spot'] = ret[contract_3] - ret[spot]
        ret['contract_2 - Spot'] = ret[contract_3] - ret[spot]
        ret['contract_1 - Spot'] = ret[contract_1] - ret[spot]
        
        ret['spot daily return'] = ret[spot] - ret[spot].shift(1)
        ret['contract 1 daily return'] = ret[contract_1] - ret[contract_1].shift(1)
        ret['contract 2 daily return'] = ret[contract_2] - ret[contract_2].shift(1)
        ret['contract 3 daily return'] = ret[contract_3] - ret[contract_3].shift(1)
        ret['contract 4 daily return'] = ret[contract_4] - ret[contract_4].shift(1)
        
        ret = ret.reset_index()
        
        return(ret)
        
    def spot_futures(self,df):
        '''splits a flat dataframe into spot and futures dataframes'''
        split = [x if x.find('Spot')!=-1 else 0 for x in df['Data']][0]
        futures = df[df['Data']!= str(split)]
        spot = df[df['Data']== str(split)]
        return(spot,futures)

    #TODO: the entire code can be sped up here. Right now, the days in the month are calculated for every day. Could use dynamic programming algo to lookup     
    def futures_dates(self,date):
        '''For a given date, determines all the days in the month'''
        date_list = []
        year = str(date.year)
        month = str(date.month)
        ran = monthrange(date.year, date.month)
        for d in range(1,ran[1]+1): #add the +1 to include months with 31 days!
            date_string = year+'-'+month+'-'+str(d)
            date_string = datetime.strptime(date_string,'%Y-%m-%d')
            date_list.append(date_string)
        return(date_list)


    def transformation(self,df):
        '''
        This function does all the data transformation needed for futures curves. The input(dataframe) will change depending on if the user wants
        to calculate futures curves for all possible dates, or a given input list    
        '''
        
        #determine which rules to use!
        #TODO: add more rules as prodcuts are added (RBOB gasoline, propane, etc)
        if 'PET.RWTC.D' in self.api_codes:
            trade_rules = self.nymex_rules
        
        
        df['months to add'] = [int(x[-1]) if x.find('Spot')==-1 else 0 for x in df['Data']]
        #calculate the cutoff for all dates. If a date for any contract is > cutoff, then the months to add needs to incease by 1!
        df['cutoff'] = [trade_rules(x) if r in [1,2,3,4] else np.nan for x,r in zip(df['Date'],df['months to add'])]
            
        #months to add needs to be increased by one, if the date is greater than the contract one cutoff, specified in the rules
        df['months to add'] = [x+1 if ltd<d else x for x, ltd,d in zip(df['months to add'],df['cutoff'],df['Date'])]
            
        df['futures date'] = [x+relativedelta(months=m) for x,m in zip(df['Date'],df['months to add'])]
        df['future trade dates'] = [self.futures_dates(x) if m != 0  else np.nan for x,m in zip(df['futures date'],df['months to add'])]
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

    def apply_days(self,df):
        '''Adds incremental numbers (days) which allows each curve to be graphed with the same start day (0)
        this function uses split-apply-combine (https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html)
        '''
        grouped = df.groupby('Date')
        l = []
        for name, group in grouped:
            group = group.copy()
            group['day'] = np.arange(len(group))
            l.append(group)
            
        df = pd.concat(l)
        return(df)
    
 
    def product_futures(self,product_frame,specified_dates=None,all_data = False):    
        '''Allows the user to calculate futures curves for every day, or an input list of days
        
        Keyword arguments:
            product_frame -- the flat dataframe returned by using the api class
            specified_dates -- a list of text dates for which to calculate futures curves
            all_data -- Will calculate curves for every day if set to True. 
                        Note: all_data=True takes a while ~10 seconds to calculate. 
                        This can be sped up by calculating days in month only once for each month (see futures_dates function)
        '''
        
        if not all_data and specified_dates == None:
            raise Exception('A list of dates was not specified. Either provide a list of dates, or set all_date=True')
        
        if specified_dates != None and all_data == False:
            #use this when only a few futures curves need to be calculated
            #only calculate futures curves for specified start dates
            l = []
            for date in specified_dates:
                slicer = product_frame[(product_frame['Date']==date)].copy() #adding .copy() avoids a setting with copy warning
                x = self.transformation(slicer)
                l.append(x)
                df = pd.concat(l)
            df = self.apply_days(df)
        
        elif specified_dates == None and all_data:  
            df = self.transformation(product_frame.copy())
            df = self.apply_days(df)
        
        else: 
            None
        #TODO: order the dataframe in a way that makes sense
        return(df)

    def graph_colors(self,n):
        ''' creates a list of ordered colors to use for graphing '''
        co = ['b','g','r','c','m','y','k','w']
        r = int(math.ceil(n/len(co)))
        color_list = co*r
        color_list = color_list[:n]
        return(color_list)
    
    def first_graph(self,df):
        '''used to graph the data before adding futures curves'''
        df_pivot = df.pivot(index='Date', columns='Data',values='Value')
        #filter based on date
        df_pivot = df_pivot.loc['2014-01-01':]
        
        fig, ax = plt.subplots(figsize=(12, 9))
        ax.plot(df_pivot)
        ax.legend(df_pivot.columns)
    
    def graph_overlay(self,merged,spot,forward_dates):
        """ Graphs the futures curves overlayed with the spot price"""
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
        
        colors = self.graph_colors(len(forward_dates))
      
        
        for fd,color in zip(forward_dates,colors):
            #plot a dot on the spot line where the futures curve applies:
            spot_dot = merged_spot[(merged_spot['Date']==str(fd))]
            x_dot = spot_dot['Date']
            y_dot = spot_dot['Value']
            ax.plot(x_dot, y_dot, marker='o', markersize=10,label = 'Trade date',color = str(color))
            
            
            futures_plot = merged_futures[(merged_futures['Date']==str(fd))]
            x_fut = futures_plot['trade dates']
            y_fut = futures_plot['Value']
            ax.plot(x_fut, y_fut, label = fd, color = str(color))
            
            
        ax.legend(loc='best')
        return(fig)
    
    def graph_curves(self,df):
        '''Graphs each futures cruve starting from the same axis'''
    
        df = self.apply_days(df)
        fig, ax = plt.subplots(figsize=(12, 9))
        for x in df['Date'].unique():
            
            graph = df[(df['Date']==str(x))]
            x_ = graph['day']
            y_ = graph['Value']
            ax.plot(x_,y_,label = x)
        ax.legend(loc='best')
        return(fig)

#%%
if __name__ == "__main__":
    wti_list = ['PET.RWTC.D','PET.RCLC1.D','PET.RCLC2.D','PET.RCLC3.D','PET.RCLC4.D']
    #instantiate the scrape_module and gather the neccecary data
    s = sc.scrape(os.getcwd())
    file = s.config_file('key.json')
    key = file['api_key']    
    eia = eia_api_data(key)
    wti = eia.gather_prices(wti_list)
    wti_add = wti.copy()
    #TODO: automaically include the most recent date by default
    
    #instantiate the futures class. One instantiation is needed for each product
    wti_futures = futures(wti_list)
    forward_dates = ['2018-04-02','2019-04-01','2018-10-05']
    spot,futures = wti_futures.spot_futures(wti)
    wti_forward_data = wti_futures.product_futures(futures,specified_dates=forward_dates,all_data=False)
    #merged.to_csv(r'C:\Users\mossgrant\data_files\fwd.csv',index=False)
    #calculate differentials and returns for each contract
    ret = wti_futures.contract_returns(wti_add)
    #ret.to_csv(r'C:\Users\mossgrant\data_files\contract_returns.csv',index=False)
    #graph the output
    fig = wti_futures.graph_overlay(wti_forward_data,spot,forward_dates)     
    fig2 = wti_futures.graph_curves(wti_forward_data)
#%%