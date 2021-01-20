# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 18:41:40 2020

@author: kxj17699
"""


#import functions
from bs4 import BeautifulSoup
import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime 
import time
from pandas.tseries.offsets import BDay
from pathlib import Path
import sys



class yahoo_finance_historical_data:
    '''
    The Task
    Read yahoo finance data to a pandas dataframe and export to csv file for future use.
    Daily files read only - groupby to look at weeks and months if required
    Dividend data is also added
    Index = datetime format
    Columns = open, daily low, daily high, close, adj close, volume, dividend
    '''
    
    def __init__(self): 
        
        #instantiate the class
        print("Class Instantiated")
        
    def extract_historical_daily(self,ticker = 'AAPL', url = 'https://uk.finance.yahoo.com/quote/', no_days = 8600, last_day = datetime.today(), outfile_root = str(Path.home())):    

        #key variables 
        self.ticker = ticker
        self.url = url
        self.no_days = no_days 
        self.last_day = last_day
        self.outfile_root = outfile_root
        
        self.daily_data = [] #empty list for daily stock data - 7 columns
        self.div_data = [] #empty list for stock dividend data
        
        #scrape file 
        self.yf_extract_hist()
            
        #converted scraped data lists into two dataframesm daily & dividend. Merge on date index
        self.create_daily_dataframe()

        #write final merged pandas dataframe to disk - csv format        
        self.output_pandas_to_csv()
        
    def yf_extract_hist(self):

        '''   
        Parameters
        ----------
        ticker : TYPE, optional
            DESCRIPTION. The default is 'AAPL'.
        url : TYPE, optional
            DESCRIPTION. The default is 'https://uk.finance.yahoo.com/quote/'.
        no_days : TYPE, optional
            DESCRIPTION. The default is 8600. Takes the earliest day to 1988, should cover all stocks listed
        last_day : TYPE, optional
            DESCRIPTION. The default is datetime.today().
        Returns
        -------
        None.
    
        Subroutine initiates extraction process for yahoo finance dividend table
        outputs csv file to specified directpry
        PRECONDITION: Both url and ticker strings are valid. Incorrect entries will cause error
        KJAGGS NOV 2020
        '''
    
        #requires iteration through the table in chunks of 100 day ranges
        days_sets = int(self.no_days/100)
       
        #iterate through set of 100 days
        for days_sets_range in range(days_sets):
            #define last day within the 100 day set - remeber, descending intervals. last is youngest day
            temp_end = self.last_day - BDay(days_sets_range * 100 + (days_sets_range))
            #temp_start is the oldest day in the set, defined as 100 business days (BDay(100)) from youngest day
            temp_start = temp_end - BDay(100)
            
            #convert a time.struct_time object or a tuple containing 9 elements corresponding to time.struct_time object to time in seconds passed since epoch in local time.
            #the epoch is January 1, 1970, 00:00:00 
            epoch_start = int(time.mktime(temp_start.timetuple()))
            epoch_end = int(time.mktime(temp_end.timetuple()))
            
            self.ticker_url = (self.url + str(self.ticker) + '/history?period1=' + str(epoch_start) + '&period2=' + str(epoch_end) + '&interval=1d&filter=history&frequency=1d')
            
            #pass information to scraping tool - if return flag is 1 then exit function
            #this flag indicates if the serach is empty i.e. there is no stock data associated with this date
            empty_flag = self.historic_analysis()
            
            #exit function if flag returned as 1
            if empty_flag == 1:
                print("End of data found")
                break
        
        
        
    def historic_analysis(self):
       '''
       (string)->(int)
       Subroutine to extract historic stock data table from yahoo finance
       Borrows script from: #https://github.com/ee07kkr/stock_forex_analysis/blob/dataGathering/dataScrape.py
       First attempts at web scraping
       Requested table and date range is converted to a pandas dataframe
       Precondition - user supplied ticker & URL addresses are correct
       KJAGGS Nov 2020
       '''

       #set to zero passed back ad 1 if empty rows are found
       empty_flag_pass = 0
       
       for attempt in range(10):
           
           try:
              #scrape data using beautiful soup
              result = requests.get(self.ticker_url)
              #print(requests.get(self.ticker_url).text)
              c = result.content
              if len(c) == 0:    
                 print("Warning- length of c = ", len(c))
              soup = BeautifulSoup(c, "lxml")
              summary = soup.find('div',{'class':'Pb(10px) Ovx(a) W(100%)'})
           
              #print(summary)
           
              tables = summary.find_all('table')
              rows  = tables[0].find_all('tr')
           except Exception as err:
                   print("Error: " + str(err))
                   print("Attempt #", attempt + 1)
           else:
              break                     
           #print(len(rows))
       
       '''
       for attempt in range(10):
           try:
               tables = summary.find_all('table')
               rows  = tables[0].find_all('tr')
               #break
           except Exception as err:
               print("Error: " + str(err))
               print("Attempt #, attempt" + 1)
           else:
               break
       else:  
            print(self.ticker, " has not been output")
       ''' 
       
       #loop through all the rows found in the data extraction
       #data split into dividend and daily lists, depending upon number of column matches
       for tr in rows:
           cols = tr.findAll('td')
           #print(len(cols))
           #7 columns match for daily stock data
           if len(cols) == 7:
               for td in cols:
                   daily_text = td.find(text=True)
                   #print(daily_text)
                   self.daily_data.append(daily_text) 
                   #print("daily")
           #dividend data can be found by matching two columns - date + dividend sum in pence
           elif len(cols) == 2:
               for td in cols:
                   div_text = td.find(text=True)
                   self.div_data.append(div_text)
                   #print("dividend")
                   #data.append(text)
           #else:
               #print("empty")
               #set flag to 1 - empty data so no stock data exists older than this date
               #empty_flag_pass = 1
               
       return empty_flag_pass #passing value of 1 tells routine to stop
    
   
    def create_daily_dataframe(self):
       '''
       Converts extracted data lists into pandas dataframes
       Dataframes are merged into 1 final table, merging on datetimeindex to create a dividebd column
       '''

        
       #convert scarped daily stock data into pandas dataframe 
       self.df_daily = pd.DataFrame(np.array(self.daily_data).reshape(int(len(self.daily_data)/7),7))
       self.df_daily.columns=['Date','Open','High','Low','Close','Adj Close','Volume']
       self.df_daily.replace(',','', regex=True, inplace=True)
       self.df_daily.set_index('Date',inplace=True)    

       #convert scarped dividend stock data into pandas dataframe 
       self.df_div = pd.DataFrame(np.array(self.div_data).reshape(int(len(self.div_data)/2),2))
       self.df_div.columns=['Date','Dividend']
       self.df_div.replace(',','', regex=True, inplace=True)
       self.df_div.set_index('Date',inplace=True) 
       
       #merge data into one dataframe, merged on index to create a new dividend column
       self.df_daily = self.df_daily.join(self.df_div, how='outer')   #self.df_daily.merge(self.df_div,  how="outer",left_index=True, right_index=True)
       self.df_daily.index = pd.to_datetime(self.df_daily.index)
       self.df_daily = self.df_daily.sort_index()
       self.df_daily = self.df_daily.fillna(0)
       #print(self.df_daily)       
    
    
    def output_pandas_to_csv(self):
        '''
        Write final dataframe to csv format text file
        PRECONDITION: Output directory is correctly defined
        '''


        filename =  self.outfile_root + str(self.ticker) + ' Daily Hist.csv'
        append_write = 'w'
        self.df_daily.to_csv(filename,sep=',',mode=append_write)   

        self.df_daily = None

    '''
    def output_csv(self):
        filename =  self.outfile_root + str(self.ticker) + 'Daily Hist.csv'
        if os.path.exists(filename):
           append_write = 'a'
           self.df.to_csv(filename,sep=',',mode=append_write,header = False)
        else:
            append_write = 'w'
            self.df.to_csv(filename,sep=',',mode=append_write)   
    '''
    
    
def yf_extract_hist_data(ticker = 'BP.L', url = 'https://uk.finance.yahoo.com/quote/', outfile_root = "Z:\\Code and Data Science\\Scripts\\Python\\Financial Data\\Yahoo Finance Web Scraping\\Historical Data\\" ):           
    '''
    Run routine to extract daily historical data + dividends for a user defined stock
    PRECONDITION: Ticker and directory paths supplied are correct
    K JAGGS NOV 2020
    '''
    #instantiate yahoo_historical_data class
    yfhd_daily = yahoo_finance_historical_data()
    #run sequence to output daily data plus dividend data into one csv file
    yfhd_daily.extract_historical_daily(ticker = ticker, url = url, no_days = 10000, last_day = datetime.today(), outfile_root = outfile_root)

    
if __name__ == "__main__":
    
    search_ticker = 'BT-A.L'
    search_outpath = "C:\\Users\\jaggs\\OneDrive\\Code and Data Science\\Scripts\\Python\\Financial Data\\Yahoo Finance Web Scraping\\Historical Data\\"
    #yf_extract_hist_data(ticker=search_ticker, outfile_root = search_outpath)        
    yf_extract_hist_data(ticker=search_ticker, outfile_root = search_outpath) 
    
    #yf_extract_hist_data(ticker=search_ticker, outfile_root = search_outpath)        