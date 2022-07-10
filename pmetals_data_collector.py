import datetime
import re
import threading
from forex_python.converter import CurrencyRates
from time import time, sleep
import requests
import pandas as pd
from db_connector import engine
from config import PMETAL_TABLE_NAME, PMETAL_REQUEST_URL, PMETAL_PATTERN, PMETAL_MARKETCAP_PATTERN, PMETAL_MARKETCAP_URL, PMETALS

TRILLION = 1000000000000
BILLION =  1000000000

class MetalCollector:

    market_caps = pd.DataFrame(columns=['market_cap_in_ounces'], index=PMETALS)

    def __init__(self):
        self.time_in_20_minutes = 0
        self.usd_eur_exchange = 0
        self.marketcap_pattern = re.compile(PMETAL_MARKETCAP_PATTERN, flags=re.RegexFlag.S)
        self.prices_pattern = re.compile(PMETAL_PATTERN)

    def get_zeros(self, zeros_string):  
            if zeros_string == 'T':
                return TRILLION
            elif zeros_string == 'B':
                return BILLION
            else:
                return 0

    def refresh_market_cap(self, p_price_per_ounce):
            match_caps = self.marketcap_pattern.search(requests.get(PMETAL_MARKETCAP_URL).content.decode("utf-8"))
            try:
                self.usd_eur_exchange = CurrencyRates().get_rate('USD', 'EUR')

                for i in range(1,int(len(match_caps.groups())),3):
                    current_market_cap = int(float(match_caps.group(i+1)) * self.get_zeros(match_caps.group(i+2)))
                    self.market_caps.loc[match_caps.group(i),'market_cap_in_ounces'] = (current_market_cap*self.usd_eur_exchange)/p_price_per_ounce.loc[match_caps.group(i),'price_in_euro']
                print(self.market_caps)
            
            except ConnectionError:
                print(datetime.datetime.now() + ' PMETALS_DATA_COLLECTOR: Error getting the CurrencyRates USD/EUR, trying again in less than a minute\n' + ConnectionError)
                self.time_in_20_minutes = 0

    def logic(self):

        while True:
            try:
                price_request = requests.get(PMETAL_REQUEST_URL + str(int(time())))

                match_prices = self.prices_pattern.search(price_request.content.decode("utf-8"))
                
                price_per_ounce = pd.DataFrame(columns=['price_in_euro'], index=PMETALS)
                for i in range(1,5):
                    try:
                        price_per_ounce.loc[PMETALS[i-1]] = float(str.replace(str.replace(match_prices.group(i), '.', ''), ',', '.'))
                    except AttributeError:
                        error = datetime.datetime.now() + ' PMETALS_DATA_COLLECTOR: No price found! The response is: ' + price_request.content.decode("utf-8")
                        print(error)
                        with open('pmetalsErrorLog.txt', 'w') as f:
                            f.writelines(error + '\n')
                            f.close()


                if round(time() / (60*20)) > self.time_in_20_minutes:
                    self.time_in_20_minutes = round(time() / (60*20))
                    self.refresh_market_cap(price_per_ounce)
                
                roundedTime = pd.to_datetime(datetime.datetime.now()).floor('min')

                df = pd.DataFrame(columns=['symbol','close','market_cap','starttime'])
                for i in range(4):
                    df = df.append({'symbol' : PMETALS[i],'close': price_per_ounce.loc[PMETALS[i], 'price_in_euro'],'market_cap' : self.market_caps.loc[PMETALS[i],'market_cap_in_ounces']*price_per_ounce.loc[PMETALS[i], 'price_in_euro'] ,'starttime' : roundedTime},ignore_index=True)

                try:              
                    df.to_sql(PMETAL_TABLE_NAME, engine, if_exists='append', index=False)
                except:
                    print(datetime.datetime.now() + " PMETALS_DATA_COLLECTOR: DATABASE UNAVAILABLE SKIPPING WRITE")
                    
                print(df)
                sleep(60 - time() % 60)
            except:
                print("some error happend in  metals data collector")

class LiveDataCollector():

    def __init__(self):
        self.run = True
        metal_collector = MetalCollector()
        self.thread = threading.Thread(name='metal_endless_loop', target=metal_collector.logic)

    def start(self):
        self.thread.start()
        print("Thread started")

    def stop(self):
        if self.thread is not None:
            self.thread.join()

    

