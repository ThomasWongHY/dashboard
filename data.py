import requests 
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import psycopg2
import csv

# Input url and downlaod the .zip file to the folder
# From link: https://www.hkex.com.hk/eng/cbbc/download/dnCSV.asp
# Link for Current Month: https://www.hkex.com.hk/eng/cbbc/download/CBBC11.zip

def download_zip_from_hkex(url, save_name, chunk_size=128):
    r = requests.get(url, stream=True)
    # Write the file as .zip and save as {save_name}
    with open(save_name, 'wb') as output:
        for chunk in r.iter_content(chunk_size=chunk_size):
            output.write(chunk)

datelist=pd.date_range(start="2020-07-01",end="2021-07-01", freq='M')
datelist=[d.strftime("%Y%m") for d in datelist]

for month in datelist:
    print(f"Downloading {month} data")
    download_zip_from_hkex(f'https://www.hkex.com.hk/eng/cbbc/download/CBBC{month[-2:]}.zip', f'CBBC_{month}.zip', chunk_size=128)

cbbc_full=pd.DataFrame()

for month in datelist:
    print('Appending ', month)
    raw=pd.read_csv(f'CBBC_{month}.zip', compression='zip', header=0, sep='\t', encoding='utf-16')
    raw=raw[:-3]
    cbbc_full=cbbc_full.append(raw)


cbbc_full['Bull/Bear']=cbbc_full['Bull/Bear'].str.strip()
# Filter out CBBC that expired
cbbc_full=cbbc_full[cbbc_full['Last Trading Date']!=cbbc_full['Trade Date']]
# Turn date to datetime format
cbbc_full['Trade Date']=cbbc_full['Trade Date'].astype('datetime64[ns]')
# Calucate the relative number of Futures
cbbc_full['future']=(cbbc_full['No. of CBBC still out in market *'])/cbbc_full['Ent. Ratio^']/100*2
# Pivot table of the figures
cbbc_full=cbbc_full.groupby(['Underlying','Trade Date','Bull/Bear'])['future'].sum()['HSI'].to_frame()
df=pd.pivot_table(cbbc_full, values=['future'], index=['Trade Date'],columns=['Bull/Bear'])['future']


# Plot Bull and Bear
plt.plot(df['Bull'], label='Bull')
plt.plot(df['Bear'], label='Bear')
plt.legend()

future=pd.read_csv('FHSI_futu.csv',index_col=0)
# Covert date as datetime
future['Trade Date']=future['time_key'].astype('datetime64[ns]')
future.set_index('Trade Date', inplace=True)
# Calculate Open to Close change of each day
future['o2c_change']=future['close']/future['open']-1


df['f_o2c_change']=future['o2c_change']

# save data as csv
df.to_csv('Raw_data.csv')


def create_rtables():
    #create tables in the PostgreSQL database
    commands = (
        """
        CREATE TABLE rdata (
            trade_date VARCHAR(30) NOT NULL PRIMARY KEY,
            bear VARCHAR(30) NOT NULL,
            bull VARCHAR(30) NOT NULL,
            f_o2c_change VARCHAR(30)
        )
        """)
    # connect to the PostgreSQL server
    conn=psycopg2.connect(
        user="dnddsxgwtueevh",
        password="c6384386c2e42ea38b53e30711401007ff2fd0f4bf3f29b8779e079293004f78",
        host="ec2-3-233-7-12.compute-1.amazonaws.com",
        database="dektg47b7r8tmp"
        )
    cur=conn.cursor()
    
    cur.execute(
        """
        SELECT * FROM information_schema.tables 
        WHERE  table_name='rdata'
        """)
    result=bool(cur.rowcount)
    
    if result: #rdata table exists
        print("Insert New Row in rTable")
        with open('Raw_data.csv', 'r') as f:
            reader = csv.reader(f)
            Rows=list(reader) 
            Tot_rows=len(Rows)-1
            lastrow=Rows[Tot_rows]
            cur.execute(
                "INSERT INTO rdata VALUES (%s, %s, %s, %s)",
                lastrow)

    else:#rdata table does NOT exist
        print("Create rTable")
        with open('Raw_data.csv', 'r') as f:
            reader = csv.reader(f)
            #cur.execute("DROP TABLE IF EXISTS rdata")
            cur.execute(commands)
            for row in reader:
                cur.execute(
                "INSERT INTO rdata VALUES (%s, %s, %s, %s)",
                row
            )

    # commit the changes
    conn.commit()
#Package for making requests to website server
import requests
#Package for turning HTML into readable object
from bs4 import BeautifulSoup as bs
#Decode html object to dict
import demjson
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import datetime as dt
#import timezone package
import pytz
tz = pytz.timezone('Asia/Hong_Kong')
#for plotting
import matplotlib.pyplot as plt
#For downloading stock data from yahoo finance
import yfinance as yf
import psycopg2
import csv

date='20210608'
link=f'https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_{date}c.js?_='
r=requests.get(link)
soup=bs(r.text, 'html.parser')

soup1=soup.text.replace('tabData = ','')
raw=demjson.decode(soup1)

# Default values
market_idx=0
summary={}

# There are 4 Different channel for south/northbound capital: 南北水/深滬
# For each channel, we make a seperate dataframe as their columns are different
for market_idx in range(0,len(raw)):
    tradingday=raw[market_idx]['tradingDay']  # If not trading day, then skip the loop
    if tradingday!=1:
      continue
    market=raw[market_idx]['market']
    date=raw[market_idx]['date']
    summary_table=raw[market_idx]['content'][0]['table']
    summary_col=summary_table['schema'][0]
    summary_col=['Market','Date']+summary_col
    summary_data=[float(summary_table['tr'][i]['td'][0][0].replace(',','')) for i in range(0,len(summary_table['tr']))]
    summary_data=[market,date]+summary_data
    summary_df=pd.DataFrame.from_dict([dict(zip(summary_col,summary_data))])    # Read as Dataframe from dict
    summary[market]=summary_df
    print(market, ' Done.')

summary['SSE Northbound']
summary['SSE Southbound']
summary['SZSE Northbound']
summary['SZSE Southbound']

now_dt=dt.datetime.now(tz)
end_str=now_dt.strftime("%Y-%m-%d")
start_dt=now_dt-timedelta(days=210)   # HKex database on web storged data for about last 200 days
start_str=start_dt.strftime("%Y-%m-%d")

# Create date list as the format of the link required (YYYYMMDD)
datelist=pd.date_range(start=start_str, end=end_str)
datelist=[datetime.strftime(date, '%Y%m%d') for date in datelist]

# Default values
market_idx=0
summary={} 


for date in datelist:
    link=f'https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_{date}c.js?_=1611069994631'
    r=requests.get(link)
    soup=bs(r.text, 'html.parser')
    try:
        soup1=soup.text.replace('tabData = ','')
        raw=demjson.decode(soup1)
    except:
        continue
    for market_idx in range(0,len(raw)):
        tradingday=raw[market_idx]['tradingDay']
        if tradingday!=1:
            continue
        market=raw[market_idx]['market']
        date=raw[market_idx]['date']
        summary_table=raw[market_idx]['content'][0]['table']
        summary_col=summary_table['schema'][0]
        summary_col=['Market','Date']+summary_col
        summary_data=[float(summary_table['tr'][i]['td'][0][0].replace(',','')) for i in range(0,len(summary_table['tr']))]
        summary_data=[market,date]+summary_data
        summary_df=pd.DataFrame.from_dict([dict(zip(summary_col,summary_data))])
        try:
            summary[market]
            summary[market]=summary[market].append(summary_df, ignore_index=True)
        except:
            summary[market]=summary_df

summary['SSE Southbound']

# Save the dict
np.save('southbound.npy', summary)

# Set index as Date for plotting
summary['SSE Southbound'].set_index('Date', inplace=True)
summary['SZSE Southbound'].set_index('Date', inplace=True)

# Download HSI data online
hsi=yf.download('^HSI')
hsi.index=hsi.index.strftime('%Y-%m-%d')
df=pd.DataFrame()
# Calculate net southbound capital flow to HK market
df['net_sse']=summary['SSE Southbound']['Buy Turnover']-summary['SSE Southbound']['Sell Turnover']
df['net_szse']=summary['SZSE Southbound']['Buy Turnover']-summary['SSE Southbound']['Sell Turnover']
df['net_southbound']=df['net_sse']+df['net_szse']
df['net_southbound_cum']=df['net_southbound'].cumsum()    # Cumulative sum of net southbound capital
df['net_southbound_mean']=df['net_southbound'].rolling(3).mean()    # Moving Average of net southbound capital
df['hsi']=hsi['Adj Close']

df.to_csv('sthbd&hsi.csv')

hsi.tail(20)

# Set xtick font size
plt.rc('xtick', labelsize=10)
fig, ax1 = plt.subplots(figsize=(25,10))
ax1.set_ylabel('net_southbound_mean', color='red')
ax1.plot(df['net_southbound_mean'], color='red', label='net_southbound_mean')
# ax1.set_ylabel('net_southbound_cum', color='red')
# ax1.plot(df['net_southbound_cum'], color='red', label='net_southbound_cum')   # un-command it if u want to plot cumulative net southbound capital data
ax1.set_xlabel('Date')
plt.xticks(rotation=90)
ax2 = ax1.twinx()
ax2.set_ylabel('hsi', color='blue')
ax2.plot(df['hsi'], color='blue', label='HSI')

plt.title(f'Southbound Capital vs HSI {end_str}')
ax1.legend(loc='upper left'), ax2.legend(loc='upper right')
fig.tight_layout()

def create_stables():
    #create tables in the PostgreSQL database
    commands = (
        """
        CREATE TABLE sdata (
            date VARCHAR(30) NOT NULL PRIMARY KEY,
            net_sse VARCHAR(30) NOT NULL,
            net_szse VARCHAR(30) NOT NULL,
            net_southbound VARCHAR(30) NOT NULL,
            net_southbound_cum VARCHAR(30) NOT NULL,
            net_southbound_mean VARCHAR(30),
            hsi VARCHAR(30) NOT NULL
        )
        """)
    # connect to the PostgreSQL server
    conn=psycopg2.connect(
        user="dnddsxgwtueevh",
        password="c6384386c2e42ea38b53e30711401007ff2fd0f4bf3f29b8779e079293004f78",
        host="ec2-3-233-7-12.compute-1.amazonaws.com",
        database="dektg47b7r8tmp"
        )
    cur=conn.cursor()

    cur.execute(
        """
        SELECT * FROM information_schema.tables 
        WHERE  table_name='sdata'
        """)
    result=bool(cur.rowcount)
    
    if result: #sdata table exists
        print("Insert New Row in sTable")
        with open('sthbd&hsi.csv', 'r') as f:
            reader = csv.reader(f)
            Rows=list(reader) 
            Tot_rows=len(Rows)-1
            lastrow=Rows[Tot_rows]
            cur.execute("INSERT INTO sdata VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        lastrow)

    else: #sdata table does NOT exist
        print("Create sTable")
        with open('sthbd&hsi.csv', 'r') as f:
            reader = csv.reader(f)
            #cur.execute("DROP TABLE IF EXISTS sdata")
            cur.execute(commands)
            for row in reader:
                cur.execute(
                "INSERT INTO sdata VALUES (%s, %s, %s, %s, %s, %s, %s)",
                row
            )

    # commit the changes
    conn.commit()

if __name__ == '__main__':
    create_rtables()
    create_stables()
