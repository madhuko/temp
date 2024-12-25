#%% 
import pandas as pd
import datetime,time,requests,json
import os
# from io import StringIO as sio

header={
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36 Edg/90.0.818.66',
    'Accept': '*/*',
    'content-type': 'application/json',
    'Accept-Encoding': 'gzip, deflate, br',
    }

chart_provider={
'nepsechart':'https://ohlcv.nepsechart.com/history?symbol={symbol}&resolution={resolution}&from={fromtime}&to={totime}',
'merocapital':'https://chartdata.merocapital.com/datafeed1/history?symbol={symbol}&resolution={resolution}&from={fromtime}&to={totime}',
'merolaganida':'https://da.merolagani.com/handlers/TechnicalChartHandler.ashx?type=get_advanced_chart&symbol={symbol}&resolution={resolution}&rangeStartDate={fromtime}&rangeEndDate={totime}&from=&isAdjust=1&currencyCode=NPR',
'merolagani':'https://www.merolagani.com/handlers/TechnicalChartHandler.ashx?type=get_advanced_chart&symbol={symbol}&resolution={resolution}&rangeStartDate={fromtime}&rangeEndDate={totime}&from=&isAdjust=1&currencyCode=NPR',
'nepsealpha':'https://nepsealpha.com/trading/1/history?symbol={symbol}&resolution={resolution}&from={fromtime}&to={totime}&currencyCode=NRS',
'nepsedata':'https://nepsedata.com/history?symbol={symbol}&resolution={resolution}&from={fromtime}&to={totime}&currencyCode=NRS'
}
def chart_data(provider='nepsealpha',symbol="NEPSE",fromtime=datetime.date(2019,1,1),totime=datetime.datetime.now(),resolution="1D"):
    """returns data fetched from nepsealpha,merocapital,merolagani and nepsechart.\nwarning!! \nNepsechart,merocapital takes D for daily resolution.\nMerocapital,nepsedata gives unadjusted chart)"""
    fromtime =int(time.mktime(fromtime.timetuple()))
    totime =int(time.mktime(totime.timetuple())) 
    url=chart_provider.get(provider)
    print(url.format(symbol=symbol,resolution=resolution,fromtime=fromtime,totime=totime))
    df=pd.read_json(url.format(symbol=symbol,resolution=resolution,fromtime=fromtime,totime=totime))
    df['t']=df['t'].apply(datetime.datetime.utcfromtimestamp)
    df.drop('s',axis=1,inplace=True)
    return df

def get_fs_chukul(dater):
  df= requests.get(f"https://chukul.com/api/data/floorsheet/bydate/?date={dater}")
  df2=pd.read_json(json.dumps(df.json()))
  df3=df2["transaction	symbol	buyer	seller	quantity	rate	amount".split()]
  df3.columns="contract,symbol,buyer,seller,qty,rate,amt".split(",")
  return df3

# %%
init_ohlc=chart_data(provider='merolaganida',symbol="NEPSE",fromtime=datetime.date(2022,1,1),totime=datetime.datetime.now(),resolution="1D")
init_ohlc.t=init_ohlc.t.dt.date
dater0=init_ohlc.t.iloc[-1]
year = dater0.year
# %%
try:
    # print(dater0)
#     requests.get("https://raw.githubusercontent.com/madhuko/temp/main/fs/{}".format(dater0)).reason=='OK'
    pd.read_csv(f"https://raw.githubusercontent.com/madhuko/temp/main/fs/{year}/{dater0}")
    print("Data is already there")
except:
    df=get_fs_chukul(dater0)
    save_folder = f"fs/{year}"
    os.makedirs(save_folder, exist_ok=True)
    save_file_path = f"{save_folder}/{dater0}"
    df.to_csv(save_file_path, index=False)
    print(f"Data saved to {save_file_path}")


# %%
