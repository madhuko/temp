#%% 
import pandas as pd
import datetime,time,requests
from io import StringIO as sio

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
def get_latest_fs():
    s=requests.Session()
    s.headers.update(header)
    s.get("https://nepsealpha.com/trading/chart")
    df=pd.DataFrame()
    live_market=pd.read_html("https://www.merolagani.com/LatestMarket.aspx")[0]
    for sym in live_market["Symbol"]:
        if "/" in sym:
            continue
        try:
          ram=getfs_nepsealpha(sym,s)
        except:
          s=requests.Session()
          s.headers.update(header)
          s.get("https://nepsealpha.com/trading/chart")
          ram=getfs_nepsealpha(sym,s)
        df=pd.concat([df,ram])
        print("Collected data of {}".format(sym))
    df.columns='contract buyer seller qty rate amt symbol'.split()
    df["amt"]=df["amt"].apply(lambda x: x.replace("NPR",""))
    df["amt"]=df["amt"].apply(lambda x: x.replace(",",""))
    df.amt=df.amt.astype(float)
    df.buyer=df.buyer.astype(str)
    df.seller=df.seller.astype(str)
    df["rate"]=df["rate"].apply(lambda x: x.replace("NPR ",""))
    return df

def getfs_nepsealpha(symbol,s):
    fs=s.get("https://nepsealpha.com/floorsheet_ajx/{}/index".format(symbol))
    new_df=pd.read_html(sio(fs.json()['html']))
    new_df[1]["Symbol"]=symbol
    return new_df[1]

# %%
init_ohlc=chart_data(provider='merolaganida',symbol="NEPSE",fromtime=datetime.date(2022,1,1),totime=datetime.datetime.now(),resolution="1D")
init_ohlc.t=init_ohlc.t.dt.date
dater0=init_ohlc.t.iloc[-1]
# %%
try:
    print(dater0)
#     requests.get("https://raw.githubusercontent.com/madhuko/temp/main/fs/{}".format(dater0)).reason=='OK'
    pd.read_csv("https://raw.githubusercontent.com/madhuko/temp/main/fs/{}".format(dater0))
    print("Data is already there")
except:
    df=get_latest_fs()
    df.to_csv("fs/"+ str(dater0),index=False)

    

# %%
