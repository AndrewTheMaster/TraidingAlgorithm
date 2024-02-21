import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as pl

def getCandles(symb, tf, limit):
    url = 'https://api.bybit.com'
    path = '/v5/market/kline'
    URL = url + path

    params = {'category': 'spot', 'symbol': symb, 'interval': tf, 'limit': limit}
    r = requests.get(URL, params=params)
    df = pd.DataFrame(r.json()['result']['list'])

    # Обработка данных как в вашем коде
    m = pd.DataFrame()
    m['Date'] = df.iloc[:, 0].astype(np.int64)
    m['Date'] = pd.to_datetime(m['Date'], unit='ms')
    m['Open'] = df.iloc[:, 1].astype(float)
    m['High'] = df.iloc[:, 2].astype(float)
    m['Low'] = df.iloc[:, 3].astype(float)
    m['Close'] = df.iloc[:, 4].astype(float)
    m['Volume'] = df.iloc[:, 5].astype(float)
    m = m.sort_values(by='Date')

    return m
def getAlert(df, tf, OBMitigationType, sens):
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)  # Устанавливаем индекс в формате DatetimeIndex
    df.index.freq = tf  # Установка частоты в минутах
    # Initialize variables

    shortBoxes = []
    longBoxes = []
    # Параметры для создания блоков заявок
    
    sens /= 100


    # Создание блоков заявок
    df['pc'] = (df['Open'] - df['Open'].shift(4)) / df['Open'].shift(4) * 100
    #print(df['pc'])

    df['ob_created'] = (df['pc'] < -sens) & (df['pc'].shift(1) >= -sens)
    df['ob_created_bull'] = (df['pc'] > sens) & (df['pc'].shift(1) <= sens)

    # Создайте столбец last_cross_index с начальными значениями None
    df['last_cross_index'] = None
    #df['last_cross_index_bull'] = None

    # Используйте векторизованные операции для установки значений в столбец
    condition1 = (df['ob_created']).values
    df.loc[condition1, 'last_cross_index'] = 0
    condition2 = (df['ob_created_bull']).values
    df.loc[condition2, 'last_cross_index'] = 0


    df.loc[df['last_cross_index'].isnull(), 'last_cross_index'] = df['last_cross_index'].shift(1) + 1
    df.loc[df['last_cross_index'].isnull(), 'last_cross_index'] = df['last_cross_index'].shift(1) + 1
    df.loc[df['last_cross_index'].isnull(), 'last_cross_index'] = df['last_cross_index'].shift(1) + 1
    df.loc[df['last_cross_index'].isnull(), 'last_cross_index'] = df['last_cross_index'].shift(1) + 1

    ob_created_df = df[df['ob_created'] & df['last_cross_index'].shift(1).isnull()]
    ob_created_bull_df = df[df['ob_created_bull'] & df['last_cross_index'].shift(1).isnull()]
    #print(ob_created_df)
    #print(ob_created_bull_df)

    bearish_dates = ob_created_df.index
    bullish_dates = ob_created_bull_df.index

    for date in bearish_dates:
        index_of_date = df.index.get_loc(date)
        for i in range(4, 16):
            if 0 <= index_of_date - i < len(df):
                if (df['Close'].iloc[index_of_date-i] > df['Open'].iloc[index_of_date-i]):
                    shortBox = {
                       # 'top': df['High'].iloc[index_of_date + i],
                        #'bottom': df['Low'].iloc[index_of_date + i],
                        'orig': df.index[index_of_date],
                        'prod': df.index[index_of_date -i],
                        #'date': df['Date'].iloc[index_of_date + i].astype(np.int64)
                    }
                    shortBoxes.append(shortBox)
                    break

    for date in bullish_dates:
        index_of_date = df.index.get_loc(date)
        for i in range(4, 16):
            if 0 <= index_of_date - i < len(df):
            #print(df['Close'].shift(i).iloc[index_of_date])
                if (df['Close'].iloc[index_of_date-i] < df['Open'].iloc[index_of_date-i]):
                    longBox = {
                        #'top': df['High'].iloc[index_of_date + i],
                        #'bottom': df['Low'].iloc[index_of_date + i],
                        'orig': df.index[index_of_date ],
                        'prod': df.index[index_of_date -i],
                        #'date': df['Date'].iloc[index_of_date + i].astype(np.int64)
                    }
                    longBoxes.append(longBox)
                    #print("222222222222222222222")
                    #print(df.shift(i).iloc[last_red])

                    break
   
  
    df['OBBullMitigation'] = np.where(OBMitigationType == 'Close', df['Close'].shift(1), df['Low'])
    df['OBBearMitigation'] = np.where(OBMitigationType == 'Close', df['Close'].shift(1), df['High'])
    #shortBoxes['prod'] = pd.to_datetime(shortBoxes['prod'])
    #condition = (df['OBBearMitigation'] > df['High']) & (df.index.isin(shortBoxes['prod']))
    #shortBoxes = shortBoxes[~condition]
    
    newLongBoxes = []

    for date in longBoxes:
        start_index = df.index.get_loc(date['prod'])
        keep_date = True

        for index in range(start_index, len(df)):

            if df.iloc[index]['OBBullMitigation'] < df.iloc[start_index]['Low']:
                keep_date = False
                break
        
        if keep_date:
            newLongBoxes.append(date)

    longBoxes = newLongBoxes

    newShortBoxes = []

    for date in shortBoxes:
        start_index = df.index.get_loc(date['prod'])
        keep_date = True

        for index in range(start_index, len(df)):
            if df.iloc[index]['OBBearMitigation'] > df.iloc[start_index]['High']:
                keep_date = False
                
                break
        
        if keep_date:
            newShortBoxes.append(date)


    shortBoxes = newShortBoxes


   
    print("shortboxes")
    print(shortBoxes)
    print("longboxes")
    print(longBoxes)


    # # Оповещения для медвежьих блоков
    if (len(shortBoxes)>1):
         for i in range(0, len(shortBoxes)-1):
             """print("shortBoxes(i)")
             print(shortBoxes[i]['prod'])
             print("shortBoxes(i+1)")
             print(shortBoxes[i+1]['prod'])"""
             sbox=df.index.get_loc(shortBoxes[i]['prod'])
             prev_sbox=df.index.get_loc(shortBoxes[i+1]['prod'])
             top = df.iloc[sbox]['High']
             prev_top = df.iloc[prev_sbox]['High']
             bot = df.iloc[sbox]['Low']
             prev_bot = df.iloc[prev_sbox]['Low']
             """print("top")
             print(top)
             print("prev_top")
             print(prev_top)
             print("bot")
             print(bot)
             print("prev_bot")
             print(prev_bot)"""
             for index in range(prev_sbox, len(df)):
                high = df.iloc[index]['High']
                if (not(high < bot) and not(high < prev_bot) and ((bot - prev_top)<0 or (prev_bot - top)<0)):
                 print(f"Alert: Price inside Double Bearish OB at {df.index[index ]}")

    # # Оповещения для бычьих блоков
    if (len(longBoxes) > 1):
        for i in range(0, len(longBoxes)-1):
             """print("longBoxes(i)")
             print(longBoxes[i]['prod'])
             print("longBoxes(i+1)")
             print(longBoxes[i+1]['prod'])"""
             sbox=df.index.get_loc(longBoxes[i]['prod'])
             prev_sbox=df.index.get_loc(longBoxes[i+1]['prod'])
             top = df.iloc[sbox]['High']
             prev_top = df.iloc[prev_sbox]['High']
             bot = df.iloc[sbox]['Low']
             prev_bot = df.iloc[prev_sbox]['Low']
             """print("top")
             print(top)
             print("prev_top")
             print(prev_top)
             print("bot")
             print(bot)
             print("prev_bot")
             print(prev_bot)"""
             for index in range(prev_sbox, len(df)):
                low = df.iloc[index]['Low']
                if (not(low>top) and not(low >prev_top) and ((bot - prev_top)<0 or (prev_bot - top)<0)):
                    print(f"Alert: Price inside Double Bullish OB at {df.index[index ]}")
    df.to_csv('output111.csv')
    return df
df = getCandles('BTCUSDT', '30',300)
print(df)
print(getAlert(df, '30min', 'Close', 28))
#for index, row in df.iterrows():
    #ob_created_bear = False
    #ob_created_bull = False





