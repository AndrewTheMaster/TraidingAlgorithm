import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as pl

def getCandles(symb, tf):
    url = 'https://api.bybit.com'
    path = '/v5/market/kline'
    URL = url + path
    params = {'category': 'spot', 'symbol': symb, 'interval': tf}
    r = requests.get(URL, params=params)
    df = pd.DataFrame(r.json()['result']['list'])
    m = pd.DataFrame()
    m['Date'] = df.iloc[:, 0].astype(np.int64)
    m['Date'] = pd.to_datetime(m['Date'], unit='ms')
    m['Open'] = df.iloc[:, 1].astype(float)
    m['High'] = df.iloc[:, 2].astype(float)
    m['Low'] = df.iloc[:, 3].astype(float)
    m['Close'] = df.iloc[:, 4].astype(float)
    m['Volume'] = df.iloc[:, 5].astype(float)
    m = m.sort_values(by='Date')
    m.to_csv('output.csv', index=False)
    return m

def getAlert(df, tf):
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)  # Устанавливаем индекс в формате DatetimeIndex
    df.index.freq = tf  # Установка частоты в минутах
    # Initialize variables

    shortBoxes = []
    longBoxes = []
    # Параметры для создания блоков заявок
    sens = 28
    sens /= 100


    # Создание блоков заявок
    df['pc'] = (df['Open'] - df['Open'].shift(4)) / df['Open'].shift(4) * 100
    #print(df['pc'])

    df['ob_created'] = (df['pc'] < -sens) & (df['pc'].shift(1) >= -sens)
    df['ob_created_bull'] = (df['pc'] > sens) & (df['pc'].shift(1) <= sens)

    # Создайте столбец last_cross_index с начальными значениями None
    df['last_cross_index'] = None
    df['last_cross_index_bull'] = None

    # Используйте векторизованные операции для установки значений в столбец
    condition1 = (df['ob_created']).values
    df.loc[condition1, 'last_cross_index'] = 0
    condition2 = (df['ob_created_bull']).values
    df.loc[condition2, 'last_cross_index_bull'] = 0


    df.loc[df['last_cross_index'].isnull(), 'last_cross_index'] = df['last_cross_index'].shift(1) + 1
    df.loc[df['last_cross_index'].isnull(), 'last_cross_index'] = df['last_cross_index'].shift(1) + 1
    df.loc[df['last_cross_index'].isnull(), 'last_cross_index'] = df['last_cross_index'].shift(1) + 1
    df.loc[df['last_cross_index'].isnull(), 'last_cross_index'] = df['last_cross_index'].shift(1) + 1

    df.loc[df['last_cross_index_bull'].isnull(), 'last_cross_index_bull'] = df['last_cross_index_bull'].shift(1) + 1
    df.loc[df['last_cross_index_bull'].isnull(), 'last_cross_index_bull'] = df['last_cross_index_bull'].shift(1) + 1
    df.loc[df['last_cross_index_bull'].isnull(), 'last_cross_index_bull'] = df['last_cross_index_bull'].shift(1) + 1
    df.loc[df['last_cross_index_bull'].isnull(), 'last_cross_index_bull'] = df['last_cross_index_bull'].shift(1) + 1

    ob_created_df = df[df['ob_created'] & df['last_cross_index'].shift(1).isnull()]
    ob_created_bull_df = df[df['ob_created_bull'] & df['last_cross_index_bull'].shift(1).isnull()]
    #print(ob_created_df)
    #print(ob_created_bull_df)

    bearish_dates = ob_created_df.index
    bullish_dates = ob_created_bull_df.index
    print(bearish_dates)
    print(bullish_dates)

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
    print(shortBoxes)
    print(longBoxes)
    # # Оповещения для медвежьих блоков
    # for i in range(1, len(ob_created_df)):
    #     if (ob_created_df['Close'].iloc[i] > ob_created_df['High'].iloc[i - 1]) and \
    #             (ob_created_df['Close'].iloc[i] > ob_created_df['Low'].iloc[i - 1]) and \
    #             abs(ob_created_df['High'].iloc[i] - ob_created_df['Low'].iloc[i - 1]) < 0.0001:
    #         print(f"Alert: Price inside Double Bearish OB at {ob_created_df.index[i]}")
    #
    # # Оповещения для бычьих блоков
    # for i in range(1, len(ob_created_bull_df)):
    #     if (ob_created_bull_df['Close'].iloc[i] < ob_created_bull_df['Low'].iloc[i - 1]) and \
    #             (ob_created_bull_df['Close'].iloc[i] < ob_created_bull_df['High'].iloc[i - 1]) and \
    #             abs(ob_created_bull_df['High'].iloc[i] - ob_created_bull_df['Low'].iloc[i - 1]) < 0.0001:
    #         print(f"Alert: Price inside Double Bullish OB at {ob_created_bull_df.index[i]}")
    df.to_csv('output11.csv')
    return df
df = getCandles('BTCUSDT', '30')
print(df)
print(getAlert(df, '30min'))
#for index, row in df.iterrows():
    #ob_created_bear = False
    #ob_created_bull = False





