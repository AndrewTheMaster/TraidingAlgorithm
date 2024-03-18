import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as pl
import csv
def merge_dataframes(df1, df2):
    # Преобразование индексов в тип datetime, если они не являются таковыми
    #df1.index = pd.to_datetime(df1.index)
    #df2.index = pd.to_datetime(df2.index)
    df2 = df2.add_suffix('_y')

    # Мердж с использованием индексов
    merged_df = pd.concat([df1, df2], axis=1, join='outer')
    
    # Замена значений из второго фрейма в соответствующих ячейках
    print(merged_df)
    for index, row in df2.iterrows():
        date_value = row['Date_y']  # Значение даты из второго DataFrame
        corresponding_row = df1[df1['Date'] == date_value]  # Находим соответствующую строку в первом DataFrame по значению даты из второго DataFrame
        if not corresponding_row.empty:
            for column in df2.columns:
                if column.endswith('_y'):
                    original_column = column[:-2]  # Получаем имя соответствующего столбца в первом DataFrame
                    merged_df.at[corresponding_row.index[0], original_column] = row[column]  # Присваиваем значение из второго DataFrame в ячейку первого DataFrame


    print(merged_df)

    # Выбор столбцов, заканчивающихся на '_y'
    columns_to_drop = merged_df.filter(like='_y').columns

    # Удаление выбранных столбцов
    
    merged_df = merged_df.drop(columns=columns_to_drop)
    merged_df = merged_df.dropna()
    merged_df = merged_df.sort_values(by='Date')
    return merged_df
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

def getCandlesHeikenAshi(symb, tf, limit):
    url = 'https://api.bybit.com'
    path = '/v5/market/kline'
    URL = url + path

    params = {'category': 'spot', 'symbol': symb, 'interval': tf, 'limit': limit}
    r = requests.get(URL, params=params)
    df = pd.DataFrame(r.json()['result']['list'])
    print("data")
    print(df)
    file_path = "CandlesHeikenAshi.csv"

    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        CSVdf = pd.read_csv(file_path, encoding='utf-8')
        print("last_row")
        last_row = CSVdf.tail(1)
        print(last_row)
        #print(last_row.loc[291, 'Open'])
        print("last_row")

        # Преобразование в хайкен-аши
        m = pd.DataFrame()
        m['Date'] = df.iloc[:, 0].astype(np.int64)
        m['Date'] = pd.to_datetime(m['Date'], unit='ms')
        m['Close'] = (df.iloc[:, 1].astype(float) + df.iloc[:, 2].astype(float) + df.iloc[:, 3].astype(float) + df.iloc[:, 4].astype(float)) / 4.0
        m['Open'] = (m['Close'].shift(1) + m['Close'].shift(1) + m['Close'].shift(1) + m['Close'].shift(1)) / 4.0
        m['High'] = df.iloc[:, 2].astype(float)
        m['Low'] = df.iloc[:, 3].astype(float)
        m['Volume'] = df.iloc[:, 5].astype(float)
        m = m.sort_values(by='Date')

        # Найдем дату в DataFrame m, соответствующую дате в последней строке CSVdf
        start_date = last_row['Date'].iloc[0]
        print(start_date)

        important_index =  last_row.index
        print("important_index")
        print(important_index)
        print("important_index")
        print(m.columns)
        start_index = m[m['Date'] == pd.to_datetime(start_date)].index[0]
        print(start_index)
        # Начинаем с найденного индекса в DataFrame m
        for index, row in m.iloc[limit-start_index:].iterrows():
            # ваш код обработки каждой строки
            print(index)
            if index+1 == start_index:
                m.loc[index, 'Open'] = (last_row['Open'].iloc[0] + last_row['Close'].iloc[0]) / 2.0

                print((last_row['Open'].iloc[0] + last_row['Close'].iloc[0]) / 2.0)
            else:
                m.loc[index, 'Open'] = (m.loc[index + 1, 'Open'] + m.loc[index + 1, 'Close']) / 2.0

            print(m.loc[index, 'Open'])
        m['High'] = m.iloc[:, 1:5].max(axis=1)
        m['Low'] = m.iloc[:, 1:5].min(axis=1)
        CSVdf['Date'] = pd.to_datetime(CSVdf['Date'])
        print("m")
        print(m)
        print("CSVdf")
        print(CSVdf)


        m = merge_dataframes(m, CSVdf)
        print("merged_m")
        print(m)
 
    

        

        #m_without_last_row = m.iloc[:-1]  # Исключаем последнюю строку
        m_without_last_row = m
        m_without_last_row.to_csv("CandlesHeikenAshi.csv", index=False, mode='w')
    
        m_without_last_row =  m_without_last_row[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]


    return  m_without_last_row

    
    


 
   
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
    #print(shortBoxes)
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
    #вхождение в одинарный шортбокс
    if (len(shortBoxes)>0):
         for i in range(0, len(shortBoxes)):
          
             sbox=df.index.get_loc(shortBoxes[i]['prod'])
             
             top = df.iloc[sbox]['High']
         
             bot = df.iloc[sbox]['Low']
          
             for index in range(sbox, len(df)):
                high = df.iloc[index]['High']
                if (not(high < bot) ):
                    print(f"Alert: Price inside Bearish OB at {df.index[index ]} ")
                    #first pattern
                    if (0.975>=(df.iloc[index]['Open'] - df.iloc[index]['Close'])/(df.iloc[index-1]['Close']-df.iloc[index-1]['Low'])>=1.025)   and (df.iloc[index-1]['Close']-df.iloc[index-1]['Low'])!=0:#Последняя свеча поглощает вторую
                        if (df.iloc[index-1]['Open'] - df.iloc[index-1]['Low'])/(df.iloc[index-1]['Close'] - df.iloc[index-1]['Open'])>=3   and (df.iloc[index-1]['Close'] - df.iloc[index-1]['Open'])!=0: #Проверка второй свечи на то что она пинбар
                            if index>2 and (0.975>=(df.iloc[index-2]['High']-df.iloc[index-2]['Close'])/ (df.iloc[index-2]['Open']-df.iloc[index-2]['Low'])>=1.025)  and (df.iloc[index-2]['Open']-df.iloc[index-2]['Low'])>0:#Проверка на первую свечу в паттерне на одинаковые хвосты
                                print("yesssssssss! first Pattern")
                    #third pattern
                    coin = 0
                    for i in range(1,7):
                        if (df.iloc[index]['Volume']>(df.iloc[index-i]['Volume']*4)):
                            coin+=1
                    if coin > 3:
                        print("yesssssssss! third Pattern")
                    #fourth pattern 
                    for i in range(0,20):
                        k_coin=0
                        ArcTop =0
                        if (df.iloc[index-i]['High']>df.iloc[index-(i+1)]['High']):
                            k_coin+=1
                        if (df.iloc[index-i]['High']<df.iloc[index-(i+1)]['High']) and (k_coin==i or ArcTop==1):
                            if k_coin==i:
                                    ArcTop=1
                            if  (not(df.iloc[index-(i+1)]['High']<bot)):
                                print("yesssssssss! fourth Pattern")
                        else: break
    #вхождение в двойной шортбокс
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
                    #first pattern
                    if (0.975>=(df.iloc[index]['Open'] - df.iloc[index]['Close'])/(df.iloc[index-1]['Close']-df.iloc[index-1]['Low'])>=1.025)   and (df.iloc[index-1]['Close']-df.iloc[index-1]['Low'])!=0:#Последняя свеча поглощает вторую
                        if (df.iloc[index-1]['Open'] - df.iloc[index-1]['Low'])/(df.iloc[index-1]['Close'] - df.iloc[index-1]['Open'])>=3   and (df.iloc[index-1]['Close'] - df.iloc[index-1]['Open'])!=0: #Проверка второй свечи на то что она пинбар
                            if index>2 and (0.975>=(df.iloc[index-2]['High']-df.iloc[index-2]['Close'])/ (df.iloc[index-2]['Open']-df.iloc[index-2]['Low'])>=1.025)  and (df.iloc[index-2]['Open']-df.iloc[index-2]['Low'])>0:#Проверка на первую свечу в паттерне на одинаковые хвосты
                                print("yesssssssss! first Pattern")
                    #third pattern
                    coin = 0
                    for i in range(1,7):
                        if (df.iloc[index]['Volume']>(df.iloc[index-i]['Volume']*4)):
                            coin+=1
                    if coin > 3:
                        print("yesssssssss! third Pattern")
                #second pattern
                if (not(high < bot) ):
                    # если в открытый ордерблок попала свеча 
                    for i in range(1,10):
                        # если все свечи зеленые
                        if((df.iloc[sbox-i]['Open']-df.iloc[sbox-i]['Close'])<0):
                            k_coin=0
                            trigger = 0
                            for k in range(i-1, 0, -1):
                                # если они прячутся в тени большой свечи
                                if(df.iloc[sbox-i]['High']<=df.iloc[sbox-k]['High']):
                                    k_coin+=1
                                    if (not(df.iloc[sbox-k]['High'] < prev_bot) ):
                                        trigger = 1
                                
                                #если все свечи были зеленые и нашлась такая что смогла спрятать все предыдущие зеленые свечи и у этих свечей была индикация
                                if(k_coin==(i-1)) and trigger == 1:
                                    #то проверяю на пулбэк
                                    if (abs((df.iloc[sbox-(i+1)]['Close']-df.iloc[sbox-(i+1)]['Open']))*3<= (df.iloc[sbox-(i+1)]['High']-df.iloc[sbox-(i+1)]['Open']) and  (abs(df.iloc[sbox-(i+1)]['Close']-df.iloc[sbox-(i+1)]['Open']))*3<= (df.iloc[sbox-(i+1)]['Close']-df.iloc[sbox-(i+1)]['Low'])):
                                    
                                        print("yesssssssss! second Pattern")
                        else:
                            break#попалась красная свеча
                    
                    
    
                    
             

    # # Оповещения для бычьих блоков
    #вхождение в одинарный лонгбокс
    if (len(longBoxes) > 0):
        for i in range(0, len(longBoxes)):
            
             sbox=df.index.get_loc(longBoxes[i]['prod'])
             
             top = df.iloc[sbox]['High']
             
             bot = df.iloc[sbox]['Low']
             
             for index in range(sbox, len(df)):
                low = df.iloc[index]['Low']
                if (not(low>top)):
                    print(f"Alert: Price inside Bullish OB at {df.index[index ]}")
                    #first pattern
                    if (0.975>=(df.iloc[index]['Close'] - df.iloc[index]['Open'])/(df.iloc[index-1]['High']-df.iloc[index-1]['Close'])>=1.025)   and (df.iloc[index-1]['High']-df.iloc[index-1]['Close'])!=0:#Последняя свеча поглощает вторую
                        if (df.iloc[index-1]['High'] - df.iloc[index-1]['Open'])/(df.iloc[index-1]['Open'] - df.iloc[index-1]['Close'])>=3   and (df.iloc[index-1]['Open'] - df.iloc[index-1]['Close'])!=0: #Проверка второй свечи на то что она пинбар
                            if index>2 and (0.975>=(df.iloc[index-2]['Close']-df.iloc[index-2]['Low'])/ (df.iloc[index-2]['High']-df.iloc[index-2]['Open'])>=1.025)  and (df.iloc[index-2]['High']-df.iloc[index-2]['Open'])>0:#Проверка на первую свечу в паттерне на одинаковые хвосты
                                print("yesssssssss! first Pattern")
                    #third pattern
                    coin = 0
                    for i in range(1,7):
                        if (df.iloc[index]['Volume']>(df.iloc[index-i]['Volume']*4)):
                            coin+=1
                    if coin > 3:
                        print("yesssssssss! third Pattern")
                    #fourth pattern 
                    for i in range(0,20):
                        k_coin=0
                        ArcTop =0
                        if (df.iloc[index-i]['Close']<df.iloc[index-(i+1)]['Close']):
                            k_coin+=1
                        if (df.iloc[index-i]['Close']>df.iloc[index-(i+1)]['Close']) and (k_coin==i or ArcTop==1):
                            if k_coin==i:
                                    ArcTop=1
                            if  (not(df.iloc[index-(i+1)]['Low']>top)):
                                print("yesssssssss! fourth Pattern")
                        else: break

    #вхождение в Двойной лонгбокс
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
                    #first pattern
                    if (0.975>=(df.iloc[index]['Close'] - df.iloc[index]['Open'])/(df.iloc[index-1]['High']-df.iloc[index-1]['Close'])>=1.025)   and (df.iloc[index-1]['High']-df.iloc[index-1]['Close'])!=0:#Последняя свеча поглощает вторую
                        if (df.iloc[index-1]['High'] - df.iloc[index-1]['Open'])/(df.iloc[index-1]['Open'] - df.iloc[index-1]['Close'])>=3   and (df.iloc[index-1]['Open'] - df.iloc[index-1]['Close'])!=0: #Проверка второй свечи на то что она пинбар
                            if index>2 and (0.975>=(df.iloc[index-2]['Close']-df.iloc[index-2]['Low'])/ (df.iloc[index-2]['High']-df.iloc[index-2]['Open'])>=1.025)  and (df.iloc[index-2]['High']-df.iloc[index-2]['Open'])>0:#Проверка на первую свечу в паттерне на одинаковые хвосты
                                print("yesssssssss! first Pattern")
                    #third pattern
                    coin = 0
                    for i in range(1,7):
                        if (df.iloc[index]['Volume']>(df.iloc[index-i]['Volume']*4)):
                            coin+=1
                    if coin > 3:
                        print("yesssssssss! third Pattern")
                #second pattern
                if (not(low>top)):
                    # если в открытый ордерблок попала свеча 
                    for i in range(1,10):
                        # если все свечи красные
                        if((df.iloc[sbox-i]['Open']-df.iloc[sbox-i]['Close'])>0):
                            k_coin=0
                            trigger = 0
                            for k in range(i-1, 0, -1):
                                # если они прячутся в тени большой свечи
                                if(df.iloc[sbox-i]['High']>=df.iloc[sbox-k]['High']):
                                    k_coin+=1
                                    if (not(df.iloc[sbox-k]['Low'] < prev_top) ):
                                        trigger = 1
                                
                                #если все свечи были зеленые и нашлась такая что смогла спрятать все предыдущие зеленые свечи и у этих свечей была индикация
                                if(k_coin==(i-1)) and trigger == 1:
                                    #то проверяю на пулбэк
                                    if ((abs(df.iloc[sbox-(i+1)]['Open']-df.iloc[sbox-(i+1)]['Close']))*3<= (df.iloc[sbox-(i+1)]['High']-df.iloc[sbox-(i+1)]['Open']) and  (abs(df.iloc[sbox-(i+1)]['Open']-df.iloc[sbox-(i+1)]['Close']))*3<= (df.iloc[sbox-(i+1)]['Close']-df.iloc[sbox-(i+1)]['Low'])):
                                    
                                        print("yesssssssss! second Pattern")
                        else:
                            break#попалась зеленая свеча
    
                    


                    


    df.to_csv('output111.csv')
    return df
df = getCandles('BTCUSDT', '30',300)
print(df)
print(getAlert(df, '30min', 'Close', 28))
dft = getCandlesHeikenAshi('BTCUSDT', '30',300)
print("CandlesHeikenAshi")
print(dft)
print(getAlert(dft, '30min', 'Close', 28))
#for index, row in df.iterrows():
    #ob_created_bear = False
    #ob_created_bull = False





