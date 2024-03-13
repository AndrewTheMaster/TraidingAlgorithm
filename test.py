import pandas as pd

def merge_dataframes(df1, df2):
    # Преобразование индексов в тип datetime, если они не являются таковыми
    df1.index = pd.to_datetime(df1.index)
    df2.index = pd.to_datetime(df2.index)
    df2 = df2.add_suffix('_y')

    # Мердж с использованием индексов
    merged_df = pd.concat([df1, df2], axis=1, join='outer')

    # Замена значений из второго фрейма в соответствующих ячейках
    for index, row in df2.iterrows():
        if index in df1.index:
            for column in df2.columns:
                # Проверка, что столбец из df2 является столбцом суффикса '_y'
                if column.endswith('_y'):
                    # Получение соответствующего имени столбца в df1
                    original_column = column[:-2]
                    
                    # Присваивание значения из df2 в соответствующую ячейку df1
                    merged_df.at[index, original_column] = row[column]



    # Выбор столбцов, заканчивающихся на '_y'
    columns_to_drop = merged_df.filter(like='_y').columns

    # Удаление выбранных столбцов
    merged_df = merged_df.drop(columns=columns_to_drop)
    merged_df = merged_df.dropna()
    return merged_df

# Пример использования
# Замените df1 и df2 своими реальными датафреймами
df1 = pd.DataFrame({'value': [1, 2, 3], 'value2': [1, 2, 3]}, index=pd.to_datetime(['2024-01-02', '2024-01-03', '2024-01-05']))


df2 = pd.DataFrame({'value': [4, 5, 6], 'value2': [4, 5, 6]}, index=pd.to_datetime(['2024-01-02', '2024-01-03', '2024-01-04']))

result_df = merge_dataframes(df1, df2)

print(result_df)
