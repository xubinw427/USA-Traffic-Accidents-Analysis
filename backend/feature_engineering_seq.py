import os
import numpy as np
import pandas as pd
from sklearn.utils import resample
import math
import random


rand_seed = 233
analysis_flag = True


def load_data(dir, file0, file1):
    df_label_0 = pd.read_csv(os.path.join(dir, file0), encoding='latin1')
    df_label_1 = pd.read_csv(os.path.join(dir, file1), encoding='latin1')
    if analysis_flag:
        print(df_label_0['Start-hour2'].head(10))
        print(df_label_1['Start-hour2'].head(10))
    df_label_0['Start-hour2'] = pd.to_datetime(df_label_0['Start-hour2'], format='%Y-%m-%d %H:%M:%S')
    df_label_1['Start-hour2'] = pd.to_datetime(df_label_1['Start-hour2'], format='%m/%d/%Y %H:%M')
    # df_label_1['Start-hour2'] = pd.to_datetime(df_label_1['Start-hour2'], format='%Y-%m-%d %H:%M:%S')
    df_binary = pd.concat([df_label_0, df_label_1])
    return df_binary


if __name__ == "__main__":
    year = 2016
    df_binary = load_data('E:\\data', f'{year}_fake_data.csv', f'{year}_real_data.csv')
    traffic_columns = ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
    selected_columns = (['Start-hour2', 'Temperature', 'Humidity', 'Condition'] +
                        traffic_columns + ['Start_Lat', 'Start_Lng', 'label'])
    df_selected = df_binary[selected_columns]
    df_filtered = df_selected.dropna()
    numeric_columns = ['Temperature', 'Humidity']
    # filter 1000
    tolerance = 1e-6
    for col in numeric_columns:
        df_filtered = df_filtered[~np.isclose(df_filtered[col], 1000.0, atol=tolerance)]

    # reset index很重要
    df_sorted = df_filtered.sort_values(by=['Start_Lat', 'Start_Lng', 'Start-hour2']).reset_index(drop=True)
    # too slow
    '''
    index_0 = []
    index_1 = []
    for index, row in df_sorted.iterrows():
        if index > 1:
            if count_all % 1000 == 0:
                print(count_all)
            prev_rows = df_sorted.iloc[index - 2:index + 1]
            time_diffs = prev_rows['Start-hour2'].diff().dt.total_seconds() / 3600
            time_diffs = time_diffs[1:]  # remove the first Nan
            if all(time_diffs == 1):
                if row['label'] == 1:
                    # indices.extend(range(index - 2, index + 1))
                    index_1.append(index)
                elif row['label'] == 0:
                    index_0.append(index)
    '''

    df_sorted['time_diff'] = df_sorted['Start-hour2'].diff().dt.total_seconds() / 3600  # 首先，计算所有时间差
    df_sorted['is_one_hour'] = (df_sorted['time_diff'] == 1)  # 判断哪些行的时间差为1小时
    df_sorted['potential_sequence'] = (df_sorted['is_one_hour'] & df_sorted['is_one_hour'].shift(1))  # 判断连续3行是否都是1小时差距
    # 根据标签和potential_sequence过滤行
    index_1 = df_sorted[(df_sorted['label'] == 1) & (df_sorted['potential_sequence'])].index
    index_0 = df_sorted[(df_sorted['label'] == 0) & (df_sorted['potential_sequence'])].index
    df_sorted.drop(columns=['time_diff', 'is_one_hour', 'potential_sequence'], inplace=True)

    print(f'All count: {len(df_sorted)}')
    print(f'Success 0 count: {len(index_0)}')
    print(f'Success 1 count: {len(index_1)}')

    index_0 = index_0.tolist()  # pd.Index()可以转化回去
    index_1 = index_1.tolist()
    if len(index_0) > len(index_1):
        index_0 = random.sample(index_0, len(index_1))
    elif len(index_1) > len(index_0):
        index_1 = random.sample(index_1, len(index_0))

    df_sorted['month'] = df_sorted['Start-hour2'].dt.month
    df_sorted['hour'] = df_sorted['Start-hour2'].dt.hour
    df_sorted['weekday'] = df_sorted['Start-hour2'].dt.weekday
    df_sorted.drop(columns=['Start-hour2'], inplace=True)
    def process_string(s):
        words = s.split()
        return words[-1]

    df_sorted['Condition'] = df_sorted['Condition'].apply(process_string)
    if year == 2016:
        df_sorted['Condition'] = df_sorted['Condition'].replace('Sand', 'Dust')
    df_encoded = pd.get_dummies(df_sorted['Condition'], prefix='Condition')
    df_sorted = pd.concat([df_sorted, df_encoded], axis=1)
    df_sorted.drop('Condition', axis=1, inplace=True)
    for item in df_encoded.columns:
        df_sorted[item] = df_sorted[item].astype(int)
    if year == 2016:
        # Condition_Sand  to  Condition_Dust
        # add 'Condition_Nearby', 'Condition_Squalls' ,'Condition_Whirlwinds'
        df_sorted = df_sorted.assign(Condition_Nearby=0, Condition_Squalls=0, Condition_Whirlwinds=0)

    # bool to int
    # ['Crossing', 'No_Exit', 'Railway', 'Station', 'Stop', 'Roundabout', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
    for item in traffic_columns:
        df_sorted[item] = df_sorted[item].astype(int)

    index_01 = index_0 + index_1
    random.shuffle(index_01)
    split_point = len(index_01) // 10
    index_test = index_01[:split_point]
    index_train = index_01[split_point:]

    indices_test = []
    indices_train = []
    for index in index_test:
        indices_test.extend(range(index - 2, index + 1))
    for index in index_train:
        indices_train.extend(range(index - 2, index + 1))
    new_df_test = df_sorted.loc[indices_test].reset_index(drop=True)  # 不会影响df_sorted的索引
    new_df_train = df_sorted.loc[indices_train].reset_index(drop=True)
    new_df_test.to_csv('grouped_2016_test.csv', index=False)
    new_df_train.to_csv('grouped_2016_train.csv', index=False)
