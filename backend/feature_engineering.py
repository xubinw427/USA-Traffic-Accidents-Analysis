import os
import numpy as np
import pandas as pd
from sklearn.utils import resample
import math


rand_seed = 233
analysis_flag = True


def load_data(dir, file0, file1):
    df_label_0 = pd.read_csv(os.path.join(file0), encoding='latin1')
    df_label_1 = pd.read_csv(os.path.join(file1), encoding='latin1')
    if analysis_flag:
        print(df_label_0['Time'].head(10))
        print(df_label_1['Time'].head(10))
    df_label_0['Time'] = pd.to_datetime(df_label_0['Time'], format='%Y-%m-%d %H:%M:%S')
    df_label_1['Time'] = pd.to_datetime(df_label_1['Time'], format='%m/%d/%Y %H:%M')
    # df_label_1 = pd.read_csv(os.path.join(file1))
    df_label_0_balanced = resample(df_label_0,
                                   replace=False,  # 不放回采样
                                   n_samples=len(df_label_1),
                                   random_state=rand_seed)
    df_balanced = pd.concat([df_label_0_balanced, df_label_1])
    return df_balanced


def transformation(column, min_value, max_value):
    max_value -= min_value
    sin_values = [math.sin((2*math.pi*(x-min_value))/max_value) for x in column]
    cos_values = [math.cos((2*math.pi*(x-min_value))/max_value) for x in column]
    return sin_values, cos_values


if __name__ == "__main__":
    df_balanced = load_data('E:\\data', '2016_fake_data.csv', '2016_real_data.csv')
    traffic_columns = ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
    selected_columns = (['Time', 'Temperature', 'Humidity', 'Condition'] +
                        traffic_columns + ['Start_Lat', 'Start_Lng', 'label'])
    df_selected = df_balanced[selected_columns]
    df_filtered = df_selected.dropna()
    numeric_columns = ['Temperature', 'Humidity']
    # filter 1000
    tolerance = 1e-6
    for col in numeric_columns:
        # df = df[df[col] != 1000.0]
        df_filtered = df_filtered[~np.isclose(df_filtered[col], 1000.0, atol=tolerance)]
    # df_filtered['Time'] = pd.to_datetime(df_filtered['Time'], errors='coerce')
    df_filtered['month'] = df_filtered['Time'].dt.month
    df_filtered['hour'] = df_filtered['Time'].dt.hour
    df_filtered.drop(columns=['Time'], inplace=True)

    if analysis_flag:
        num_rows, num_columns = df_filtered.shape
        print("Size of DataFrame:")
        print("Row count:", num_rows)
        print("Column count:", num_columns)

        data_types = df_filtered.dtypes
        print("每个属性的数据类型：")
        print(data_types)

        label_counts = df_filtered['label'].value_counts()
        print("Label 为 0 的数量:", label_counts[0])
        print("Label 为 1 的数量:", label_counts[1])

        hour_counts = df_filtered['hour'].value_counts()
        for i in range(24):
            print(f"hour 为 {i} 的数量:", hour_counts[i])

    def process_string(s):
        words = s.split()
        return words[-1]

    df_filtered['Condition'] = df_filtered['Condition'].apply(process_string)

    sin_hour, cos_hour = transformation(df_filtered['hour'], 0, 23)
    df_filtered['sin_hour'] = sin_hour
    df_filtered['cos_hour'] = cos_hour
    df_filtered.drop('hour', axis=1, inplace=True)
    sin_month, cos_month = transformation(df_filtered['month'], 1, 12)
    df_filtered['sin_month'] = sin_month
    df_filtered['cos_month'] = cos_month
    df_filtered.drop('month', axis=1, inplace=True)

    df_encoded = pd.get_dummies(df_filtered['Condition'], prefix='Condition')
    if analysis_flag:
        category_counts = df_encoded.sum()
        print("总共有", len(category_counts), "种")
        print("每种的数量：")
        print(category_counts)
    df_filtered = pd.concat([df_filtered, df_encoded], axis=1)
    df_filtered.drop('Condition', axis=1, inplace=True)
    for item in df_encoded.columns:
        df_filtered[item] = df_filtered[item].astype(int)

    # bool to int
    # ['Crossing', 'No_Exit', 'Railway', 'Station', 'Stop', 'Roundabout', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
    for item in traffic_columns:
        df_filtered[item] = df_filtered[item].astype(int)

    # shuffle
    df_shuffled = df_filtered.sample(frac=1, random_state=rand_seed)
    print(df_shuffled.head(10))
    df_shuffled.to_csv('output_2016.csv', index=False)
