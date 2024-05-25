import os
import pandas as pd
import numpy as np

np.random.seed(233)


def process_coordinates(data_dir, csv_files, lon, lat):
    lon_min, lon_max = lon - 0.02, lon + 0.02
    lat_min, lat_max = lat - 0.02, lat + 0.02

    dfs = []
    for file in csv_files:
        df = pd.read_csv(os.path.join(data_dir, file))
        dfs.append(df)

    merged_df = pd.concat(dfs, ignore_index=True)

    merged_df['round_lat'] = np.round(merged_df['Start_Lat'] / 0.005) * 0.005
    merged_df['round_lng'] = np.round(merged_df['Start_Lng'] / 0.005) * 0.005

    filtered_df = merged_df[(merged_df['Start_Lng'] >= lon_min) & (merged_df['Start_Lng'] <= lon_max) &
                            (merged_df['Start_Lat'] >= lat_min) & (merged_df['Start_Lat'] <= lat_max)]

    # Calculate number of records per group
    num_records_per_group = filtered_df.groupby(['round_lat', 'round_lng']).size().reset_index(name='num_records')

    # Merge number of records into filtered_df
    filtered_df = pd.merge(filtered_df, num_records_per_group, on=['round_lat', 'round_lng'], how='left')

    # Identify top 10 locations
    top_locations = filtered_df[filtered_df['num_records'] > 0].nlargest(10, 'num_records')

    return top_locations[['Start_Lat', 'Start_Lng'] +
                         ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']]


def process_coordinates_old(data_dir, csv_files, lon, lat):
    lon_min, lon_max = lon - 0.02, lon + 0.02
    lat_min, lat_max = lat - 0.02, lat + 0.02

    dfs = []
    for file in csv_files:
        # df = pd.read_csv(os.path.join(data_dir, file), usecols=['Start_Lat', 'Start_Lng'])
        df = pd.read_csv(os.path.join(data_dir, file))
        dfs.append(df)

    merged_df = pd.concat(dfs, ignore_index=True)
    # print(merged_df.columns)

    merged_df['round_lat'] = np.round(merged_df['Start_Lat'] / 0.005) * 0.005
    merged_df['round_lng'] = np.round(merged_df['Start_Lng'] / 0.005) * 0.005

    filtered_df = merged_df[(merged_df['Start_Lng'] >= lon_min) & (merged_df['Start_Lng'] <= lon_max) &
                            (merged_df['Start_Lat'] >= lat_min) & (merged_df['Start_Lat'] <= lat_max)]

    grouped_df = filtered_df.groupby(['round_lat', 'round_lng']).apply(lambda x: x.sample(1, random_state=233))

    grouped_df.reset_index(drop=True, inplace=True)

    # Calculate number of records per group
    num_records_per_group = filtered_df.groupby(['round_lat', 'round_lng']).size().reset_index(name='num_records')

    # Merge number of records into grouped_df
    grouped_df = pd.merge(grouped_df, num_records_per_group, on=['round_lat', 'round_lng'], how='left')

    # Identify top 10 locations
    grouped_df['tops'] = grouped_df.groupby(['round_lat', 'round_lng'])['num_records'].rank(method='max',
                                                                                            ascending=False) <= 8

    # Filter out non-top locations
    top_locations = grouped_df[grouped_df['tops']][['Start_Lat', 'Start_Lng', 'Condition'] +
                                                   ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']]

    return top_locations


def add_input_info(df_locations, temperature, humidity, condition, month, hour, weekday):
    df_locations['Temperature'] = temperature
    df_locations['Humidity'] = humidity
    df_locations['Condition'] = condition
    df_locations['month'] = month
    df_locations['hour'] = hour
    df_locations['weekday'] = weekday
    return df_locations


def feature_engineering_tree(df_locations):
    def process_string(s):
        words = s.split()
        return words[-1]

    df_locations['Condition'] = df_locations['Condition'].apply(process_string)
    df_encoded = pd.get_dummies(df_locations['Condition'], prefix='Condition')
    df_locations = pd.concat([df_locations, df_encoded], axis=1)
    df_locations.drop('Condition', axis=1, inplace=True)
    for item in df_encoded.columns:
        df_locations[item] = df_locations[item].astype(int)
    traffic_columns = ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout',
                       'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
    for item in traffic_columns:
        df_locations[item] = df_locations[item].astype(int)
    columns_list = ['Temperature', 'Humidity', 'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop', 'Start_Lat', 'Start_Lng', 'month', 'hour', 'weekday', 'Condition_Cloud', 'Condition_Cloudy', 'Condition_Drizzle', 'Condition_Dust', 'Condition_Fair', 'Condition_Fog', 'Condition_Hail', 'Condition_Haze', 'Condition_Mist', 'Condition_Mix', 'Condition_Nearby', 'Condition_Precipitation', 'Condition_Rain', 'Condition_Shower', 'Condition_Sleet', 'Condition_Smoke', 'Condition_Snow', 'Condition_Squalls', 'Condition_T-Storm', 'Condition_Thunder', 'Condition_Tornado', 'Condition_Vicinity', 'Condition_Whirlwinds', 'Condition_Windy', 'Condition_Grains']
    df_all = df_locations.reindex(columns=columns_list, fill_value=0)
    df_all = df_all[sorted(df_all.columns)]
    return df_all


if __name__ == "__main__":
    '''
    data_dir = 'E:\\SFU\\classes\\CMPT-733\\Project\\code\\data_50w\\cleaned'
    csv_files = ['part-00000-a0960d4f-919d-4e35-af21-57829675a319-c000.csv',
                 'part-00001-a0960d4f-919d-4e35-af21-57829675a319-c000.csv',
                 'part-00002-a0960d4f-919d-4e35-af21-57829675a319-c000.csv',
                 'part-00003-a0960d4f-919d-4e35-af21-57829675a319-c000.csv',
                 'part-00004-a0960d4f-919d-4e35-af21-57829675a319-c000.csv',
                 'part-00005-a0960d4f-919d-4e35-af21-57829675a319-c000.csv',
                 'part-00006-a0960d4f-919d-4e35-af21-57829675a319-c000.csv',
                 'part-00007-a0960d4f-919d-4e35-af21-57829675a319-c000.csv',
                 'part-00008-a0960d4f-919d-4e35-af21-57829675a319-c000.csv',
                 'part-00009-a0960d4f-919d-4e35-af21-57829675a319-c000.csv',
                 'part-00010-a0960d4f-919d-4e35-af21-57829675a319-c000.csv']
    '''
    data_dir = 'E:\\data'
    csv_files = ['2020_real_data.csv']
    lon = -74.00
    lat = 40.70

    pd_df = process_coordinates(data_dir, csv_files, lon, lat)
    print(pd_df.head(3))

    pd_df = add_input_info(pd_df, 50, 50, 'Rain', 4, 18, 1)

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd_df = feature_engineering_tree(pd_df)
    print(pd_df.head())
